package cn.kilo.durablogkv.storage;

import cn.kilo.durablogkv.operation.DeleteOperation;
import cn.kilo.durablogkv.operation.Operation;
import cn.kilo.durablogkv.operation.PutOperation;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
public class SSTable implements Closeable {


    /**
     * SSTable文件句柄
     */
    private final RandomAccessFile ssTableFile;

    /**
     * SSTable文件路径
     */
    private final String filePath;

    /**
     * SSTable元信息
     */
    private SSTableMetaInfo ssTableMetaInfo;

    {
        ssTableMetaInfo = new SSTableMetaInfo();
    }


    /**
     * SSTable中最大的键
     */
    protected String maxKey;

    /**
     * SSTable中最小的键
     */
    protected String minKey;

    /**
     * SSTable字段稀疏索引
     */
    private ConcurrentSkipListMap<String, Index> sparseIndex;


    /**
     * 构造函数
     * 实例化SSTable类请使用静态工厂方法
     *
     * @param filePath     SSTable文件路径
     * @param segnmentSize SSTable中分段的大小
     */
    private SSTable(String filePath, int segnmentSize) {
        this.filePath = filePath;
        try {
            this.ssTableFile = new RandomAccessFile(filePath, "rw");
            ssTableFile.seek(0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        this.ssTableMetaInfo.setSegnmentSize(segnmentSize);
        this.sparseIndex = new ConcurrentSkipListMap<>();
    }


    /**
     * 从MemTable中构建SSTable
     *
     * @param filePath     SSTable文件路径
     * @param segnmentSize SSTable中分段的大小
     * @return
     */
    public static SSTable getInstanceFromMemTable(String filePath, int segnmentSize, MemTable memTable) {
        SSTable ssTable = new SSTable(filePath, segnmentSize);
        ssTable.persistMemTable(memTable);
        return ssTable;
    }

    /**
     * 从SSTable文件中构建SSTable
     *
     * @param filePath SSTable文件路径
     * @return
     */
    public static SSTable getInstanceFromSSTableFile(String filePath) {
        SSTable ssTable = new SSTable(filePath, 0);
        ssTable.readFromSSTableFile();
        return ssTable;
    }

    /**
     * 从SSTable文件中读取数据
     *
     * 读取顺序为写入顺序的逆过程，先读取元信息，再读取稀疏索引，最后读取数据段
     *
     */
    private void readFromSSTableFile() {
        try {
            //读取SSTable元信息
            this.ssTableMetaInfo.readFromFile(this.ssTableFile);
            log.debug("[SSTable][readFromSSTableFile]: {}", this.ssTableMetaInfo);

            //读取稀疏索引
            byte[] sparseIndexBytes = new byte[(int) this.ssTableMetaInfo.getIndexLen()];
            this.ssTableFile.seek(this.ssTableMetaInfo.getIndexStart());
            this.ssTableFile.read(sparseIndexBytes);
            TypeReference<ConcurrentSkipListMap<String, Index>> typeReference = new TypeReference<ConcurrentSkipListMap<String, Index>>() {};
            this.sparseIndex = JSONObject.parseObject(new String(sparseIndexBytes, StandardCharsets.UTF_8), typeReference);
            log.debug("[SSTable][readFromSSTableFile][sparseIndex]: {}", this.sparseIndex);

            /**
             * 读取数据段中第一个段的第一个key作为最小key
             */
            this.ssTableFile.seek(this.ssTableMetaInfo.getDataStart());
            byte[] dataBytes = new byte[(int) this.sparseIndex.firstEntry().getValue().getLength()];
            this.ssTableFile.read(dataBytes);
//            JSONObject data = JSONObject.parseObject(new String(dataBytes, StandardCharsets.UTF_8));
            TypeReference<ConcurrentSkipListMap<String, Operation>> typeReference_seg = new TypeReference<ConcurrentSkipListMap<String, Operation>>() {};
            ConcurrentSkipListMap<String, Operation> headSegnment = JSONObject.parseObject(new String(dataBytes, StandardCharsets.UTF_8), typeReference_seg);
            //获取segnment的第一个key作为最小key
            Optional<String> firstKey = headSegnment.keySet().stream().findFirst();
            firstKey.ifPresent(key -> this.minKey = key);
            dataBytes = null;
            headSegnment.clear();


            /**
             * 读取数据段中最后一个段的最后一个key作为最大key
             */
            this.ssTableFile.seek(this.sparseIndex.lastEntry().getValue().getStartPos());
            dataBytes = new byte[(int) this.sparseIndex.lastEntry().getValue().getLength()];
            this.ssTableFile.read(dataBytes);
            ConcurrentSkipListMap<String, Operation> tailSegnment = JSONObject.parseObject(new String(dataBytes, StandardCharsets.UTF_8), typeReference_seg);
            //获取segnment的最后一个个key作为最小key
            Optional<String> lastKey = tailSegnment.keySet().stream().reduce((first, second) -> second);
            lastKey.ifPresent(key -> this.maxKey = key);
            dataBytes = null;
            tailSegnment.clear();

            log.debug("[SSTable][readFromSSTableFile][data]: minKey-{}; maxKey-{}", this.minKey, this.maxKey);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * TODO: query
     */


    /**
     * 持久化MemTable到SSTable
     *
     * @param memTable
     * @return
     */
    private void persistMemTable(MemTable memTable) {
        this.minKey = memTable.getMinKey();
        this.maxKey = memTable.getMaxKey();
        try {
            JSONObject segnment = new JSONObject(true);
            //记录数据段的起始位置
            this.ssTableMetaInfo.setDataStart(this.ssTableFile.getFilePointer());
            memTable.getTable().forEach((key, operation) -> {
                if (operation instanceof PutOperation) {
                    segnment.put(key, (PutOperation) operation);
                } else if (operation instanceof DeleteOperation) {
                    segnment.put(key, (DeleteOperation) operation);
                }

                //达到分段大小临界值，写入文件
                if (segnment.size() >= this.ssTableMetaInfo.getSegnmentSize()) {
                    try {
                        writeSegnmentToFile(segnment);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            //若最后一段数据不足分段大小，也写入文件
            if (segnment.size() > 0) {
                try {
                    writeSegnmentToFile(segnment);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            //记录SSTable数据段的结束位置
            long dataLen = this.ssTableFile.getFilePointer() - this.ssTableMetaInfo.getDataStart();
            this.ssTableMetaInfo.setDataLen(dataLen);

            //将稀疏索引写入文件
            byte[] sparseIndexBytes = JSONObject.toJSONString(this.sparseIndex).getBytes(StandardCharsets.UTF_8);
            this.ssTableMetaInfo.setIndexStart(this.ssTableFile.getFilePointer());
            this.ssTableFile.write(sparseIndexBytes);
            this.ssTableMetaInfo.setIndexLen(sparseIndexBytes.length);
            log.debug("[SSTable][transFromMemTable][sparseIndex]: {}", this.sparseIndex);

            //将SSTable元信息写入文件
            this.ssTableMetaInfo.writeToFile(this.ssTableFile);
            log.debug("[SSTable][transFromMemTable]: {},{}", this.filePath, this.ssTableMetaInfo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 将数据段写入到文件中
     *
     * @param segnment
     */
    private void writeSegnmentToFile(JSONObject segnment) throws IOException {
        byte[] segnmentBytes = segnment.toJSONString().getBytes(StandardCharsets.UTF_8);
        long segnmentStart = this.ssTableFile.getFilePointer();
        long segnmentLength = segnmentBytes.length;
        ssTableFile.write(segnmentBytes);

        //记录数据段的第一个key到稀疏索引中
        Optional<String> firstKey = segnment.keySet().stream().findFirst();
        firstKey.ifPresent(key -> this.sparseIndex.put(key, new Index(segnmentStart, segnmentLength)));
        segnmentBytes = null;
        segnment.clear();
    }



    @Override
    public void close() throws IOException {
        this.ssTableFile.close();
    }
}
