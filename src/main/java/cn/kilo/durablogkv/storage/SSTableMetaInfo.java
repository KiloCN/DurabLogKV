package cn.kilo.durablogkv.storage;

import lombok.Data;

import java.io.RandomAccessFile;

@Data
public class SSTableMetaInfo {


    /**
     * SSTable文件中数据区开始
     */
    private long dataStart;

    /**
     * SSTable文件中数据区长度
     */
    private long dataLen;

    /**
     * SSTable文件中稀疏索引区开始
     */
    private long indexStart;

    /**
     * SSTable文件中稀疏索引区长度
     */
    private long indexLen;

    /**
     * SSTable文件中的分段大小
     */
    private long segnmentSize;


    /**
     * 将SSTable元数据写入到文件中
     * @param file
     */
    protected void writeToFile(RandomAccessFile file) {
        try {
            file.writeLong(segnmentSize);
            file.writeLong(dataStart);
            file.writeLong(dataLen);
            file.writeLong(indexStart);
            file.writeLong(indexLen);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }


    /**
     * 从文件中读取SSTable元数据，从尾部倒序读取
     * @param file
     */
    protected void readFromFile(RandomAccessFile file) {
        try {
            file.seek(file.length() - 8);
            this.indexLen = file.readLong();
            file.seek(file.length() - 8 * 2);
            this.indexStart = file.readLong();
            file.seek(file.length() - 8 * 3);
            this.dataLen = file.readLong();
            file.seek(file.length() - 8 * 4);
            this.dataStart = file.readLong();
            file.seek(file.length() - 8 * 5);
            this.segnmentSize = file.readLong();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
