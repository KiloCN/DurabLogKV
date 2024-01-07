
package cn.kilo.durablogkv.storage;

import cn.kilo.durablogkv.operation.DeleteOperation;
import cn.kilo.durablogkv.operation.PutOperation;
import org.junit.Test;

public class SSTableTest {

    @Test
    public void createFromMemTable() {
        MemTable memTable = new MemTable();
        for (int i = 0; i < 100; i++) {
            PutOperation putOperation = new PutOperation("key" + i, "value" + i);
            memTable.put(putOperation.getKey(), putOperation);
        }
        memTable.put("key100", new PutOperation("key100", "value100"));
        memTable.put("key100", new DeleteOperation("key100"));
        SSTable ssTable = SSTable.getInstanceFromMemTable("test.txt", 3, memTable);
    }


    @Test
    public void createFromFile() {
        SSTable ssTable = SSTable.getInstanceFromSSTableFile("test.txt");
        System.out.println(ssTable.toString());
    }
}
