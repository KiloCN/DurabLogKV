package cn.kilo.durablogkv.storage;

import cn.kilo.durablogkv.operation.Operation;

import java.util.concurrent.ConcurrentSkipListMap;;

public class MemTable {

    /**
     * 使用ConcurrentSkipListMap作为内存表的数据结构, 保证线程安全,跳表具有比红黑树更快的插入和删除操作，因为它允许直接跳过多个节点。
     *
     */
    private ConcurrentSkipListMap<String, Operation> table;

    public MemTable() {
        this.table = new ConcurrentSkipListMap<>();
    }

    public ConcurrentSkipListMap<String, Operation> getTable() {
        return table;
    }


    public String getMinKey() {
        return table.firstKey();
    }

    public  String getMaxKey() {
        return table.lastKey();
    }


    public void put(String key, Operation operation) {
        table.put(key, operation);
    }
}
