package cn.kilo.durablogkv.storage;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

@Data
@AllArgsConstructor
public class Index implements Serializable {

    /**
     * 分区起始位置
     */
    public long startPos;

    /**
     * 分区长度
     */
    public long length;
}
