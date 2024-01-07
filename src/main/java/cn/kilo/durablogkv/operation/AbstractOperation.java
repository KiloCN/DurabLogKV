package cn.kilo.durablogkv.operation;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
/**
 * 抽象操作类
 */

@Setter
@Getter
public abstract class AbstractOperation implements Operation{

    /**
     * 操作类型
     */
    private OperationTypeEnum type;

    public AbstractOperation(OperationTypeEnum type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
