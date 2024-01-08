package cn.kilo.durablogkv.operation;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class PutOperation extends AbstractOperation {

    private String key;

    private String value;

    public PutOperation(String key, String Value) {
        super(OperationTypeEnum.PUT);
        this.key = key;
        this.value = Value;
    }

}
