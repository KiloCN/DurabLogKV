package cn.kilo.durablogkv.operation;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PutOperation extends AbstractOperation {

    private String key;

    private String value;

    public PutOperation(String key, String newValue) {
        super(OperationTypeEnum.PUT);
        this.key = key;
        this.value = newValue;
    }

}
