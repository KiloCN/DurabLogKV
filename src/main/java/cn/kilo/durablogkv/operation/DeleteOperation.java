package cn.kilo.durablogkv.operation;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DeleteOperation extends AbstractOperation {

    private String key;

    public DeleteOperation(String key) {
        super(OperationTypeEnum.DELETE);
        this.key = key;
    }
}
