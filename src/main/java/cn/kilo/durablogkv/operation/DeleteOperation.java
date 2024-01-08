package cn.kilo.durablogkv.operation;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class DeleteOperation extends AbstractOperation {

    private String key;

    public DeleteOperation(String key) {
        super(OperationTypeEnum.DELETE);
        this.key = key;
    }

}
