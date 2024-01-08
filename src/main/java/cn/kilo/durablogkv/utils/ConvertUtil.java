package cn.kilo.durablogkv.utils;

import cn.kilo.durablogkv.operation.DeleteOperation;
import cn.kilo.durablogkv.operation.Operation;
import cn.kilo.durablogkv.operation.OperationTypeEnum;
import cn.kilo.durablogkv.operation.PutOperation;
import com.alibaba.fastjson.JSONObject;

public class ConvertUtil {

    public static final String TYPE = "type";


    public static Operation jsonObjectToOperation(JSONObject value) {
        if (value.getString(TYPE).equals(OperationTypeEnum.PUT.name())) {
            return value.toJavaObject(PutOperation.class);
        } else if (value.getString(TYPE).equals(OperationTypeEnum.DELETE.name())) {
            return value.toJavaObject(DeleteOperation.class);
        }
        return null;
    }
}
