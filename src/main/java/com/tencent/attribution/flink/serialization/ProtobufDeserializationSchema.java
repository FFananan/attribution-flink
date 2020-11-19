package com.tencent.attribution.flink.serialization;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.GeneratedMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Method;

public class ProtobufDeserializationSchema<T extends AbstractMessage> implements DeserializationSchema<T> {

    private transient Method parseFunc;
    private Class<T> pbClass;

    public ProtobufDeserializationSchema(Class<T> pbClass) throws NoSuchMethodException {
        this.pbClass = pbClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] bytes) throws Exception {
        return (T) parseFunc.invoke(null, bytes);
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public void initialize(Configuration parameters) throws NoSuchMethodException {
        parseFunc = pbClass.getMethod("parseFrom", byte[].class);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(pbClass);
    }
}
