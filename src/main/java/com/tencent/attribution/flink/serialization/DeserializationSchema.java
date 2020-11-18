package com.tencent.attribution.flink.serialization;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

public interface DeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {
    T deserialize(byte[] var1) throws Exception;

    boolean isEndOfStream(T var1);

    void initialize(Configuration parameters) throws Exception;
}