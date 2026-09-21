package com.cdc.pipeline.function;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.IOException;

/**
 * Debezium tombstone(null) 메시지를 안전하게 처리하는 Deserializer.
 * SimpleStringSchema는 null 바이트에서 NPE 발생 → 이 클래스로 대체.
 */
public class NullSafeStringSchema implements DeserializationSchema<String> {

    @Override
    public String deserialize(byte[] message) throws IOException {
        if (message == null) return null;
        return new String(message, java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}
