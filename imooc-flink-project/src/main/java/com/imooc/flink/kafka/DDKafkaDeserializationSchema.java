package com.imooc.flink.kafka;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class DDKafkaDeserializationSchema implements KafkaDeserializationSchema<Tuple2<String, String>> {

    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return false;
    }

    /**
     *
     * @param record kafka 中数据
     *   通过每一条数据的topic，partition，offset 来设计主键
     * @return
     * @throws Exception
     */
    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String id = topic + "_" + partition + "_" + offset;

        String value = new String(record.value());
        return Tuple2.of(id, value);
    }

    // ??? 黑人问号
    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        });
    }
}
