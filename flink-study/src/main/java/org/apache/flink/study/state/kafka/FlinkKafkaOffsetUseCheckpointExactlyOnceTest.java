package org.apache.flink.study.state.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkKafkaOffsetUseCheckpointExactlyOnceTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT.key(),8082);
        configuration.setString(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT.key(), "2 min");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setTopics("test4")
                .setGroupId("g-cken-1")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setBootstrapServers("127.0.0.1:9092")
                .build();
        Properties producerProps = new Properties();
        producerProps.put("transaction.timeout.ms","120000");
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setKafkaProducerConfig(producerProps)
                .setBootstrapServers("127.0.0.1:9092")
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setTopic("test9").build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "kafka").setParallelism(1);
        SingleOutputStreamOperator<String> ot = source
                .disableChaining()
                .map(e -> {
                    if("60000".equals(e)){
                        throw new RuntimeException();
                    }
                    return e;
                }).setParallelism(14)
                .disableChaining();
        ot.sinkTo(kafkaSink).setParallelism(1);
        ot.print().setParallelism(1);
        env.execute();
    }
}
