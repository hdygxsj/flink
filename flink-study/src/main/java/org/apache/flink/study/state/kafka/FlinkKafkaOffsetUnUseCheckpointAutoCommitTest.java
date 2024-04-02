package org.apache.flink.study.state.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkKafkaOffsetUnUseCheckpointAutoCommitTest {

    private static final Logger logger = LoggerFactory.getLogger(FlinkKafkaOffsetUnUseCheckpointAutoCommitTest.class);
    public static void main(String[] args) throws Exception {
        logger.info("test log");
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT.key(),8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        Properties props = new Properties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"10000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1000");
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setTopics("test4")
                .setGroupId("g-uck-2")
                .setProperties(props)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setBootstrapServers("127.0.0.1:9092")
                .build();
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setTopic("test9").build())
                .build();
        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "kafka");
        SingleOutputStreamOperator<String> ot = source
                .disableChaining()
                .map(e -> {
                    TimeUnit.MILLISECONDS.sleep(100000);
                    return e;
                }).setParallelism(14)
                .disableChaining();
        ot.sinkTo(kafkaSink).setParallelism(1);
        ot.print().setParallelism(1);
        env.execute();
    }
}
