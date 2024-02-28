package org.apache.flink.study.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class FlinkKafkaOffsetUnUseCheckpointTest {

    private static final Logger logger = LoggerFactory.getLogger(FlinkKafkaOffsetUnUseCheckpointTest.class);
    public static void main(String[] args) throws Exception {
        logger.info("test log");
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT.key(),8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setTopics("test4")
                .setGroupId("g-f-default")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setBootstrapServers("192.168.0.101:9092")
                .build();
        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "kafka");
        source.map(e->{
            TimeUnit.SECONDS.sleep(1);
            return e;
        }).print();
        env.execute();
    }
}
