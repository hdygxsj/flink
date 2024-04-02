package org.apache.flink.study.state.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.concurrent.TimeUnit;

public class FlinkKafkaOffsetUseCheckpointTimeOutTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT.key(),8082);
        configuration.setString(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT.key(),"30 S");
        configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY,"file://home/zhongyangyang/temp/ck");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(10000, CheckpointingMode.AT_LEAST_ONCE);
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setTopics("test4")
                .setGroupId("g-ck")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setBootstrapServers("127.0.0.1:9092")
                .build();
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setTopic("test5").build())
                .build();
        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "kafka").setParallelism(1);
        source.disableChaining().map(e->{
            TimeUnit.SECONDS.sleep(1);
            return e;
        }).disableChaining().sinkTo(kafkaSink).setParallelism(1);
        env.execute();
    }
}
