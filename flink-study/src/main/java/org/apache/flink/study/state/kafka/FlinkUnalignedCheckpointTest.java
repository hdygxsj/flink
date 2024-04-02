package org.apache.flink.study.state.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class FlinkUnalignedCheckpointTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT.key(),8082);
        configuration.setString(ExecutionCheckpointingOptions.ENABLE_UNALIGNED.key(),"true");
        configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY,"file:///Users/zhongyangyang/temp/ck");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointTimeout(40000);
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(30));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(10, TimeUnit.SECONDS) // 延时
        ));
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setTopics("test4")
                .setGroupId("g-uack-1")
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
        SingleOutputStreamOperator<String> ot = source
                .disableChaining()
                .map(e -> {
                    if(Integer.parseInt(e)%50000==0){
                        TimeUnit.MINUTES.sleep(1);
                    }
                    return e;
                })
                .disableChaining()
                .setParallelism(1);
        ot.sinkTo(kafkaSink).setParallelism(1);
        ot.print().setParallelism(1);
        env.execute();
    }
}
