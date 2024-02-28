package org.apache.flink.study.streamapi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

public class TransformationDebugDemo {

    public static void main(String[] args) throws Exception {
        URL resource = TransformationDebugDemo.class.getResource("/");
        if (resource == null) {
            return;
        }
        String path = resource.getPath();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> source = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(path + "/word-count.txt"))
                .build();
        DataStreamSource<String> fileDataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(),
                "file");
        SingleOutputStreamOperator<String> sop = fileDataStreamSource.map(e -> "map: " + e).disableChaining();
        DataStreamSink<String> print = sop.print();
        DataStreamSink<String> print1 = fileDataStreamSource.print().name("print 1").setParallelism(1);
        env.execute();
    }
}
