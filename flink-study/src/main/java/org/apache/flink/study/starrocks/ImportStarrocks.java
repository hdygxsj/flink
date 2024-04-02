package org.apache.flink.study.starrocks;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.study.streamapi.TransformationDebugDemo;

import java.net.URL;

public class ImportStarrocks {

    public static void main(String[] args) {
        String createStarrocksDdl = "CREATE TABLE `score_board` (\n"
                + "    `id` INT,\n"
                + "    `name` STRING,\n"
                + "    `score` INT,\n"
                + "    PRIMARY KEY (id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "    'connector' = 'starrocks',\n"
                + "    'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',\n"
                + "    'load-url' = '127.0.0.1:8030',\n"
                + "    'database-name' = 'test',\n"
                + "    'table-name' = 'score_board',\n"
                + "    'username' = 'root',\n"
                + "    'password' = ''\n"
                + ");";
        URL resource = TransformationDebugDemo.class.getResource("/");
        if (resource == null) {
            return;
        }
        String path = resource.getPath();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        FileSource<String> source = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(path + "/word-count.txt"))
                .build();

    }
}
