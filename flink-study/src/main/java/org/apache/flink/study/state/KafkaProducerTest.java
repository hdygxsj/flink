package org.apache.flink.study.state;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerTest {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.101:9092"); // 替换为你的Kafka broker地址
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建Kafka生产者实例
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 发送消息到名为"test-topic"的主题
        for (int i = 0; i < 10000000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test4", Integer.toString(i), Integer.toString(i));
            producer.send(record);
        }

        // 关闭生产者以确保所有缓冲的消息都被发送且资源被正确释放
        producer.close();
    }
}
