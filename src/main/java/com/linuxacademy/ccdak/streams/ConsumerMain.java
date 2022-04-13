package com.linuxacademy.ccdak.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMain {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:29092");
        props.setProperty("group.id", "group1");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of("inventory_purchases"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("output.dat"));

        try {
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    String recordString = "key=" + consumerRecord.key() + ", topic=" + consumerRecord.topic() +
                            ", partition=" + consumerRecord.partition() + ", offset=" + consumerRecord.offset();
                    System.out.println(recordString);
                    writer.write(recordString + "\n");
                    writer.flush();
                    consumer.commitAsync();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writer.close();
            consumer.close();
        }
    }

}
