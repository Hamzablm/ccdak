package com.linuxacademy.ccdak.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ProducerMain {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("acks", "all");

        // kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        String fileName = "src/main/resources/sample_transaction_log.txt";
        List<String> list = new ArrayList<>();

        try (BufferedReader br = Files.newBufferedReader(Paths.get(fileName))) {

            //br returns as stream and convert it into a List
            list = br.lines().collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("list = " + list);
        list.forEach(statement -> {
            String[] split = statement.split(":");
            producer.send(new ProducerRecord<>("inventory_purchases", split[0], split[1]), (metadata, exception) -> {
                System.out.println("topic: " + metadata.topic() + " offset: " + metadata.offset() + " partition: " + metadata.partition());
            });
            if (statement.startsWith("apples")) {
                producer.send(new ProducerRecord<>("apple_purchases", split[0], split[1]), (metadata, exception) -> {
                    System.out.println("topic: " + metadata.topic() + " offset: " + metadata.offset() + " partition: " + metadata.partition());
                });
            }
        });

        producer.close();

    }

}
























