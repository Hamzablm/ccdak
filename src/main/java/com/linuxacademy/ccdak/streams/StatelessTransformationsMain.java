package com.linuxacademy.ccdak.streams;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

public class StatelessTransformationsMain {

    public static void main(String[] args) {
        // Set up the configuration.
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateless-transformations-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:229092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Since the input topic uses Strings for both key and value, set the default Serdes to String.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Get the source stream.
        final StreamsBuilder builder = new StreamsBuilder();
        
        //Implement streams logic.
        final KStream<String, String> source = builder.stream("stateless-transformation-input-topic");

        // Split the stream into two streams, one containing all records where the key begins with "a", and the other
        // containing all other records
        KStream<String, String>[] branches = source.branch((key, value) -> key.startsWith("a"),
                (key, value) -> true);

        KStream<String, String> aKeysStream = branches[0];
        KStream<String, String> othersStream = branches[1];

        // Removes any records from the "a" stream where the value does not also start with "a"
        aKeysStream = aKeysStream.filter((key, value) -> value.startsWith("a"));

        // For the "a" stream, convert each record into two records, one with an uppercase value
        // and one with a lowercase value.
        aKeysStream = aKeysStream.flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, value.toUpperCase()));
            result.add(KeyValue.pair(key, value.toLowerCase()));
            return result;
        });

//        aKeysStream.foreach((key, value) -> System.out.println("key=" + key + ", value=" + value));



        // For the "a" stream, modify all records by uppercasing the key.
        aKeysStream = aKeysStream.map((key, value) -> KeyValue.pair(key.toUpperCase(), value));

        // Merge the two streams back together
        KStream<String, String> mergedStream = aKeysStream.merge(othersStream);

        //Print each record to the console
        mergedStream.peek((key, value) -> System.out.println("key=" + key + ", value=" + value));

        // Output the transformation date to a topic
        mergedStream.to("stateless-transformation-output-topic");
        System.out.println("transformation is sent to output topic");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        // Print the topology to the console.
        System.out.println(topology.describe());
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach a shutdown handler to catch control-c and terminate the application gracefully.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }

}
