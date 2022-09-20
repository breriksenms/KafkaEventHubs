package org.example;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

public class Main {
    private static ScheduledReporter reporter;
    private static ScheduledReporter csvReporter;
    private static Meter meter;
    private static long now;
    private static AtomicReference<Long> numEvents;
    static String bootstrapServers = "{domain}.servicebus.windows.net:9093"; 

    private static Properties getKafkaProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "kafkaraw");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "10000");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{Connection String}\";");
        return props;
    }

    public static void main(String[] args) {
        MetricRegistry metricRegistry = new MetricRegistry();
        meter = metricRegistry.meter("benchmark");

        reporter = ConsoleReporter
                .forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build();
        csvReporter = CsvReporter
                .forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build(Paths.get("results").toFile());

        now = System.currentTimeMillis();
        numEvents = new AtomicReference<>(1000000L);

        System.out.println("Starting event processor");

        // Running 1 KafkaConsumer
        // Tries to own all partitions and bogs down.
        final KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(getKafkaProperties());
        consumer.assign(Arrays.asList(new TopicPartition("devtest", 0)));

        while(true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(10000));

            System.out.println("size of records polled is "+ records.count());
            for (ConsumerRecord<Integer, String> record : records) {
                meter.mark();
                Long eventsLeft = numEvents.getAndSet(numEvents.get() - 1);
                if (eventsLeft == 0) {
                    long after = System.currentTimeMillis();
                    long took = after - now;
                    System.out.printf("Test took %d ms%n", took);
                    reporter.report();
                    csvReporter.report();
                    consumer.close();
                    System.exit(0);
                }
                if (eventsLeft % 10000 == 0) {
                    System.out.println(eventsLeft);
                }
            }
        }
        
        // Running a KafkaConsumer per partition.
        // Consumes much faster.
        /*for (int i = 0; i < 32; i++) {
            final int x = i;
            Thread t = new Thread() {
                public void run() {
                    final KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(getKafkaProperties());
                    consumer.assign(Collections.singletonList(new TopicPartition("devtest", x)));
                    //consumer.subscribe(Collections.singletonList("devtest"));

                    while(true) {
                        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(10000));

                        System.out.println("size of records polled is "+ records.count());
                        for (ConsumerRecord<Integer, String> record : records) {
                            //System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                            meter.mark();
                            Long eventsLeft = numEvents.getAndSet(numEvents.get() - 1);
                            if (eventsLeft == 0) {
                                long after = System.currentTimeMillis();
                                long took = after - now;
                                System.out.printf("Test took %d ms%n", took);
                                reporter.report();
                                csvReporter.report();
                                consumer.close();
                                System.exit(0);
                            }
                            if (eventsLeft % 10000 == 0) {
                                System.out.println(eventsLeft);
                            }
                        }
                        //consumer.commitSync();
                    }
                }
            };
            t.start();
        }
        while (true) Thread.sleep(1000);*/
    }
}
