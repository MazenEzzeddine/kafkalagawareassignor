package org.hps;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;

public class KafkaConsumerTestAssignor {
    private static final Logger log = LogManager.getLogger(KafkaConsumerTestAssignor.class);
    private static long iteration = 0;

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);
        int receivedMsgs = 0;

        String rebalance = System.getenv("REBALANCE");
        // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
       // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, LagBasedPartitionAssignor.class.getName());

        if (rebalance.equalsIgnoreCase("continualflow")) {

            props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    org.apache.kafka.clients.consumer.CooperativeStickyAssignor.class.getName());

        } else {

            props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    LagBasedPartitionAssignor.class.getName());

        }

        boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()), new HandleRebalance());

        int[] percentile = new int[11];
        for (int i = 0; i < 11; i++)
            percentile[i] = 0;
        while (receivedMsgs < config.getMessageCount()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {

                log.info("Received message:");
                log.info("\tpartition: {}", record.partition());
                log.info("\toffset: {}", record.offset());
                log.info("\tvalue: {}", record.value());


                receivedMsgs++;

            }
            try {
                consumer.commitSync();
            } catch (RebalanceInProgressException e) {

                log.info ("Non-fatal commit failure");
            }



            log.info("Sleeping for {} milliseconds", config.getSleep());


            Thread.sleep(Long.parseLong(config.getSleep()));
            log.info("==============================Calling Poll Again =====================");
            iteration++;
        }

        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                log.info("shutdown hook you can commit your offsets or close your state.");
            }
        });
         log.info("received msgs {}", receivedMsgs);
    }
}







