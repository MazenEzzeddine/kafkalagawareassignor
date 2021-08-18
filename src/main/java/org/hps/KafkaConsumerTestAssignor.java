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
        consumer.subscribe(Collections.singletonList(config.getTopic()));

        int[] percentile = new int[11];
        for (int i = 0; i < 11; i++)
            percentile[i] = 0;
        while (receivedMsgs < config.getMessageCount()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                receivedMsgs++;
                if (System.currentTimeMillis() - record.timestamp()  <= 1000) {
                    percentile[0]++;
                } else if (System.currentTimeMillis() - record.timestamp()  > 1000
                        && System.currentTimeMillis() - record.timestamp()  <= 2000) {
                    percentile[1]++;
                } else if (System.currentTimeMillis() - record.timestamp()  > 2000
                        && (System.currentTimeMillis() - record.timestamp()  <= 3000)) {
                    percentile[2]++;
                } else if (System.currentTimeMillis() - record.timestamp()  > 3000
                        && (System.currentTimeMillis() - record.timestamp()  <= 4000)) {
                    percentile[3]++;
                } else if (System.currentTimeMillis() - record.timestamp()  > 4000
                        && (System.currentTimeMillis() - record.timestamp()  <= 5000)) {
                    percentile[4]++;
                } else if (System.currentTimeMillis() - record.timestamp()  > 5000
                        && (System.currentTimeMillis() - record.timestamp()  <= 6000)) {
                    percentile[5]++;
                }  else if (System.currentTimeMillis() - record.timestamp()  > 6000
                        && System.currentTimeMillis() - record.timestamp()  <= 7000) {
                    percentile[6]++;
                } else if (System.currentTimeMillis() - record.timestamp()  > 7000
                        && (System.currentTimeMillis() - record.timestamp()  <= 8000)) {
                    percentile[7]++;
                } else if (System.currentTimeMillis() - record.timestamp()  > 8000
                        && System.currentTimeMillis() - record.timestamp()  <= 9000) {
                    percentile[8]++;
                } else if (System.currentTimeMillis() - record.timestamp()  > 9000
                        && (System.currentTimeMillis() - record.timestamp()  <= 10000)) {
                    percentile[9]++;
                } else if (System.currentTimeMillis() - record.timestamp()  > 10000 ) {
                    percentile[10]++;
                }
            }
            try {
                consumer.commitSync();
            } catch (RebalanceInProgressException e) {

                log.info ("Non-fatal commit failure");
            }


            log.info("==============================Statistics of {} poll =====================");

            log.info ("Number {} of message less than 1 secs",
                    percentile[0]);
            log.info ("Number {} of message  between  1  and 2 secs ",
                    percentile[1]);
            log.info ("Number {} and Percentage of message between  between  2  and 3  secs ",
                    percentile[2]);
            log.info ("Number {}  messages   between  3  and 4  secs",
                    percentile[3]);
            log.info (" Number {} of message  between  4  and 5  secs",
                    percentile[4]);
            log.info ("Number {}  of messages  between  5  and 6  secs ",
                    percentile[5]);
            log.info ("Number {} of messages  between  6  and 7  secs",
                    percentile[6]);
            log.info ("Number {}  of message  between  7  and 8  secs ",
                    percentile[7]);
            log.info ("Number {}  of message  between  8  and 9  secs ",
                    percentile[8]);
            log.info ("Number {} and Percentage of message  between  9  and 10  secs",
                    percentile[9]);
            log.info ("Number  {}  of messages greater than 10  secs",
                    percentile[10]);

            long sum = 0;
           for(int m=5; m<=10; m++) {
               sum += percentile[m];
            }

            log.info(" So far Percentage of authorization that violated the SLA so far {}",((double)sum/(double)receivedMsgs)*100.0);
            log.info("Sleeping for {} milliseconds", config.getSleep());


            Thread.sleep(Long.parseLong(config.getSleep()));
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







