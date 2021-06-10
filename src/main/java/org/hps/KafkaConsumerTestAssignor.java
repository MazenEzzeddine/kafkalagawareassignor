package org.hps;

import org.apache.kafka.clients.consumer.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerTestAssignor {
    private static final Logger log = LogManager.getLogger(KafkaConsumerTestAssignor.class);


   // private static long iteration = 0;

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();

        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());

        Properties props = KafkaConsumerConfig.createProperties(config);
        //int receivedMsgs = 0;

        // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, LagBasedPartitionAssignor.class.getName());
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()));



             Instant now = null;
             Instant end = null;
             long elapsedTime;
             Long noww = 0L;
             Long endd;


        while ( true /*receivedMsgs < config.getMessageCount()*/) {

            //  consumer.enforceRebalance();


     /*        do {
                 current= 0;*/

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

            for (ConsumerRecord<String, String> record : records) {
                if(noww == 0L) {
                    noww = record.timestamp();
                }
              /*  log.info("Received message:");
                log.info("\tpartition: {}", record.partition());
                log.info("\toffset: {}", record.offset());
                log.info("\tvalue: {}", record.value());*/
                /*if (record.headers() != null) {
                    log.info("\theaders: ");
                    for (Header header : record.headers()) {
                        log.info("\t\tkey: {}, value: {}", header.key(), new String(header.value()));
                    }
                }*/
                //receivedMsgs++;

               // log.info("iteration:{}", iteration);



               // receivedMsgs += records.count();
                 /*current = records.count();
                 if (current ==0){
                     break;
                 }*/
                /*  counter += records.count();*/
                /*} while(counter<15);
                 */
                //if (commit) {

                // }
               // iteration++;

                log.info("log append timestamp {}", record.timestamp());
                log.info(" it took {} ms to get the message from the broker and process it {}", record.timestamp() - System.currentTimeMillis());


            }


            consumer.commitSync();
            end = Instant.now();
            endd = System.currentTimeMillis();


             //elapsedTime = Duration.between(end, now).toMillis();

            elapsedTime = endd - noww;


            log.info("It took around {} ms to get and process a batch of {}", elapsedTime, records.count());

            log.info("Sleeping for {} milliseconds", config.getSleep());



            Thread.sleep(Long.parseLong(config.getSleep()));
            noww = 0L;
        }
       // log.info("Received {} messages", receivedMsgs);
    }
}





