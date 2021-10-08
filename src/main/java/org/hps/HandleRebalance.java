package org.hps;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Collection;

public class HandleRebalance implements ConsumerRebalanceListener {
    private static final Logger log = LogManager.getLogger(HandleRebalance.class);

    public void onPartitionsAssigned(Collection<TopicPartition>
                                             partitions) {
        System.out.println("We are Rebalancing assigned  partitions :");

        for(TopicPartition p: partitions) {
            log.info(p.partition());
            if (p.partition()==0) {
                try {
                    log.info("opened DB for partition 0");
                    KafkaConsumerTestAssignor.db0 = RocksDB.open(KafkaConsumerTestAssignor.options, "/disk1/consumer00");
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            } else if (p.partition()==1) {
                try {
                    log.info("opened DB for partition 1");
                    KafkaConsumerTestAssignor.db1 = RocksDB.open(KafkaConsumerTestAssignor.options, "/disk2/consumer11");
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            }
        }


    }
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("We are Rebalancing lost partitions :");
        for(TopicPartition p: partitions) {
            log.info(p.partition());
            if (p.partition()==0) {
                    log.info("closed DB for partition 0");
                    KafkaConsumerTestAssignor.db0.close();
            } else if (p.partition()==1) {
                    log.info("opened DB for partition 1");
                    KafkaConsumerTestAssignor.db1.close();
            }
        }
    }
}
