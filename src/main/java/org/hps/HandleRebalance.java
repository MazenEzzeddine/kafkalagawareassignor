package org.hps;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

public class HandleRebalance implements ConsumerRebalanceListener {
    private static final Logger log = LogManager.getLogger(HandleRebalance.class);

    public void onPartitionsAssigned(Collection<TopicPartition>
                                             partitions) {
        System.out.println("We are Rebalancing assigned  partitions :");

        for(TopicPartition p: partitions)
            log.info(p.partition());


    }
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("We are Rebalancing lost partitions :");
        for(TopicPartition p: partitions)
            log.info(p.partition());
    }
}
