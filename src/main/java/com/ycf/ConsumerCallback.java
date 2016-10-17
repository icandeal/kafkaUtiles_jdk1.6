package com.ycf;

import kafka.consumer.ConsumerIterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Map;

/**
 * Created by sniper on 16-10-12.
 */
public interface ConsumerCallback {
    void afterReceive(ConsumerIterator<byte[], byte[]> iterator);
}
