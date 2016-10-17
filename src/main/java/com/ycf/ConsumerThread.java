package com.ycf;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;

/**
 * Created by sniper on 16-10-12.
 */
public class ConsumerThread extends Thread {

    private KafkaStream stream;
    private ConsumerCallback callback;
    private Integer numThread;

    public ConsumerThread(KafkaStream stream, ConsumerCallback callback, Integer numThread){
        this.stream = stream;
        this.callback = callback;
        this.numThread =numThread;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        callback.afterReceive(it);
    }
}
