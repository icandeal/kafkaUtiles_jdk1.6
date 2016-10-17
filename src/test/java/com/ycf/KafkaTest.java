package com.ycf;

import com.ycf.ConsumerCallback;
import com.ycf.PooledKafka;
import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by sniper on 16-10-12.
 */
public class KafkaTest {
    private PooledKafka pooledKafka;
    @Before
    public void beforeTest() throws IOException {
        pooledKafka = new PooledKafka();
        Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream("/properties/config.properties"));
        pooledKafka.initPooled(properties);
    }

    @Test
    public void doSend(){
        Assert.assertTrue(pooledKafka.send("test", "ffff", "飞娃儿额"));
    }

    @Test
    public void doReceive(){
        pooledKafka.receive(new ConsumerCallback() {
            public void afterReceive(ConsumerIterator<byte[], byte[]> iterator) {
                while (iterator.hasNext()) {
                    MessageAndMetadata<byte[],byte[]> mm = iterator.next();
                    System.out.println(" message:"+(new String(mm.message())));
                }
            }
        },"test222", "test", 3);
        while(true){

        }
    }
}
