package com.ycf;

import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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
        for(int i = 0; i <=200; i++) {
            pooledKafka.send("test", "ttt="+i);
        }
        Assert.assertTrue(true);
    }

    @Test
    public void doReceive(){
        pooledKafka.receive(new ConsumerCallback() {
            public void afterReceive(ConsumerIterator<byte[], byte[]> iterator) {
                while (iterator.hasNext()) {
                    MessageAndMetadata<byte[],byte[]> mm = iterator.next();
                    String message = new String(mm.message());
                    System.out.println(" message:"+message);
                    JSONObject json = new JSONObject(message);
                    System.out.println("aaa");
                }
            }
        },"test222", "test", 3);
        while(true){

        }
    }
}
