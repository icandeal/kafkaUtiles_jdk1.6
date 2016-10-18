package com.ycf;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * Created by sniper on 16-10-12.
 */
public class PooledKafka implements InitializingBean {
    private Logger logger = Logger.getLogger(this.getClass());

    private GenericObjectPool<Producer> pool;
    private Resource configLocation;
    private Properties config;

    public PooledKafka (){}

    public void initPooled(Properties config){
        synchronized (this) {
            if(this.pool == null) {
                this.config = config;
                pool = new GenericObjectPool<Producer>(new KafkaPooledProducerFactory(config));
                pool.setMaxIdle(config.containsKey("pool.maxIdle") ? Integer.parseInt(config.getProperty("pool.maxIdle")) : 10);
                pool.setMinIdle(config.containsKey("pool.minIdle") ? Integer.parseInt(config.getProperty("pool.minIdle")) : 3);
                pool.setMaxTotal(config.containsKey("pool.maxTotal") ? Integer.parseInt(config.getProperty("pool.maxTotal")) : 500);
                pool.setMaxWaitMillis(config.containsKey("pool.maxWaitMillis") ? Integer.parseInt(config.getProperty("pool.maxWaitMillis")) : 100000);
            }
        }
    }

    public boolean send(String topic, String key, String value) {
        KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, key, value);

        if (pool != null) {
            try {
                pool.borrowObject().send(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    public boolean send(String topic, String value) {
        KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, value);

        if (pool != null) {
            try {
                pool.borrowObject().send(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    public void receive(ConsumerCallback callback, String groupId, String topic, Integer numPartitions) {
        this.config.setProperty("group.id",groupId);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(this.config));
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put(topic, numPartitions);
        Map<String, List<KafkaStream<byte[],byte[]>>> consumerMap = consumer.createMessageStreams(map);

        List<KafkaStream<byte[],byte[]>> streams = consumerMap.get(topic);

        int threadNumber = 0;
        for (KafkaStream stream : streams) {
            ConsumerThread thread = new ConsumerThread(stream,callback,threadNumber);
            thread.start();
            threadNumber++;
        }
    }

    public Resource getConfigLocation() {
        return configLocation;
    }

    public void setConfigLocation(Resource configLocation) {
        this.configLocation = configLocation;
    }

    public void afterPropertiesSet() throws Exception {
        try {
            Properties config = new Properties();
            config.load(configLocation.getInputStream());
            initPooled(config);
        } catch (IOException e)  {
            logger.error("Properties Load Error !\n",e);
        }
    }
}
