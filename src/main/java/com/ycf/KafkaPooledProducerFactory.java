package com.ycf;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.Properties;

/**
 * Created by sniper on 16-10-12.
 */
public class KafkaPooledProducerFactory extends BasePooledObjectFactory<Producer> {
    private Properties properties;

    public KafkaPooledProducerFactory (Properties properties){
        this.properties = properties;
    }

    public Producer create() throws Exception {
        return new Producer(new ProducerConfig(properties));
    }

    public PooledObject<Producer> wrap(Producer kafkaProducer) {
        return new DefaultPooledObject<Producer>(kafkaProducer);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
