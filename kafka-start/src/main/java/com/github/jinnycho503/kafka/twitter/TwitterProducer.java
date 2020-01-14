package com.github.jinnycho503.kafka.twitter;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class TwitterProducer {
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

    }

    public KafkaProducer<String, String> createKafkaProducer() {
        final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

        // Producer Properties
        String bootstrapServerAddr = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerAddr);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
