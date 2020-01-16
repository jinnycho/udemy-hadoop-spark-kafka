package com.github.jinnycho503.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("--- Stopping application ---");
            twitterClient.stop();
            kafkaProducer.close();
            logger.info("--- Done stopping application ---");
        }));

        while (!twitterClient.isDone()) {
            String twitterMsg = null;
            try {
                twitterMsg = msgQueue.poll(5, TimeUnit.SECONDS);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_topic", twitterMsg);
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info("Successfully Received new metadata: \n" + "Topic: " + recordMetadata.topic() + "\n" + "Partition: " + recordMetadata.partition() + "\n" + "Offset: " + recordMetadata.offset() + "\n" + "Timestamp: " + recordMetadata.timestamp() + "\n");
                        } else {
                            logger.error("Error while producing tweets");
                        }
                    }
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        // Declare the connection information
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("dog", "cat");
        hosebirdEndpoint.trackTerms(terms);

        String consumerKey = "dS4U9f2EbmsctsIp1LGsYhPHN";
        String consumerSecret = "A9uhXRWiMBRUvnM6KmmZgkJp6fse2d5tgJVFy7qIttDipfwfV5";
        String token = "3004406534-iQC8JDHeePjUe3A4vu1glyMa7lVxXViDWm19cEc";
        String secret = "U4Gcy4tKR08NJcPvSfL2mM0agCBTYLFgihIO3f4ZzJ10z";
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer() {
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
