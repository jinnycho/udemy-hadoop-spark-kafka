package kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String offset_reset_config = "latest";
        String topic1 = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Take bytes and convert them to string
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset_reset_config); // option: earliest, latest, none

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek = mostly used to replay data or fetch a specific msg

        // 1. assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic1, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // 2. seek (go to specific offset)
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        // Why don't we exit after 5 messages to read
        int numOfMsgToRead = 5;
        int numOfMsgReadSofar = 0;
        boolean keepOnReading = true;

        // poll new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numOfMsgReadSofar += 1;
                logger.info("Key: " + record.key() + "\n" + "Value: " + record.value() + "\n" + "Partition: " + record.partition() + "\n" + "Offset: " + record.offset());
                if (numOfMsgReadSofar >= numOfMsgToRead) {
                    keepOnReading = false;
                    break; // exit for loop
                }
            }
        }
        logger.info("Exiting the application");
    }
}
