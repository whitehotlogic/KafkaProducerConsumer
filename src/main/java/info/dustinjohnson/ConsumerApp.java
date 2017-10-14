package info.dustinjohnson;

// modeled from https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html

import java.util.Properties;
import java.io.IOException;
import java.util.UUID;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerApp {

    private final static String SERVERS = "192.168.42.12:9092";

    private final static String TOPIC = "testaroo";
    // confirmed topic exists: testaroo
    // confirmed topic exists: test
    // confirmed topic exists: test2


    public static void main(String[] args) throws IOException {

        org.apache.log4j.BasicConfigurator.configure(); // use log4j quicktest configuration (for testing -- not production)

        Properties props = new Properties();
        props.put("bootstrap.servers", SERVERS);
        //props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.intervals.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //key is a string (for testing -- not production)
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value is a string (for testing -- not production)
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); // let's look like a different consumer
                                                                                 //    group each run, so i can grab all messages
                                                                                 //    (for testing -- not production)


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));

        while (true) {

            // For testing, just grab all messages from this topic from the beginning of time
            //     which simulates:
            //     kafka-console-consumer.sh --from-beginning

            consumer.poll(0); // at this point, there is no heartbeat from consumer
                                      //    so seekToBeginning() wont work yet
                                      //    so call poll()

            consumer.seekToBeginning(consumer.assignment()); // Now there is heartbeat and consumer is "alive"

            ConsumerRecords<String, String> records = consumer.poll(0); // Now consume

            for (ConsumerRecord<String, String> record : records) // get all the messages and log them to console
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }



    }


}
