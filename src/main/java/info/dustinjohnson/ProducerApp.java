package info.dustinjohnson;

// modeled from: https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerApp
{

    private final static String SERVERS = "192.168.42.12:9092";
    private final static String TOPIC = "testaroo";

    public static void main(String [] args) {

        org.apache.log4j.BasicConfigurator.configure(); // use quicktest configuration (do not use for production)

        Properties props = new Properties();
        props.put("bootstrap.servers", SERVERS);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // create some dummy records
        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String, String>(TOPIC, Integer.toString(i), Integer.toString(i)));

        producer.close();
    }
}

