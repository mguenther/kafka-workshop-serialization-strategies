package workshop.kafka.json;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class DeadLetterProducer {

    public static final String TOPIC = "dead-letter";

    private final KafkaProducer<String, byte[]> producer;

    DeadLetterProducer(String bootstrapServers) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    public void publishDeadLetter(byte[] message) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, null, message);
        producer.send(record);
    }
}
