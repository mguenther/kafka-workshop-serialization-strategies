package workshop.kafka.avro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import workshop.kafka.model.User;

import java.util.Properties;

public class AvroProducer {

    public static final String TOPIC = "avro-embedded-user";

    private final KafkaProducer<String, User> producer;

    public AvroProducer(String bootstrapServers) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    public void publishUser(User user) {
        ProducerRecord<String, User> record = new ProducerRecord<>(TOPIC, null, user);
        producer.send(record);
    }
}
