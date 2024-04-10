package workshop.kafka.util;

import com.google.common.base.Stopwatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class GenericConsumer {

    private final KafkaConsumer<String, byte[]> consumer;

    public GenericConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        consumer = new KafkaConsumer<>(props);
    }

    // This is not a very stable and robust thing to do - so please use with caution
    public List<ConsumerRecord<String, byte[]>> pollAll(String topic) {
        var stopwatch = Stopwatch.createStarted();
        consumer.subscribe(Collections.singletonList(topic));
        var retrievedRecords = new ArrayList<ConsumerRecord<String, byte[]>>();

        // Poll for a maximum of 5 seconds
        while (stopwatch.elapsed().toMillis() < 5_000) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(retrievedRecords::add);

            if (!retrievedRecords.isEmpty() && records.isEmpty()) {
                break;
            }
        }
        consumer.unsubscribe();
        return retrievedRecords;
    }
}
