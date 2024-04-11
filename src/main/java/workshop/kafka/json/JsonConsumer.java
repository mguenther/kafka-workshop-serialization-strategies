package workshop.kafka.json;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import workshop.kafka.model.User;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static workshop.kafka.json.JsonProducer.TOPIC;

public class JsonConsumer implements Runnable {


    private final List<ConsumerRecord<String, User>> observedRecords;

    private final KafkaConsumer<String, User> consumer;

    private boolean keepRunning = true;

    public JsonConsumer(String bootstrapServers) {
        consumer = initializeConsumer(bootstrapServers);
        consumer.subscribe(Collections.singletonList(TOPIC));
        observedRecords = new ArrayList<>();
    }

    private static KafkaConsumer<String, User> initializeConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-group");

        return new KafkaConsumer<>(props);
    }

    public void run() {
        while (keepRunning) {
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(200));
            records.forEach(observedRecords::add);
        }
    }

    public void stop() {
        keepRunning = false;
        consumer.wakeup();
    }

    public Stream<ConsumerRecord<String, User>> getObservedRecords() {
        return observedRecords.stream();
    }

    public int observedRecordsCount() {
        return observedRecords.size();
    }

}
