package workshop.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import workshop.kafka.json.JsonConsumer;
import workshop.kafka.json.JsonProducer;
import workshop.kafka.model.User;
import workshop.kafka.util.GenericConsumer;
import workshop.kafka.util.GenericProducer;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class JsonSerializationTest {

    private EmbeddedKafkaCluster kafka;

    private JsonProducer producer;

    private JsonConsumer consumer;

    private final ObjectMapper mapper = new ObjectMapper();


    @BeforeEach
    void prepareTest() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
        kafka.createTopic(withName(JsonProducer.TOPIC).useDefaults());
    }

    @AfterEach
    void tearDown() {
        kafka.deleteTopic(JsonProducer.TOPIC);
        kafka.stop();
    }

    @Test
    @DisplayName("Task 2.1: JSON Serialization works")
    void serialization() {
        producer = new JsonProducer(kafka.getBrokerList());
        var users = List.of(
                new User("Heinz Harald", 56),
                new User("Harald Heinz", 65)
        );

        users.forEach(producer::publishUser);

        var records = new GenericConsumer(kafka.getBrokerList()).pollAll(JsonProducer.TOPIC);

        var observedValues = records.stream()
                .map(ConsumerRecord::value)
                .map(this::fromByteArray)
                .collect(Collectors.toList());

        assertThat(observedValues).hasSize(2);
        assertThat(observedValues)
                .usingRecursiveFieldByFieldElementComparator()
                .containsAll(users);
    }

    @Test
    @DisplayName("Task 2.2: JSON Deserialization works")
    void deserialization() throws Exception {
        consumer = new JsonConsumer(kafka.getBrokerList());
        var users = List.of(
                new User("Heinz Harald", 56),
                new User("Harald Heinz", 65)
        );

        var genericProducer = new GenericProducer(kafka.getBrokerList());
        users.forEach(user -> genericProducer.publish(JsonProducer.TOPIC, toByteArray(user)));


        var consumerThread = new Thread(consumer);
        consumerThread.start();

        await().atMost(Duration.ofSeconds(10))
                .until(() -> consumer.observedRecordsCount() == 2);

        var observedValues = consumer
                .getObservedRecords()
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());

        assertThat(observedValues)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyInAnyOrderElementsOf(users);

        consumer.stop();
        consumerThread.join(2_000);
    }

    @Test
    @DisplayName("Task 2: JSON Serialization & Deserialization works together as intended")
    void serdes() throws Exception {

        producer = new JsonProducer(kafka.getBrokerList());
        consumer = new JsonConsumer(kafka.getBrokerList());

        var users = List.of(
                new User("Max Mustermann", 26),
                new User("Erika Musterfrau", 22));

        users.forEach(producer::publishUser);

        var consumerThread = new Thread(consumer);
        consumerThread.start();

        await().atMost(Duration.ofSeconds(10))
                .until(() -> consumer.observedRecordsCount() == 2);

        var observedValues = consumer
                .getObservedRecords()
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());

        assertThat(observedValues)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyInAnyOrderElementsOf(users);

        consumer.stop();
        consumerThread.join(2_000);
    }

    private User fromByteArray(byte[] serializedForm) {
        try {
            return mapper.readValue(serializedForm, User.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private byte[] toByteArray(User user) {
        try {
            return mapper.writeValueAsBytes(user);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
