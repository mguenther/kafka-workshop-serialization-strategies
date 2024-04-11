package workshop.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import workshop.kafka.json.DeadLetterProducer;
import workshop.kafka.json.JsonConsumer;
import workshop.kafka.json.JsonProducer;
import workshop.kafka.model.User;
import workshop.kafka.util.GenericConsumer;
import workshop.kafka.util.GenericProducer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class DeadLetterQueueTest {

    private EmbeddedKafkaCluster kafka;

    private JsonProducer producer;

    private JsonConsumer consumer;

    private final ObjectMapper mapper = new ObjectMapper();


    @BeforeEach
    void prepareTest() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
        kafka.createTopic(withName(JsonProducer.TOPIC).useDefaults());
        kafka.createTopic(withName(DeadLetterProducer.TOPIC).useDefaults());
    }

    @AfterEach
    void tearDown() {
        kafka.stop();
    }

    @Test
    @DisplayName("Task 3.1: A bad pill should not crash/block the consumer")
    void badMessageDoesNotBlockTheConsumer() throws Exception {
        producer = new JsonProducer(kafka.getBrokerList());
        consumer = new JsonConsumer(kafka.getBrokerList());
        var genericProducer = new GenericProducer(kafka.getBrokerList());

        var users = List.of(
                new User("Erna von und vor dem Müll", 42),
                new User("Gerhard zum und hinter dem Müll", 65)
        );

        producer.publishUser(users.get(0));
        genericProducer.publish(JsonProducer.TOPIC, "This obviously is not a well structured message".getBytes(StandardCharsets.UTF_8));
        producer.publishUser(users.get(1));

        var consumerThread = new Thread(consumer);
        consumerThread.start();

        await().atMost(Duration.ofSeconds(10))
                .until(() -> consumer.observedRecordsCount() >= 2);

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
    @DisplayName("Task 3.2: A bad pill should be published in the dead letter topic")
    void produceDeadLetter() throws Exception {
        producer = new JsonProducer(kafka.getBrokerList());
        consumer = new JsonConsumer(kafka.getBrokerList());
        var genericProducer = new GenericProducer(kafka.getBrokerList());

        var users = List.of(
                new User("Erna von und vor dem Müll", 42),
                new User("Gerhard zum und hinter dem Müll", 65)
        );

        producer.publishUser(users.get(0));
        var badPillMessage = "This obviously is not a well structured message".getBytes(StandardCharsets.UTF_8);
        genericProducer.publish(JsonProducer.TOPIC, badPillMessage);
        producer.publishUser(users.get(1));

        var consumerThread = new Thread(consumer);
        consumerThread.start();

        await().atMost(Duration.ofSeconds(10))
                .until(() -> consumer.observedRecordsCount() >= 2);

        consumer.stop();
        consumerThread.join(2_000);

        var genericConsumer = new GenericConsumer(kafka.getBrokerList());
        var records = genericConsumer.pollAll(DeadLetterProducer.TOPIC);

        assertThat(records).hasSize(1);
        assertThat(records.get(0).value()).isEqualTo(badPillMessage);
    }

}
