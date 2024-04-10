package workshop.kafka;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import workshop.kafka.avro.AvroConsumer;
import workshop.kafka.avro.AvroProducer;
import workshop.kafka.model.User;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class AvroTestBase {

    private EmbeddedKafkaCluster kafka;

    private AvroProducer producer;

    private AvroConsumer consumer;


    @BeforeEach
    void prepareTest() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
        kafka.createTopic(withName(AvroProducer.TOPIC).useDefaults());
    }

    @AfterEach
    void tearDown() {
        kafka.stop();
    }

    @Test
    @DisplayName("Task 4: AVRO Serialization & Deserialization works together as intended")
    void serdes() throws Exception {

        producer = new AvroProducer(kafka.getBrokerList());
        consumer = new AvroConsumer(kafka.getBrokerList());

        var users = List.of(
                new User("Max Mustermann", 26),
                new User("Erika Musterfrau", 22));

        users.forEach(producer::publishUser);

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
}
