# Hints

**Spoiler Alert**

We encourage you to work on the assignment yourself or together with your peers. However, situations may present themselves to you where you're stuck on a specific assignment. Thus, this document contains a couple of hints/solutions that ought to guide you through a specific task of the lab assignment.

In any case, don't hesitate to talk to us if you're stuck on a given problem!

## Task 1

### Question 1

Answer 1.

### Question 2

Answer 3.

### Question 3

Answer 3.

### Question 4

Answer 2.

### Question 5

Answer 1.

### Question 5

Answer 2.

## Task 2.1

Hint 1: Create a new class `JsonSerializer` that implements the `Serializer<User>` interface.

Hint 2: The `JsonSerializer` class only needs to override the method `public byte[] serialize(String s, User user)` - every other methods have default implementations that fall back on this one.

Hint 3: The `JsonSerializer` should use an `ObjectMapper` object to serialize the User object to a `byte[]`

Hint 4: One possible implementation for the `JsonSerializer` is:

```java
public class JsonSerializer implements Serializer<User> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String s, User user) {
        try {
            return mapper.writeValueAsBytes(user);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
```

Hint 5: Finally you need to configure your serializer in the `JsonProducer`, the configuration should look like this:

```java
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
```

## Task 2.2

Hint 1: Create a new class `JsonDeserializer` that implements the `Deserializer<User>` interface.

Hint 2: The `JsonDeserializer` class only needs to override the method `public User deserialize(String topic, byte[] data)` - every other methods have default implementations that fall back on this one.

Hint 3: The `JsonDeerializer` should use an `ObjectMapper` object to deserialize the `byte[]` array to a User object.

Hint 4: One possible implementation for the `JsonDeserializer` is:

```java
public class JsonDeserializer implements Deserializer<User> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public User deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, User.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
```

Hint 5: Finally you need to configure your serializer in the `JsonConsumer`, the configuration should look like this:

```java
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
```

## Task 3.1

Hint 1: The deserializer must handle the Jackson `IOException` in a way that allows the consumer to continue. The consumer must ensure that only valid objects are processed/saved.

## Task 3.2

Hint 1: One way to adapt the `JsonDeserializer` is to just catch the exception and return `null`

Hint 2: The `JsonConsumer` then can check if the value of the record is null or not and filter for non-null values.

Hint 3: The `JsonDeserializer` could be implemented like that:

```java
public User deserialize(String topic, byte[] data) {
    try {
        return mapper.readValue(data, User.class);
    } catch (IOException e) {
        return null;
    }
}
```

Hint 4: The `JsonConsumer` could be adapted like this:

```java
ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(200));
records.forEach(record -> {
    if (record != null) {
        observedRecords.add(record);
    }
});
```

## Task 3.3

Hint 1: If we catch the `IOException` and rethrow it as a custom RuntimeException instead of returning `null`, e.g. as a `JsonDeserializationException` we could keep the context of the bad pill.

Hint 2: Such an exception could be written this way:

```java
public class JsonDeserializationException extends RuntimeException {

    private final byte[] faultyMessageString;

    public JsonDeserializationException(byte[] faultyMessage, String message, Throwable ex) {
        this.faultyMessageString = faultyMessage;
    }

    public byte[] getFaultyMessageString() {
        return faultyMessageString;
    }
}
```
Hint 3: Now we can use the Exception in the `JsonDeserializer`

```java
public User deserialize(String topic, byte[] data) {
    try {
        return mapper.readValue(data, User.class);
    } catch (IOException e) {
        throw new JsonDeserializationException(data, e.getMessage(), e);
    }
}
```

Hint 4: The exception will be wrapped in a `RecordDeserializationException` by Kafka, but is still available as cause, so we can and should handle this in the `JsonConsumer`.

Hint 5: The `JsonConsumer` will need to hold an instance of the `DeadLetterProducer` to pass on the bad pill as dead-letter.

Hint 6: The `JsonConsumer` now has everything it needs: The `RecordDeserializationException` with our custom exception as cause and the `DeadLetterProducer`. We now need to handle the exception, get the cause, publish the dead-letter and make sure, that the consumer will skip the faulty message.

Hint 7: The full implementation of the `JsonConsumer` could be:

```java
public class JsonConsumer implements Runnable {


    private final List<ConsumerRecord<String, User>> observedRecords;

    private final KafkaConsumer<String, User> consumer;

    private final DeadLetterProducer deadLetterProducer;

    private boolean keepRunning = true;

    public JsonConsumer(String bootstrapServers) {
        consumer = initializeConsumer(bootstrapServers);
        consumer.subscribe(Collections.singletonList(TOPIC));
        observedRecords = new ArrayList<>();
        // Create and keep an instance of a DeadLetterProducer
        deadLetterProducer = new DeadLetterProducer(bootstrapServers);
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
            try {
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(200));
                records.forEach(observedRecords::add);
            } catch ( RecordDeserializationException ex ) {
                // Seek to the offset after the bad pill
                consumer.seek(ex.topicPartition(), ex.offset() + 1);

                // Ensure that we have the cause we need to publish our dead-letter
                if ( ex.getCause() instanceof JsonDeserializationException ) {
                    // And publish it via the deadLetterProducer
                    deadLetterProducer.publishDeadLetter(((JsonDeserializationException) ex.getCause()).getFaultyMessageString());
                }
            }
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
```

## Task 4

Come on - you made it this far, you can do this on your own!