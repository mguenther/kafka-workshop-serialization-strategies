# Lab Assignment

## Task 1: Serialization (Multiple Choice)

Before we dive into the coding exercise, let's take a minute to refresh a couple of things that we've just learned in the session before. We have assorted a couple of questions for you to test your understanding of the topic.


### Question 1

What is the primary purpose of serialization in distributed systems?

1. To convert a data object into a series of bytes for transmission.
2. To increase data processing speed on a single system.
3. To encrypt data for secure transmission.
4. To replicate data across multiple databases.

### Question 2

Which of the following is **NOT** a challenge typically associated with serialization?

1. Managing changes in data structure.
2. Transferring large datasets.
3. Reducing data redundancy.
4. Computational overhead.

### Question 3

What does Kafka primarily provide with regard to (de)serialization?

1. Custom serialization formats.
2. Data contract enforcement.
3. Built-in mechanisms for handling serialization.
4. Encryption of data in transit.

### Question 4

What advantage does the Schema Registry provide?

1. It decentralizes schema management.
2. It supports schema evolution with compatibility checks.
3. It eliminates the need for serialization.
4. It simplifies Kafka's internal architecture.

### Question 5

What is the biggest drawback of embedding schemas within each message payload?
 
1. Increases message size.
2. Increases computational time.
3. Complex schema management.
4. Decreases data transmission speed.

### Question 6

Why might you choose a serialization format that supports schema evolution?

1. To reduce the need for data compression
2. To ensure backward and forward compatibility
3. To increase the security of data transmissions
4. To comply with international data handling standards


## Task 2: JSONSerializer & JSONDeserializer

The class `JsonProducer` is a minimal bootstrap of a Kafka Producer for our domain object `User`. Sadly it's missing the capability to serialize the object.
`
1. Implement a custom `Serializer`, that is able to serialize objects of the type `User` to `byte[]` and wire it with the `JsonProducer`. You are finished whenever the `JsonProducerTest.serialization` test is green.

2. Implement a custom `Deserializer`, that is able to read the produced messages and returns the `User` object. The goal is for the `JsonSerializationTest.deserialization` test to run successfully. If you already finished Task 2.1 - the `JsonSerializationTest.serdes` test should also be green.


## Task 3: Dead letter topic

There is another test class named `DeadLetterQueueTest`. This class will publish rubbish - as other producers might do - in your user topic. 

1. With your implementation from Task 2: analyze what's going wrong when running the `DeadLetterQueueTest.badMessageDoesNotBlockTheConsumer`. Where can and should this be handled so a bad message does not block our consumer?

2. Adapt your code to handle the deserialization errors - for this task: just ignore every message that can't be deserialized. You are done when the `DeadLetterQueueTest.badMessageDoesNotBlockTheConsumer` test is green.

3. In most cases it's a good idea to keep the bad messages for later analysis. This might give use the information we need to find a possible bug or gap in our understanding of the domain. Using the `DeadLetterProducer.produceDeadLetter` helper-method, adapt your code to sort every bad message away into our dead-letter topic. 

## Task 4: (Optional) Avro without Schema registry

The drawback of the JSONApproach is that the producer and consumer have to be in sync regarding the data model and JSON-Annotations. Another approach - as discussed in the talk - is to use a schema that is embedded in every message. This way every message if self-contained and should be readable by any consumer that understands the schema. Take a peek at the `avro` package. 

1. Implement a custom `Serializer`, that is able to serialize objects of the type `User` to `byte[]` via avro and wire it with the `AvroProducer`. You should use the helper methods for serialization, that the `AvroUserHelper` object provides. Write your own test if you want for this part - this time we only provided a singular test for Serialization + Deserialization

2. Implement a custom `Deserializer`, that is able to read the produced messages and returns the `User` object and configure it in the `AvroConsumer`. Again you should use the helper method in the `AvroUserHelper` class. The goal is for the `AvroTestBase` test(s) to run successfully.

3. Now it's time to take a deeper look at what the `AvroUserHelper` actually provides. Analyze the four methods and try to reason about the following questions:
   1. What is the benefit and drawback of a read-schema with Avro compared to a precompiled class from the schema?
   2. What would happen with the read-schema and compiled class if we'd evolve the schema (e.g. adding another field).
   3. Try to come up with use-cases where you'd prefer one over the other.

## That's it! You've done great!

You have completed all assignments. If you have any further questions or need clarification, please don't hesitate to reach out to us. We're here to help.