package uk.ac.ed.acp.cw2.controller;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@RestController
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private static final String STUDENT_ID = "s2795419";

    private final RuntimeEnvironment environment;

    public KafkaController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    @PutMapping("/kafka/{writeTopic}/{messageCount}")
    public ResponseEntity<Void> sendMessagesToKafka(
            @PathVariable String writeTopic,
            @PathVariable int messageCount,
            @RequestBody(required = false) String customMessage
    ) {
        logger.info("Producing {} messages to topic {}", messageCount, writeTopic);

        try (var producer = new KafkaProducer<String, String>(getProducerConfig())) {
            for (int i = 0; i < messageCount; i++) {
                String message = (customMessage != null) ?
                        customMessage :
                        String.format("{\"uid\": \"%s\", \"counter\": %d}", STUDENT_ID, i);

                producer.send(new ProducerRecord<>(writeTopic, message));
            }
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            logger.error("Kafka production failed: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/kafka/{readTopic}/{timeoutInMsec}")
    public List<String> getMessagesFromKafka(
            @PathVariable String readTopic,
            @PathVariable int timeoutInMsec
    ) {
        return readMessagesWithTimeout(readTopic, timeoutInMsec);
    }

    public List<String> readMessagesFromKafka(String topic, int messageCount) {
        List<String> messages = new ArrayList<>();
        int maxAttempts = 10;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerConfig())) {
            List<TopicPartition> partitions = getTopicPartitions(consumer, topic);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            int attempts = 0;
            while (messages.size() < messageCount && attempts++ < maxAttempts) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(record -> {
                    if (messages.size() < messageCount) {
                        messages.add(record.value());
                    }
                });
            }
        }
        return messages;
    }

    private Properties getProducerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        return props;
    }

    private Properties getConsumerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        props.put("group.id", "cg-" + UUID.randomUUID());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }

    private List<TopicPartition> getTopicPartitions(Consumer<String, String> consumer, String topic) {
        return consumer.partitionsFor(topic).stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toList());
    }

    private String createMessage(int counter) {
        return String.format(
                "{\"uid\": \"%s\", \"counter\": %d}",
                STUDENT_ID, counter
        );
    }

    private List<String> readMessagesWithTimeout(String topic, int timeoutMs) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerConfig())) {
            List<TopicPartition> partitions = getTopicPartitions(consumer, topic);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeoutMs));
            return records.records(new TopicPartition(topic, 0)) // All partitions
                    .stream()
                    .map(ConsumerRecord::value)
                    .collect(Collectors.toList());
        }
    }
}