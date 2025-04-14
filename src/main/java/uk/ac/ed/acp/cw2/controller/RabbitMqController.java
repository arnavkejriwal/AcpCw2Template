package uk.ac.ed.acp.cw2.controller;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

@RestController
public class RabbitMqController {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqController.class);
    private static final String STUDENT_ID = "s2795419"; // Replace with your ID

    private final ConnectionFactory factory;
    private final RuntimeEnvironment environment;

    public RabbitMqController(RuntimeEnvironment environment) {
        this.environment = environment;
        this.factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());
    }

    @PutMapping("/rabbitMq/{queueName}/{messageCount}")
    public ResponseEntity<Void> sendMessages(
            @PathVariable String queueName,
            @PathVariable int messageCount
    ) {
        logger.info("Writing {} messages to queue {}", messageCount, queueName);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            for (int counter = 0; counter < messageCount; counter++) {
                String message = String.format(
                        "{\"uid\": \"%s\", \"counter\": %d}",
                        STUDENT_ID, counter
                );
                channel.basicPublish("", queueName, null, message.getBytes());
                logger.debug("Published message: {}", message);
            }

            return ResponseEntity.ok().build();

        } catch (IOException | TimeoutException e) {
            logger.error("RabbitMQ write failed: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/rabbitMq/{queueName}/{timeoutInMsec}")
    public List<String> getMessages(
            @PathVariable String queueName,
            @PathVariable int timeoutInMsec
    ) {
        logger.info("Reading from queue {} with {}ms timeout", queueName, timeoutInMsec);
        final List<String> messages = Collections.synchronizedList(new ArrayList<>());
        final long startTime = System.currentTimeMillis();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                messages.add(new String(delivery.getBody(), StandardCharsets.UTF_8));
                logger.debug("Received message: {}", delivery.getBody());
            };

            String consumerTag = channel.basicConsume(queueName, true, deliverCallback, ct -> {});

            // Precise timeout handling (max timeoutInMsec + 200ms)
            while ((System.currentTimeMillis() - startTime) < (timeoutInMsec + 200)) {
                Thread.sleep(50); // Reduced polling interval
            }

        } catch (IOException | TimeoutException | InterruptedException e) {
            logger.error("RabbitMQ read failed: {}", e.getMessage());
            return Collections.emptyList();
        }

        logger.info("Returning {} messages from queue {}", messages.size(), queueName);
        return new ArrayList<>(messages);
    }

    // Helper method used by ServiceController
    public void sendToQueue(String queueName, String message) {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);
            channel.basicPublish("", queueName, null, message.getBytes());

        } catch (IOException | TimeoutException e) {
            logger.error("Failed to send message to {}: {}", queueName, e.getMessage());
            throw new RuntimeException("RabbitMQ write failed", e);
        }
    }
}