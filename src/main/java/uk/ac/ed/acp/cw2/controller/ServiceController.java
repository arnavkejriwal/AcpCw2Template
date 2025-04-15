package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.ProcessMessagesRequest;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.data.TransformMessagesRequest;
import uk.ac.ed.acp.cw2.service.StorageService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@RestController
public class ServiceController {

    private static final Logger logger = LoggerFactory.getLogger(ServiceController.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private final RuntimeEnvironment environment;
    private static final String STUDENT_ID = "s2795419";

    @Autowired private StorageService storageService;
    @Autowired private RabbitMqController rabbitMqController;
    @Autowired private KafkaController kafkaController;
    @Autowired private RedisController redisController;

    public ServiceController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    @GetMapping("/")
    public String index() {
        StringBuilder envVars = new StringBuilder("<ul>");
        System.getenv().forEach((key, value) ->
                envVars.append("<li>").append(key).append(" = ").append(value).append("</li>"));
        envVars.append("</ul>");

        return "<html><body>" +
                "<h1>Welcome from ACP CW2</h1>" +
                "<h2>Environment variables <div>" + envVars.toString() + "</div></h2>" +
                "</body></html>";
    }

    @PostMapping("/processMessages")
    public ResponseEntity<Void> processMessages(@RequestBody ProcessMessagesRequest request) {
        List<String> messages = kafkaController.readMessagesFromKafka(
                request.getReadTopic(),
                request.getMessageCount()
        );

        AtomicReference<Double> goodTotal = new AtomicReference<>(0.0);
        AtomicReference<Double> badTotal = new AtomicReference<>(0.0);

        messages.forEach(rawMessage -> {
            try {
                JsonNode message = mapper.readTree(rawMessage);
                String key = message.get("key").asText();

                if (isGoodMessage(key)) {
                    processGoodMessage(message, request.getWriteQueueGood(), goodTotal);
                } else {
                    processBadMessage(rawMessage, request.getWriteQueueBad(), badTotal);
                }
            } catch (IOException e) {
                logger.error("Failed to process message: {}", e.getMessage());
            }
        });

        sendTotalMessage(request.getWriteQueueGood(), goodTotal.get());
        sendTotalMessage(request.getWriteQueueBad(), badTotal.get());

        return ResponseEntity.ok().build();
    }

    @PostMapping("/transformMessages")
    public ResponseEntity<Void> transformMessages(@RequestBody TransformMessagesRequest request) {
        List<String> messages = rabbitMqController.readMessagesForTransform(
                request.getReadQueue(),
                request.getMessageCount()
        );

        AtomicInteger totalProcessed = new AtomicInteger(0);
        AtomicInteger totalWritten = new AtomicInteger(0);
        AtomicInteger redisUpdates = new AtomicInteger(0);
        AtomicReference<Double> totalValue = new AtomicReference<>(0.0);
        AtomicReference<Double> totalAdded = new AtomicReference<>(0.0);

        messages.forEach(rawMessage -> processTransformMessage(
                rawMessage,
                request.getWriteQueue(),
                totalProcessed,
                totalWritten,
                redisUpdates,
                totalValue,
                totalAdded
        ));

        return ResponseEntity.ok().build();
    }

    private void sendTotalMessage(String queueName, double total) {
        String message = String.format(
                "{\"uid\": \"%s\", \"key\": \"TOTAL\", \"value\": %.2f, \"comment\": \"\"}",
                STUDENT_ID, total
        );
        rabbitMqController.sendToQueue(queueName, message);
    }

    private boolean isGoodMessage(String key) {
        int length = key.length();
        return length == 3 || length == 4;
    }

    private void processGoodMessage(JsonNode message, String queue, AtomicReference<Double> total)
            throws IOException {
        double value = message.get("value").asDouble();
        total.set(total.get() + value);

        ObjectNode modified = ((ObjectNode) message)
                .put("runningTotalValue", total.get())
                .put("uuid", storageService.storeMessage(message.toString()));

        rabbitMqController.sendToQueue(queue, modified.toString());
    }

    private void processBadMessage(String rawMessage, String queue, AtomicReference<Double> total) {
        try {
            double value = mapper.readTree(rawMessage).get("value").asDouble();
            total.set(total.get() + value);
            rabbitMqController.sendToQueue(queue, rawMessage);
        } catch (IOException e) {
            logger.error("Invalid bad message: {}", rawMessage);
        }
    }

    private void processTransformMessage(
            String rawMessage,
            String writeQueue,
            AtomicInteger totalProcessed,
            AtomicInteger totalWritten,
            AtomicInteger redisUpdates,
            AtomicReference<Double> totalValue,
            AtomicReference<Double> totalAdded
    ) {
        try {
            JsonNode message = mapper.readTree(rawMessage);
            totalProcessed.incrementAndGet();

            if (isTombstone(message)) {
                handleTombstone(
                        message, writeQueue,
                        totalProcessed, totalWritten, redisUpdates,
                        totalValue, totalAdded
                );
            } else {
                handleNormalMessage(
                        message, writeQueue,
                        redisUpdates, totalWritten,
                        totalValue, totalAdded
                );
            }
        } catch (IOException e) {
            logger.error("Failed to parse message: {}", rawMessage);
        }
    }

    private boolean isTombstone(JsonNode message) {
        return !message.has("version") || !message.has("value");
    }

    private void handleTombstone(
            JsonNode message,
            String writeQueue,
            AtomicInteger totalProcessed,
            AtomicInteger totalWritten,
            AtomicInteger redisUpdates,
            AtomicReference<Double> totalValue,
            AtomicReference<Double> totalAdded
    ) {
        String key = message.get("key").asText();
        redisController.deleteKey(key);
        redisUpdates.incrementAndGet();

        ObjectNode summary = mapper.createObjectNode()
                .put("totalMessagesProcessed", totalProcessed.get())
                .put("totalMessagesWritten", totalWritten.get() + 1)
                .put("totalRedisUpdates", redisUpdates.get())
                .put("totalValueWritten", totalValue.get())
                .put("totalAdded", totalAdded.get());

        rabbitMqController.sendToQueue(writeQueue, summary.toString());
        totalWritten.incrementAndGet();
    }

    private void handleNormalMessage(
            JsonNode message,
            String writeQueue,
            AtomicInteger redisUpdates,
            AtomicInteger totalWritten,
            AtomicReference<Double> totalValue,
            AtomicReference<Double> totalAdded
    ) {
        try {
            String key = message.get("key").asText();
            int version = message.get("version").asInt();
            double value = message.get("value").asDouble();

            Integer storedVersion = redisController.getVersion(key);
            if (storedVersion == null || version > storedVersion) {
                processNewVersion(message, writeQueue, key, version, value, redisUpdates, totalAdded);
                totalValue.set(totalValue.get() + value + 10.5);
            } else {
                rabbitMqController.sendToQueue(writeQueue, message.toString());
                totalValue.set(totalValue.get() + value);
            }
            totalWritten.incrementAndGet();
        } catch (Exception e) {
            logger.error("Failed to process normal message: {}", e.getMessage());
        }
    }

    private void processNewVersion(
            JsonNode message,
            String writeQueue,
            String key,
            int version,
            double value,
            AtomicInteger redisUpdates,
            AtomicReference<Double> totalAdded
    ) throws IOException {
        double newValue = value + 10.5;
        ObjectNode modified = ((ObjectNode) message).put("value", newValue);

        redisController.setVersion(key, version);
        rabbitMqController.sendToQueue(writeQueue, modified.toString());

        totalAdded.set(totalAdded.get() + 10.5);
        redisUpdates.incrementAndGet();
    }
}