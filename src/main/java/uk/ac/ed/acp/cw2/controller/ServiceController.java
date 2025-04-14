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
import uk.ac.ed.acp.cw2.service.StorageService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@RestController
public class ServiceController {

    private static final Logger logger = LoggerFactory.getLogger(ServiceController.class);
    private final RuntimeEnvironment environment;
    private static final String STUDENT_ID = "s2795419"; // Replace with your ID

    @Autowired
    private StorageService storageService;

    @Autowired
    private RabbitMqController rabbitMqController;

    @Autowired
    private KafkaController kafkaController;

    public ServiceController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    @GetMapping("/")
    public String index() {
        StringBuilder currentEnv = new StringBuilder();
        currentEnv.append("<ul>");
        System.getenv().keySet().forEach(key -> currentEnv.append("<li>").append(key).append(" = ").append(System.getenv(key)).append("</li>"));
        currentEnv.append("</ul>");

        return "<html><body>" +
                "<h1>Welcome from ACP CW2</h1>" +
                "<h2>Environment variables </br><div> " + currentEnv.toString() + "</div></h2>" +
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
        ObjectMapper mapper = new ObjectMapper();

        messages.forEach(rawMessage -> {
            try {
                JsonNode message = mapper.readTree(rawMessage);
                String key = message.get("key").asText();

                if (key.length() == 3 || key.length() == 4) {
                    // Process good message
                    double value = message.get("value").asDouble();
                    goodTotal.set(goodTotal.get() + value);

                    ObjectNode modifiedMessage = (ObjectNode) message;
                    modifiedMessage.put("runningTotalValue", goodTotal.get());

                    String uuid = storageService.storeMessage(modifiedMessage.toString());
                    modifiedMessage.put("uuid", uuid);

                    rabbitMqController.sendToQueue(
                            request.getWriteQueueGood(),
                            modifiedMessage.toString()
                    );
                } else {
                    // Process bad message
                    badTotal.set(badTotal.get() + message.get("value").asDouble());
                    rabbitMqController.sendToQueue(
                            request.getWriteQueueBad(),
                            rawMessage
                    );
                }
            } catch (Exception e) {
                logger.error("Error processing message: {}", e.getMessage());
            }
        });

        // Send TOTAL messages
        sendTotalMessage(request.getWriteQueueGood(), goodTotal.get());
        sendTotalMessage(request.getWriteQueueBad(), badTotal.get());

        return ResponseEntity.ok().build();
    }

    private void sendTotalMessage(String queueName, double total) {
        String message = String.format(
                "{\"uid\": \"%s\", \"key\": \"TOTAL\", \"value\": %.2f, \"comment\": \"\"}",
                STUDENT_ID, total
        );
        rabbitMqController.sendToQueue(queueName, message);
    }
}