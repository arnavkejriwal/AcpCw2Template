package uk.ac.ed.acp.cw2.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class StorageService {
    private final RestTemplate restTemplate;
    private final String storageUrl;

    public StorageService(@Value("${ACP_STORAGE_SERVICE}") String storageUrl) {
        this.storageUrl = storageUrl;
        this.restTemplate = new RestTemplate();
    }

    public String storeMessage(String message) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(message, headers);

        ResponseEntity<String> response = restTemplate.exchange(
                storageUrl + "/api/v1/blob",
                HttpMethod.POST,
                request,
                String.class
        );

        String uuid = response.getBody();
        if (uuid != null && uuid.startsWith("\"") && uuid.endsWith("\"")) {
            uuid = uuid.substring(1, uuid.length() - 1);
        }
        return uuid;
    }
}