package uk.ac.ed.acp.cw2.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

@Configuration
public class AppConfig {

    // Required for environment variable handling
    @Bean
    public RuntimeEnvironment runtimeEnvironment() {  // Changed to camelCase
        return RuntimeEnvironment.getEnvironment();
    }

    // Required for AcpStorageService integration
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}