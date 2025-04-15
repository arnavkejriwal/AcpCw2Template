package uk.ac.ed.acp.cw2.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

@Configuration
public class AppConfig {
    @Bean
    public RuntimeEnvironment runtimeEnvironment() {
        return RuntimeEnvironment.getEnvironment();
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}