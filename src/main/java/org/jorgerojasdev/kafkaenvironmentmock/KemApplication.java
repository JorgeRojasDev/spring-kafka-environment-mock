package org.jorgerojasdev.kafkaenvironmentmock;

import org.jorgerojasdev.kafkaenvironmentmock.props.event.EventProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({EventProperties.class})
public class KemApplication {

    public static void main(String[] args) {
        SpringApplication.run(KemApplication.class, args);
    }

}
