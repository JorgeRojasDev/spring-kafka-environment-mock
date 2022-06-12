package org.jorgerojasdev.kafkaenvironmentmock.topic;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@EnableKafka
@Configuration
@Order(0)
@Slf4j
public class TopicConfig {

    @Value("#{'${event.topics}'.replaceAll(' ','').split(',')}")
    private List<String> topics;

    private boolean initialized = false;

    @Bean
    public KafkaAdmin.NewTopics createTopics(KafkaAdmin admin) {
        admin.setAutoCreate(false);
        List<NewTopic> newTopics =
                topics.stream().map(topic -> TopicBuilder.name(topic).build()).collect(Collectors.toList());
        NewTopic[] topicsArray = new NewTopic[newTopics.size()];
        return new KafkaAdmin.NewTopics(
                newTopics.toArray(topicsArray)
        );
    }

    @Bean
    public ApplicationRunner runner(KafkaAdmin admin) {
        return args -> {
            try (AdminClient client = AdminClient.create(admin.getConfigurationProperties())) {
                waitUntilTopicsHaveBeenCreated(client);
            }
        };
    }

    private void waitUntilTopicsHaveBeenCreated(AdminClient adminClient) throws ExecutionException, InterruptedException {

        for (String topic : topics) {
            waitUntilTopicHasBeenCreated(adminClient, topic);
        }
    }

    private void waitUntilTopicHasBeenCreated(AdminClient adminClient, String topic) throws ExecutionException, InterruptedException {

        List<String> topicsKafkaDomain = new ArrayList<>(adminClient.listTopics().names().get());

        if (!initialized) {
            initialized = true;
            log.info("Waiting for topics info...");
            while (!topicsKafkaDomain.contains(topic)) {
                Thread.sleep(1000L);
                topicsKafkaDomain = new ArrayList<>(adminClient.listTopics().names().get());
            }
            log.info("All topics are running!");
        }

    }
}
