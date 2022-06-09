package org.jorgerojasdev.kafkaenvironmentmock.topic;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.List;
import java.util.stream.Collectors;

@EnableKafka
@Configuration
public class TopicConfig {

    @Value("#{'${event.topics}'.replaceAll(' ','').split(',')}")
    private List<String> topics;

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
}
