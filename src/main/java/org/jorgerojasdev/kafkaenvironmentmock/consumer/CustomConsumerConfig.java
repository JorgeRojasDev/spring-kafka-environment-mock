package org.jorgerojasdev.kafkaenvironmentmock.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jorgerojasdev.kafkaenvironmentmock.constants.KEMConstants;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class CustomConsumerConfig {
    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KEMConstants.BOOTSTRAP_SERVERS);
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                KEMConstants.APPLICATION_NAME);
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class);
        props.put(KEMConstants.SCHEMA_REGISTRY_KEY, KEMConstants.SCHEMA_REGISTRY_VALUE);
        return new DefaultKafkaConsumerFactory<>(props);
    }
    
}
