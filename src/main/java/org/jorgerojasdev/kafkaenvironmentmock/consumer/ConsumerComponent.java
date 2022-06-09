package org.jorgerojasdev.kafkaenvironmentmock.consumer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jorgerojasdev.kafkaenvironmentmock.producer.ProducersComponent;
import org.jorgerojasdev.kafkaenvironmentmock.props.common.MockType;
import org.jorgerojasdev.kafkaenvironmentmock.props.event.ConsumerProperties;
import org.jorgerojasdev.kafkaenvironmentmock.props.event.ProducerProperties;
import org.jorgerojasdev.kafkaenvironmentmock.props.global.GlobalProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class ConsumerComponent {

    private final GlobalProperties globalProperties;

    private final ProducersComponent producersComponent;

    private Map<String, List<String>> topicsAndConsumersMap = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(ConsumerComponent.class);

    @EventListener(ApplicationReadyEvent.class)
    private void autoConfigure() {
        globalProperties.getConsumerPropertiesMap().forEach((operationId, consumerProps) -> {
            if (topicsAndConsumersMap.containsKey(consumerProps.getTopic())) {
                topicsAndConsumersMap.get(consumerProps.getTopic()).add(operationId);
            } else {
                List<String> newOperationIds = new ArrayList<>();
                newOperationIds.add(operationId);
                topicsAndConsumersMap.put(consumerProps.getTopic(), newOperationIds);
            }
        });
    }

    @KafkaListener(topics = "#{'${event.topics}'.replaceAll(' ','').split(',')}", groupId = "mytopicconsumer")
    private void listen(@Payload ConsumerRecord<Object, Object> record) throws ClassNotFoundException {
        String topicName = record.topic();
        if (!topicsAndConsumersMap.containsKey(record.topic())) {
            return;
        }

        for (String consumerOperationId : topicsAndConsumersMap.get(topicName)) {
            ConsumerProperties consumerProperties = globalProperties.getConsumerPropertiesMap().get(consumerOperationId);
            logger.info(String.format("[Consumer = %s, Received Message From Topic: %s, Next Operations: [%s]]", consumerOperationId, topicName, String.join(", ", consumerProperties.getLaunchOperationIds())));
            for (String launchOperationId : consumerProperties.getLaunchOperationIds()) {
                if (MockType.PRODUCER.equals(globalProperties.getOperationIds().get(launchOperationId))) {
                    ProducerProperties producerProperties = globalProperties.getProducerPropertiesMap().get(launchOperationId);
                    producersComponent.executeProducer(producerProperties);
                }
            }
        }
    }
}
