package org.jorgerojasdev.kafkaenvironmentmock.props.event;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.jorgerojasdev.kafkaenvironmentmock.exception.AutoconfigureKEMException;
import org.jorgerojasdev.kafkaenvironmentmock.props.global.GlobalProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "event")
@Data
@RequiredArgsConstructor
public class EventProperties {

    private final GlobalProperties globalProperties;

    private List<ProducerProperties> producers = new ArrayList<>();

    private List<ConsumerProperties> consumers = new ArrayList<>();

    private List<String> topics = new ArrayList<>();

    private Map<String, Object> components = new HashMap<>();

    @PostConstruct
    public void autoConfigure() {
        producers.forEach(this::validateProducer);
        consumers.forEach(this::validate);
        addGlobalConfig();
    }

    private void addGlobalConfig() {
        addGlobalProducers();
        addGlobalConsumers();
    }

    private void addGlobalProducers() {
        globalProperties.addProducers(producers);
    }

    private void addGlobalConsumers() {
        globalProperties.addConsumers(consumers);
    }

    private <T extends TopicProperties> void validate(T topicProperties) {
        topicProperties.validate();
        String topic = topicProperties.getTopic();
        if (!topics.contains(topic)) {
            throw new AutoconfigureKEMException(String.format("Topic: %s must be named on event.topics", topic));
        }
    }

    private void validateProducer(ProducerProperties producerProperties) {
        this.validate(producerProperties);
        this.assignRefValue(producerProperties);
    }

    private void assignRefValue(ProducerProperties producerProperties) {
        String ref = producerProperties.getRef();

        if (ref == null) {
            return;
        }

        Map<String, Object> referencedObject = (Map<String, Object>) components.get(ref);

        if (referencedObject == null) {
            throw new AutoconfigureKEMException(String.format("Reference: %s not found in Producer: %s", ref, producerProperties.getOperationId()));
        }

        producerProperties.assignValueFromRef(referencedObject);
    }

}
