package org.jorgerojasdev.kafkaenvironmentmock.props.event;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.jorgerojasdev.kafkaenvironmentmock.exception.AutoconfigureKEMException;
import org.jorgerojasdev.kafkaenvironmentmock.props.global.GlobalProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.ClassPathResource;

import javax.annotation.PostConstruct;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

@ConfigurationProperties(prefix = "event")
@Data
@RequiredArgsConstructor
public class EventProperties {

    private final GlobalProperties globalProperties;

    private List<ProducerProperties> producers = new ArrayList<>();

    private List<ConsumerProperties> consumers = new ArrayList<>();

    private List<String> topics = new ArrayList<>();

    private Map<String, Object> refs = new HashMap<>();

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

        Map<String, Object> referencedObject =
                getJsonRefIfRefObjectIsNull(producerProperties.getOperationId(), ref, (Map<String, Object>) refs.get(ref));

        producerProperties.assignValueFromRef(referencedObject);
    }

    private Map<String, Object> getJsonRefIfRefObjectIsNull(String operationId, String refName, Map<String, Object> referencedObject) {
        if (referencedObject != null) {
            return referencedObject;
        }

        try (JsonReader reader = new JsonReader(new FileReader(new ClassPathResource(String.format("refs/%s.json", refName)).getFile()))) {
            Gson gson = new Gson();
            return gson.fromJson(reader, LinkedHashMap.class);
        } catch (IOException e) {
            throw new AutoconfigureKEMException(String.format("Reference: %s not found in Producer: %s", refName, operationId));
        }
    }

}
