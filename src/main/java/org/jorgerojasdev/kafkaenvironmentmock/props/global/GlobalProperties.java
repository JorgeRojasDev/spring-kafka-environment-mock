package org.jorgerojasdev.kafkaenvironmentmock.props.global;

import lombok.Getter;
import org.jorgerojasdev.kafkaenvironmentmock.exception.AutoconfigureKEMException;
import org.jorgerojasdev.kafkaenvironmentmock.props.common.MockType;
import org.jorgerojasdev.kafkaenvironmentmock.props.event.ConsumerProperties;
import org.jorgerojasdev.kafkaenvironmentmock.props.event.ProducerProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Getter
public class GlobalProperties {

    private Map<String, MockType> operationIds = new HashMap<>();

    private Map<String, ConsumerProperties> consumerPropertiesMap = new HashMap<>();

    private Map<String, ProducerProperties> producerPropertiesMap = new HashMap<>();

    private List<ProducerProperties> initialProducers = new ArrayList<>();

    public void addProducers(List<ProducerProperties> producers) {
        producers.forEach(producer -> {
            validateOperationId(producer.getOperationId());
            producerPropertiesMap.put(producer.getOperationId(), producer);
            operationIds.put(producer.getOperationId(), MockType.PRODUCER);
        });
    }

    public void addConsumers(List<ConsumerProperties> consumers) {
        List<String> initialPreprocessedProducers =
                producerPropertiesMap.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());

        consumers.forEach(consumer -> {
            validateOperationId(consumer.getOperationId());
            consumerPropertiesMap.put(consumer.getOperationId(), consumer);
            consumer.getLaunchOperationIds().forEach(initialPreprocessedProducers::remove);
            operationIds.put(consumer.getOperationId(), MockType.CONSUMER);
        });

        initialProducers = initialPreprocessedProducers.stream().map(initialProducer -> {
            ProducerProperties producer = producerPropertiesMap.get(initialProducer);
            producerPropertiesMap.remove(initialProducer);
            return producer;
        }).collect(Collectors.toList());

    }

    private void validateOperationId(String operationId) {
        if (operationIds.containsKey(operationId)) {
            throw new AutoconfigureKEMException(String.format("OperationId duplicated: %s", operationId));
        }
    }
}
