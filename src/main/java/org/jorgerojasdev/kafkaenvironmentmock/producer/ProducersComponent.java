package org.jorgerojasdev.kafkaenvironmentmock.producer;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jorgerojasdev.kafkaenvironmentmock.constants.KEMConstants;
import org.jorgerojasdev.kafkaenvironmentmock.mapper.PropertiesToAvroMapper;
import org.jorgerojasdev.kafkaenvironmentmock.props.event.ProducerProperties;
import org.jorgerojasdev.kafkaenvironmentmock.props.global.GlobalProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
public class ProducersComponent {

    private final GlobalProperties globalProperties;

    private final PropertiesToAvroMapper propertiesToAvroMapper;

    private Map<String, Producer> producerMap = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(ProducersComponent.class);

    @EventListener(ApplicationReadyEvent.class)
    private void generateInitialProducers() throws ClassNotFoundException {
        for (ProducerProperties producer : globalProperties.getInitialProducers()) {
            executeProducer(producer);
        }
    }

    public <T extends SpecificRecord> void executeProducer(ProducerProperties producer) throws ClassNotFoundException {
        if (producer.getFixedScheduleTimeMs() != null) {
            Producer kafkaProducer = this.getOrCreateProducer(producer);

            Executors.newSingleThreadScheduledExecutor().execute(() -> {
                try {
                    Properties object = producer.getObjectToProduce();
                    String namespace = object.getProperty("namespace");
                    String name = object.getProperty("name");
                    Thread.sleep(producer.getDelayMs() > 3000 ? producer.getDelayMs() : 3000);
                    ProducerRecord<Object, T> producerRecord = new ProducerRecord<>(producer.getTopic(), propertiesToAvroMapper.mapPropertiesToAvro(namespace, name, object));
                    kafkaProducer.send(producerRecord);
                    logger.info(String.format("[Producer = %s, Send To Topic: %s, Message: %s]", producer.getOperationId(), producer.getTopic(), producerRecord));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public <K, V> Producer<K, V> getOrCreateProducer(ProducerProperties producerProperties) throws ClassNotFoundException {
        return getOrCreateProducer(
                producerProperties.getOperationId(),
                Class.forName(producerProperties.getKeySerializer()),
                Class.forName(producerProperties.getValueSerializer()));
    }

    public <K, V> Producer<K, V> getOrCreateProducer(String operationId, Class<?> keySerializer,
                                                     Class<?> valueSerializer) {
        if (producerMap.containsKey(operationId)) {
            return producerMap.get(operationId);
        }

        return createProducer(operationId, keySerializer, valueSerializer);
    }

    private <K, V> Producer<K, V> createProducer(String operationId, Class<?> keySerializer,
                                                 Class<?> valueSerializer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KEMConstants.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, String.format("%s-%s", KEMConstants.APPLICATION_NAME, producerMap.size()));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                valueSerializer);
        if (valueSerializer.getName().toLowerCase().contains("avro")) {
            props.put(KEMConstants.SCHEMA_REGISTRY_KEY, KEMConstants.SCHEMA_REGISTRY_VALUE);
        }

        //TODO no engancha con el schema registry
        Producer producer = new KafkaProducer<>(props);
        producerMap.put(operationId, producer);

        return producer;
    }
}
