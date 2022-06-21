package org.jorgerojasdev.kafkaenvironmentmock.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jorgerojasdev.kafkaenvironmentmock.constants.KEMConstants;
import org.jorgerojasdev.kafkaenvironmentmock.mapper.MapToAvroMapper;
import org.jorgerojasdev.kafkaenvironmentmock.props.event.ProducerProperties;
import org.jorgerojasdev.kafkaenvironmentmock.props.global.GlobalProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class ProducersComponent {

    private final GlobalProperties globalProperties;

    private final MapToAvroMapper mapToAvroMapper;

    private final Map<String, Producer> producerMap = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(ProducersComponent.class);

    @EventListener(ApplicationReadyEvent.class)
    private void generateInitialProducers() throws ClassNotFoundException {
        for (ProducerProperties producer : globalProperties.getInitialProducers()) {
            Long initialDelayMs = producer.getDelayMs() > 3000 ? producer.getDelayMs() : 3000;
            executeProducer(producer, initialDelayMs, producer.getFixedScheduleTimeMs());
        }
    }

    public <T extends SpecificRecord> void executeProducer(ProducerProperties producer, Long initialDelayMs, Long fixedRateMs) throws ClassNotFoundException {
        Runnable produceAction = getRunnableProducerAction(producer, initialDelayMs);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        if (fixedRateMs >= 5000) {
            executorService.scheduleAtFixedRate(produceAction, 0, fixedRateMs, TimeUnit.MILLISECONDS);
        } else {
            executorService.execute(produceAction);
        }
    }

    public <K, V> Producer<K, V> getOrCreateProducer(ProducerProperties producerProperties) throws ClassNotFoundException {
        return getOrCreateProducer(
                producerProperties.getOperationId(),
                Class.forName(producerProperties.getKeySerializer()));
    }

    public <K, V> Producer<K, V> getOrCreateProducer(String operationId, Class<?> keySerializer) {
        if (producerMap.containsKey(operationId)) {
            return producerMap.get(operationId);
        }

        return createProducer(operationId, keySerializer);
    }

    private <T> Runnable getRunnableProducerAction(ProducerProperties producerProperties, Long initialDelayMs) throws ClassNotFoundException {
        Producer kafkaProducer = this.getOrCreateProducer(producerProperties);

        return () -> {
            try {
                Map<String, Object> object = producerProperties.getRecord();
                String namespace = object.get("namespace").toString();
                String name = object.get("name").toString();
                Thread.sleep(initialDelayMs);
                ProducerRecord<Object, T> producerRecord = new ProducerRecord<>(producerProperties.getTopic(), null, new Date().getTime(), producerProperties.getKey(), (T) mapToAvroMapper.mapToAvro(namespace, name, object));
                kafkaProducer.send(producerRecord);
                logger.info(String.format("[Producer = %s, Send To Topic: %s, Message: %s]", producerProperties.getOperationId(), producerProperties.getTopic(), producerRecord));
            } catch (Exception e) {
                logger.error(String.format("Error Sending message on producer. OperationId: %s", producerProperties.getOperationId()), e);
            }
        };
    }

    private <K, V> Producer<K, V> createProducer(String operationId, Class<?> keySerializer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KEMConstants.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, String.format("%s-%s", KEMConstants.APPLICATION_NAME, producerMap.size()));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class);
        props.put(KEMConstants.SCHEMA_REGISTRY_KEY, KEMConstants.SCHEMA_REGISTRY_VALUE);

        Producer producer = new KafkaProducer<>(props);
        producerMap.put(operationId, producer);

        return producer;
    }
}
