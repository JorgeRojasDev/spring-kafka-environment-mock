package org.jorgerojasdev.kafkaenvironmentmock.props.event;

import lombok.Data;
import org.jorgerojasdev.kafkaenvironmentmock.exception.AutoconfigureKEMException;

import java.util.Properties;

@Data
public class ProducerProperties extends TopicProperties {

    private String keySerializer;

    private String valueSerializer;

    private Properties objectToProduce;

    public void validate() {
        super.validate();
        String errorField = resolveErrorField();
        if (errorField != null) {
            throw new AutoconfigureKEMException(String.format("Field %s must not be null on producer: %s", errorField, this.getOperationId()));
        }
    }

    private String resolveErrorField() {

        if (keySerializer == null) {
            return "keySerializer";
        }

        if (valueSerializer == null) {
            return "valueSerializer";
        }

        if (objectToProduce == null) {
            return "objecToProduce";
        }

        if (!objectToProduce.containsKey("namespace") && objectToProduce.get("namespace") != null) {
            return "objectToProduce.namespace";
        }

        if (!objectToProduce.containsKey("name") && objectToProduce.get("name") != null) {
            return "objectToProduce.name";
        }

        return null;
    }
}
