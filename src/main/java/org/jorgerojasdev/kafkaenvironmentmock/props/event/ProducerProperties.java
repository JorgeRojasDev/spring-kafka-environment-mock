package org.jorgerojasdev.kafkaenvironmentmock.props.event;

import lombok.Data;
import org.jorgerojasdev.kafkaenvironmentmock.exception.AutoconfigureKEMException;

import java.util.HashMap;
import java.util.Map;

@Data
public class ProducerProperties extends TopicProperties {

    private String keySerializer;

    private String key;

    private Map<String, Object> record = new HashMap<>();

    private Long delayMs = 0L;

    private Long fixedScheduleTimeMs = 0L;

    public void validate() {
        super.validate();
        String errorField = resolveErrorField();
        if (errorField != null) {
            throw new AutoconfigureKEMException(String.format("Field %s must not be null on producer: %s", errorField, this.getOperationId()));
        }
    }

    public String getRef() {
        Object ref = record.get("ref");
        if (ref == null) {
            return null;
        }
        return String.valueOf(ref);
    }

    public void assignValueFromRef(Map<String, Object> value) {
        record.put("value", value);
    }

    private String resolveErrorField() {

        if (keySerializer == null) {
            return "keySerializer";
        }

        if (record == null) {
            return "record";
        }

        if (!record.containsKey("namespace") && record.get("namespace") != null) {
            return "record.namespace";
        }

        if (!record.containsKey("name") && record.get("name") != null) {
            return "record.name";
        }

        if (!record.containsKey("value") && !record.containsKey("ref")) {
            return "record.value || record.ref";
        }

        return null;
    }
}
