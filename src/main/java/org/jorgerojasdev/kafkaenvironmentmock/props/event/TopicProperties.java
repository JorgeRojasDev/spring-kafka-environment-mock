package org.jorgerojasdev.kafkaenvironmentmock.props.event;

import lombok.Data;
import org.jorgerojasdev.kafkaenvironmentmock.exception.AutoconfigureKEMException;

@Data
public abstract class TopicProperties {

    private String operationId;

    private String topic;

    private Long delayMs = 0L;

    private Long fixedScheduleTimeMs = 0L;

    public void validate() {
        String errorField = null;

        if (operationId == null) {
            errorField = "Operation Id";
        }

        if (topic == null) {
            errorField = "Topic";
        }

        if (errorField != null) {
            throw new AutoconfigureKEMException(String.format("%s must not be null", errorField));
        }
    }
}
