package org.jorgerojasdev.kafkaenvironmentmock.props.event;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ConsumerProperties extends TopicProperties {

    private List<String> launchOperationIds = new ArrayList<>();
}
