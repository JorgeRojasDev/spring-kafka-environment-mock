package org.jorgerojasdev.kafkaenvironmentmock.exception;

public class AutoconfigureKEMException extends RuntimeException {

    public AutoconfigureKEMException(String message) {
        super(message);
    }

    public AutoconfigureKEMException(String message, Throwable e) {
        super(message, e);
    }
}
