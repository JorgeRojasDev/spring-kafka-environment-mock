package org.jorgerojasdev.kafkaenvironmentmock.mapper;

import org.apache.avro.specific.SpecificRecord;
import org.jorgerojasdev.kafkaenvironmentmock.exception.AutoconfigureKEMException;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.*;

@Component
public class PropertiesToAvroMapper {

    public <T extends SpecificRecord> T mapPropertiesToAvro(String namespace, String avro, Properties properties) {
        try {
            Class<?> avroClass = Class.forName(String.format("%s.%s", namespace, avro));
            return (T) getComplexObjectFromProperties(avroClass, "props", properties);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new AutoconfigureKEMException("Error parsing object to avro, check your object definition");
        }
    }

    private boolean checkIfClassIsAutoAssignable(Class<?> clazz) {
        if (clazz.getGenericSuperclass() == null) {
            return true;
        }

        if (Number.class.isAssignableFrom(clazz)) {
            return true;
        }

        if (String.class.equals(clazz)) {
            return true;
        }

        return false;
    }


    private <T> T castToFinalObject(Class<T> classToCast, Object object) {
        try {
            if (object == null) {
                return null;
            }
            return classToCast.cast(object);
        } catch (ClassCastException e) {
            Object objectCasted;
            switch (classToCast.getName()) {
                case "int":
                    objectCasted = Integer.valueOf(object.toString());
                    break;
                case "long":
                    objectCasted = Long.valueOf(object.toString());
                    break;
                case "float":
                    objectCasted = Float.valueOf(object.toString());
                    break;
                case "double":
                    objectCasted = Double.valueOf(object.toString());
                    break;
                case "boolean":
                    objectCasted = Boolean.valueOf(object.toString());
                    break;
                default:
                    throw new AutoconfigureKEMException("Can't cast object", e);
            }
            return (T) objectCasted;
        }
    }

    private <T> T getComplexObjectFromProperties(Class<T> clazz, String prefix, Properties properties) throws IllegalAccessException, InvocationTargetException, InstantiationException {
        T object = (T) clazz.getDeclaredConstructors()[0].newInstance();
        List<Field> fields = Arrays.asList(clazz.getDeclaredFields());
        for (Field field : fields) {
            if (field.getModifiers() == Modifier.PUBLIC) {
                if (checkIfClassIsAutoAssignable(field.getType())) {
                    field.set(object, castToFinalObject(field.getType(), properties.get(resolvePropertyKey(prefix, field.getName()))));
                } else {
                    this.treatComplexObject(field.getType(), field.getName(), prefix, properties);
                }
            }
        }
        return object;
    }

    private <T> void treatComplexObject(Class<T> fieldClass, String fieldName, String prefix, Properties properties) throws InvocationTargetException, IllegalAccessException, InstantiationException {
        //TODO specialTreatments
        if (Map.class.isAssignableFrom(fieldClass)) {
            return;
        }
        if (Collection.class.isAssignableFrom(fieldClass)) {
            return;
        }
        this.getComplexObjectFromProperties(fieldClass, resolvePropertyKey(prefix, fieldName), properties);
    }

    private String resolvePropertyKey(String... args) {
        return String.join(".", Arrays.asList(args));
    }
}
