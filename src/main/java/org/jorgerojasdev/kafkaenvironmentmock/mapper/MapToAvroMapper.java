package org.jorgerojasdev.kafkaenvironmentmock.mapper;

import org.apache.avro.specific.SpecificRecord;
import org.jorgerojasdev.kafkaenvironmentmock.exception.AutoconfigureKEMException;
import org.springframework.stereotype.Component;

import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class MapToAvroMapper {

    public <T extends SpecificRecord> T mapToAvro(String namespace, String avro, Map<String, Object> properties) {
        try {
            Class<?> avroClass = Class.forName(String.format("%s.%s", namespace, avro));
            return (T) getComplexObjectFromMap(avroClass, (Map<String, Object>) properties.get("value"));
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new AutoconfigureKEMException("Error parsing object to avro, check your object definition");
        }
    }

    private boolean checkIfClassIsAutoAssignable(Class<?> clazz) {
        if (Map.class.isAssignableFrom(clazz) || Collection.class.isAssignableFrom(clazz)) {
            return false;
        }

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
            switch (classToCast.getSimpleName()) {
                case "int":
                case "Integer":
                    objectCasted = Integer.valueOf(object.toString());
                    break;
                case "long":
                case "Long":
                    objectCasted = Long.valueOf(object.toString());
                    break;
                case "float":
                case "Float":
                    objectCasted = Float.valueOf(object.toString());
                    break;
                case "double":
                case "Double":
                    objectCasted = Double.valueOf(object.toString());
                    break;
                case "boolean":
                case "Boolean":
                    objectCasted = Boolean.valueOf(object.toString());
                    break;
                default:
                    throw new AutoconfigureKEMException("Can't cast object", e);
            }
            return (T) objectCasted;
        }
    }

    private <T> T getComplexObjectFromMap(Class<T> clazz, Map<String, Object> properties) throws IllegalAccessException, InvocationTargetException, InstantiationException {
        T object = getEmptyConstructor(clazz).newInstance();
        List<Field> fields = Arrays.asList(clazz.getDeclaredFields());
        for (Field field : fields) {
            if (field.getModifiers() == Modifier.PUBLIC) {
                assignValue(field, object, properties);
            }
        }
        return object;
    }

    private <T> void assignValue(Field field, T objectToAssign, Map<String, Object> properties) throws IllegalAccessException, InvocationTargetException, InstantiationException {
        if (checkIfClassIsAutoAssignable(field.getType())) {
            field.set(objectToAssign, castToFinalObject(field.getType(), properties.get(field.getName())));
        } else {
            this.treatComplexObject(field, objectToAssign, properties);
        }
    }

    private <T> void treatComplexObject(Field field, Object objectToAssign, Map<String, Object> properties) throws InvocationTargetException, IllegalAccessException, InstantiationException {
        //TODO specialTreatments
        if (Map.class.isAssignableFrom(field.getType())) {
            field.set(objectToAssign, properties.get(field.getName()) != null ? getTreatedMap(field, properties) : new HashMap<>());
            return;
        }
        if (Collection.class.isAssignableFrom(field.getType())) {
            field.set(objectToAssign, properties.get(field.getName()) != null ? getTreatedList(field, properties) : new ArrayList<>());
            return;
        }
        this.getComplexObjectFromMap(field.getType(), (Map<String, Object>) properties.get(field.getName()));
    }

    private <T> Map<String, T> getTreatedMap(Field field, Map<String, Object> properties) {
        Class<T> valueType = getGenericInferredClass(field.getGenericType(), 1);
        Map<String, T> map = new HashMap<>();
        ((Map<String, Object>) properties.get(field.getName())).forEach((key, value) -> {
            T object;
            if (this.checkIfClassIsAutoAssignable(valueType)) {
                object = this.castToFinalObject(valueType, value);
            } else {
                try {
                    object = this.getComplexObjectFromMap(valueType, (Map<String, Object>) value);
                } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
                    throw new AutoconfigureKEMException(String.format("Error casting object: %s", value), e);
                }
            }
            map.put(key, object);
        });
        return map;
    }

    private <T> List<T> getTreatedList(Field field, Map<String, Object> properties) {
        Class<T> listType = getGenericInferredClass(field.getGenericType(), 0);
        List<T> list = new ArrayList<>();
        ((Map<String, Object>) properties.get(field.getName())).forEach((key, value) -> {
            T object;
            if (this.checkIfClassIsAutoAssignable(listType)) {
                object = this.castToFinalObject(listType, value);
            } else {
                try {
                    object = this.getComplexObjectFromMap(listType, (Map<String, Object>) value);
                } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
                    throw new AutoconfigureKEMException(String.format("Error casting object: %s", value), e);
                }
            }
            list.add(object);
        });
        return list;
    }

    private <T> Class<T> getGenericInferredClass(Type genericType, int genericIndexType) {
        ParameterizedType parameterizedType = (ParameterizedType) genericType;
        return (Class<T>) parameterizedType.getActualTypeArguments()[genericIndexType];
    }

    private <T> Constructor<T> getEmptyConstructor(Class<T> clazz) {
        return (Constructor<T>) Arrays.asList(
                clazz.getDeclaredConstructors()).stream()
                .filter(constructor -> constructor.getParameters().length == 0).collect(Collectors.toList()).get(0);
    }
}
