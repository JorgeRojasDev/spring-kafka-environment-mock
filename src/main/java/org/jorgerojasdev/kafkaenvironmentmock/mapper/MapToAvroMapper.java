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
                    objectCasted = resolveInteger(object);
                    break;
                case "long":
                case "Long":
                    objectCasted = resolveLong(object);
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

    private Integer resolveInteger(Object object) {
        if (Double.class.equals(object.getClass())) {
            return ((Double) object).intValue();
        }

        return Integer.valueOf(object.toString());
    }

    private Long resolveLong(Object object) {
        if (Double.class.equals(object.getClass())) {
            return ((Double) object).longValue();
        }

        return Long.valueOf(object.toString());
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
        Object value = properties.get(field.getName());
        if (value == null) {
            return;
        }
        if (checkIfClassIsAutoAssignable(field.getType())) {
            field.set(objectToAssign, castToFinalObject(field.getType(), value));
            return;
        }

        if (field.getType().isEnum()) {
            assignEnum(field, objectToAssign, properties);
            return;
        }
        this.treatComplexObject(field, objectToAssign, properties);
    }

    private <T, E extends Enum<E>> void assignEnum(Field field, T objectToAssign, Map<String, Object> properties) throws IllegalAccessException {
        Object value = properties.get(field.getName());
        if (value == null) {
            return;
        }
        Class<E> enumClazz = (Class<E>) field.getType();
        field.set(objectToAssign, Enum.valueOf(enumClazz, String.valueOf(value)));
    }

    private <T> void treatComplexObject(Field field, Object objectToAssign, Map<String, Object> properties) throws InvocationTargetException, IllegalAccessException, InstantiationException {
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
        Object objectToProcess = properties.get(field.getName());

        if (List.class.isAssignableFrom(objectToProcess.getClass())) {
            return getTreatedListFromAnotherList(listType, field, properties);
        }

        return getTreatedListFromMap(listType, field, properties);
    }

    private <T> List<T> getTreatedListFromMap(Class<T> listType, Field field, Map<String, Object> properties) {
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

    private <T> List<T> getTreatedListFromAnotherList(Class<T> listType, Field field, Map<String, Object> properties) {
        if (checkIfClassIsAutoAssignable(listType)) {
            return getTreatedListFromListAutoAssignable(listType, field, properties);
        }
        return getTreatedListFromMapListNonAutoAssignable(listType, field, properties);
    }

    private <T> List<T> getTreatedListFromListAutoAssignable(Class<T> listType, Field field, Map<String, Object> properties) {
        List<T> list = new ArrayList<>();
        ((List<Object>) properties.get(field.getName())).forEach((map) -> {
            T object;
            object = this.castToFinalObject(listType, map);
            list.add(object);
        });
        return list;
    }

    private <T> List<T> getTreatedListFromMapListNonAutoAssignable(Class<T> listType, Field field, Map<String, Object> properties) {
        List<T> list = new ArrayList<>();
        ((List<Map<String, Object>>) properties.get(field.getName())).forEach((map) -> {
            T object;

            try {
                object = this.getComplexObjectFromMap(listType, map);
            } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
                throw new AutoconfigureKEMException(String.format("Error casting object: %s", map), e);
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
