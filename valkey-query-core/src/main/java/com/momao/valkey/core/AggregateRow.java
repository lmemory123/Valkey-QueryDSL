package com.momao.valkey.core;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record AggregateRow(Map<String, Object> values) {

    private static final ClassValue<RowObjectMapper<?>> ROW_MAPPERS = new ClassValue<>() {
        @Override
        protected RowObjectMapper<?> computeValue(Class<?> type) {
            return createRowMapper(type);
        }
    };

    public AggregateRow {
        values = values == null ? Map.of() : Map.copyOf(values);
    }

    public Object get(String field) {
        return values.get(field);
    }

    public String getString(String field) {
        Object value = values.get(field);
        return value == null ? null : String.valueOf(value);
    }

    public Long getLong(String field) {
        Object value = values.get(field);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    public Double getDouble(String field) {
        Object value = values.get(field);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return Double.parseDouble(String.valueOf(value));
    }

    public <R> R toObject(Class<R> type) {
        if (type == null) {
            throw new IllegalArgumentException("Aggregate row target type cannot be null");
        }
        try {
            return mapperFor(type).map(this);
        } catch (Throwable throwable) {
            if (throwable instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException("Failed to map aggregate row to " + type.getName(), throwable);
        }
    }

    @SuppressWarnings("unchecked")
    private <R> RowObjectMapper<R> mapperFor(Class<R> type) {
        return (RowObjectMapper<R>) ROW_MAPPERS.get(type);
    }

    private static RowObjectMapper<?> createRowMapper(Class<?> type) {
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(type, MethodHandles.lookup());
            if (type.isRecord()) {
                RecordComponent[] components = type.getRecordComponents();
                Class<?>[] parameterTypes = new Class<?>[components.length];
                String[] names = new String[components.length];
                for (int i = 0; i < components.length; i++) {
                    parameterTypes[i] = components[i].getType();
                    names[i] = components[i].getName();
                }
                MethodHandle constructor = lookup.findConstructor(type, java.lang.invoke.MethodType.methodType(void.class, parameterTypes));
                return new RecordRowMapper(type, constructor, names, parameterTypes);
            }
            MethodHandle constructor = lookup.findConstructor(type, java.lang.invoke.MethodType.methodType(void.class));
            List<FieldBinding> bindings = new ArrayList<>();
            Class<?> current = type;
            while (current != null && current != Object.class) {
                MethodHandles.Lookup currentLookup = MethodHandles.privateLookupIn(current, MethodHandles.lookup());
                for (Field field : current.getDeclaredFields()) {
                    bindings.add(new FieldBinding(field.getName(), field.getType(), currentLookup.unreflectSetter(field)));
                }
                current = current.getSuperclass();
            }
            return new BeanRowMapper(type, constructor, bindings);
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Failed to initialize aggregate row mapper for " + type.getName(), exception);
        }
    }

    private static Object convertValue(Object value, Class<?> targetType) {
        if (value == null) {
            if (targetType.isPrimitive()) {
                if (targetType == boolean.class) {
                    return false;
                }
                if (targetType == char.class) {
                    return '\0';
                }
                return 0;
            }
            return null;
        }
        if (targetType.isInstance(value)) {
            return value;
        }
        if (targetType == String.class) {
            return String.valueOf(value);
        }
        if (targetType == Long.class || targetType == long.class) {
            return value instanceof Number number ? number.longValue() : Long.parseLong(String.valueOf(value));
        }
        if (targetType == Integer.class || targetType == int.class) {
            return value instanceof Number number ? number.intValue() : Integer.parseInt(String.valueOf(value));
        }
        if (targetType == Double.class || targetType == double.class) {
            return value instanceof Number number ? number.doubleValue() : Double.parseDouble(String.valueOf(value));
        }
        if (targetType == Float.class || targetType == float.class) {
            return value instanceof Number number ? number.floatValue() : Float.parseFloat(String.valueOf(value));
        }
        if (targetType == Boolean.class || targetType == boolean.class) {
            return value instanceof Boolean bool ? bool : Boolean.parseBoolean(String.valueOf(value));
        }
        if (targetType.isEnum()) {
            @SuppressWarnings({"rawtypes", "unchecked"})
            Enum<?> enumValue = Enum.valueOf((Class<? extends Enum>) targetType.asSubclass(Enum.class), String.valueOf(value));
            return enumValue;
        }
        return value;
    }

    @FunctionalInterface
    private interface RowObjectMapper<R> {
        R map(AggregateRow row) throws Throwable;
    }

    private record FieldBinding(String name, Class<?> type, MethodHandle setter) {
    }

    private record RecordRowMapper(
            Class<?> type,
            MethodHandle constructor,
            String[] componentNames,
            Class<?>[] componentTypes
    ) implements RowObjectMapper<Object> {

        @Override
        public Object map(AggregateRow row) throws Throwable {
            Object[] args = new Object[componentNames.length];
            for (int i = 0; i < componentNames.length; i++) {
                args[i] = convertValue(row.values.get(componentNames[i]), componentTypes[i]);
            }
            return constructor.invokeWithArguments(args);
        }
    }

    private record BeanRowMapper(
            Class<?> type,
            MethodHandle constructor,
            List<FieldBinding> bindings
    ) implements RowObjectMapper<Object> {

        @Override
        public Object map(AggregateRow row) throws Throwable {
            Object target = constructor.invokeWithArguments();
            for (FieldBinding binding : bindings) {
                if (!row.values.containsKey(binding.name())) {
                    continue;
                }
                binding.setter().invoke(target, convertValue(row.values.get(binding.name()), binding.type()));
            }
            return target;
        }
    }
}
