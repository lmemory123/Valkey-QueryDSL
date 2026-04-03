package com.momao.valkey.processor;

import com.google.auto.service.AutoService;
import com.momao.valkey.annotation.FieldType;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.annotation.ValkeyDocument;
import com.momao.valkey.annotation.ValkeyId;
import com.momao.valkey.annotation.ValkeyIndexed;
import com.momao.valkey.annotation.ValkeySearchable;
import com.momao.valkey.annotation.ValkeyVector;
import com.momao.valkey.core.NumericFieldBuilder;
import com.momao.valkey.core.TagFieldBuilder;
import com.momao.valkey.core.TextFieldBuilder;
import com.momao.valkey.core.VectorFieldBuilder;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@AutoService(Processor.class)
@SupportedAnnotationTypes("com.momao.valkey.annotation.ValkeyDocument")
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class ValkeyQueryProcessor extends AbstractProcessor {

    private static final String DEFAULT_TAG_SEPARATOR = ",";

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element element : roundEnv.getElementsAnnotatedWith(ValkeyDocument.class)) {
            if (!(element instanceof TypeElement typeElement)) {
                continue;
            }

            generateQueryType(typeElement);
        }
        return true;
    }

    private void generateQueryType(TypeElement typeElement) {
        PackageElement packageElement = processingEnv.getElementUtils().getPackageOf(typeElement);
        String originalClassName = typeElement.getSimpleName().toString();
        ValkeyDocument document = typeElement.getAnnotation(ValkeyDocument.class);
        if (document == null) {
            return;
        }
        String generatedName = originalClassName + resolveQuerySuffix(document);

        ClassName textFieldBuilderClass = ClassName.get(TextFieldBuilder.class);
        ClassName numericFieldBuilderClass = ClassName.get(NumericFieldBuilder.class);
        ClassName tagFieldBuilderClass = ClassName.get(TagFieldBuilder.class);
        ClassName vectorFieldBuilderClass = ClassName.get(VectorFieldBuilder.class);
        ClassName indexSchemaClass = ClassName.get(IndexSchema.class);
        ClassName schemaFieldClass = ClassName.get(SchemaField.class);
        ClassName storageTypeClass = ClassName.get(StorageType.class);
        ClassName distanceMetricClass = ClassName.get("com.momao.valkey.annotation", "DistanceMetric");

        TypeSpec.Builder queryTypeBuilder = TypeSpec.classBuilder(generatedName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        String entityName = document.value().isEmpty() ? originalClassName : document.value();
        queryTypeBuilder.addField(FieldSpec.builder(String.class, "ENTITY_NAME", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer("$S", entityName)
                .build());
        queryTypeBuilder.addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).build());

        StorageType storageType = document.storageType();
        List<QueryNode> queryNodes = collectQueryNodes(typeElement, storageType, List.of(), List.of(), new LinkedHashSet<>());
        addQueryMembers(queryTypeBuilder, queryNodes, textFieldBuilderClass, numericFieldBuilderClass, tagFieldBuilderClass, vectorFieldBuilderClass);
        List<FieldInfo> fieldInfos = flattenFieldInfos(queryNodes);

        String indexName = document.indexName().isEmpty()
                ? "idx:" + originalClassName.toLowerCase(Locale.ROOT)
                : document.indexName();
        List<String> prefixes = resolvePrefixes(document, originalClassName);

        CodeBlock.Builder prefixesBuilder = CodeBlock.builder();
        prefixesBuilder.add("java.util.List.of(");
        for (int i = 0; i < prefixes.size(); i++) {
            if (i > 0) {
                prefixesBuilder.add(", ");
            }
            prefixesBuilder.add("$S", prefixes.get(i));
        }
        prefixesBuilder.add(")");

        CodeBlock.Builder fieldsInitBuilder = CodeBlock.builder();
        fieldsInitBuilder.add("java.util.List.of(\n");
        for (int i = 0; i < fieldInfos.size(); i++) {
            FieldInfo fieldInfo = fieldInfos.get(i);
            if (i > 0) {
                fieldsInitBuilder.add(",\n");
            }
            switch (fieldInfo.fieldType()) {
                case TEXT -> {
                    if (fieldInfo.alias().equals(fieldInfo.jsonPath())) {
                        fieldsInitBuilder.add(
                                "    $T.text($S, $L, $L, $L)",
                                schemaFieldClass,
                                fieldInfo.alias(),
                                Double.toString(fieldInfo.weight()),
                                fieldInfo.noStem(),
                                fieldInfo.sortable());
                    } else {
                        fieldsInitBuilder.add(
                                "    $T.text($S, $S, $L, $L, $L)",
                                schemaFieldClass,
                                fieldInfo.alias(),
                                fieldInfo.jsonPath(),
                                Double.toString(fieldInfo.weight()),
                                fieldInfo.noStem(),
                                fieldInfo.sortable());
                    }
                }
                case TAG -> {
                    if (fieldInfo.alias().equals(fieldInfo.jsonPath())) {
                        fieldsInitBuilder.add(
                                "    $T.tag($S, $S, $L)",
                                schemaFieldClass,
                                fieldInfo.alias(),
                                DEFAULT_TAG_SEPARATOR,
                                fieldInfo.sortable());
                    } else {
                        fieldsInitBuilder.add(
                                "    $T.tag($S, $S, $S, $L)",
                                schemaFieldClass,
                                fieldInfo.alias(),
                                fieldInfo.jsonPath(),
                                DEFAULT_TAG_SEPARATOR,
                                fieldInfo.sortable());
                    }
                }
                case NUMERIC -> {
                    if (fieldInfo.alias().equals(fieldInfo.jsonPath())) {
                        fieldsInitBuilder.add(
                                "    $T.numeric($S, $L)",
                                schemaFieldClass,
                                fieldInfo.alias(),
                                fieldInfo.sortable());
                    } else {
                        fieldsInitBuilder.add(
                                "    $T.numeric($S, $S, $L)",
                                schemaFieldClass,
                                fieldInfo.alias(),
                                fieldInfo.jsonPath(),
                                fieldInfo.sortable());
                    }
                }
                case VECTOR -> {
                    if (fieldInfo.alias().equals(fieldInfo.jsonPath())) {
                        fieldsInitBuilder.add(
                                "    $T.vector($S, $L, $T.$L, $L, $L)",
                                schemaFieldClass,
                                fieldInfo.alias(),
                                fieldInfo.dimension(),
                                distanceMetricClass,
                                fieldInfo.distanceMetric().name(),
                                fieldInfo.m(),
                                fieldInfo.efConstruction());
                    } else {
                        fieldsInitBuilder.add(
                                "    $T.vector($S, $S, $L, $T.$L, $L, $L)",
                                schemaFieldClass,
                                fieldInfo.alias(),
                                fieldInfo.jsonPath(),
                                fieldInfo.dimension(),
                                distanceMetricClass,
                                fieldInfo.distanceMetric().name(),
                                fieldInfo.m(),
                                fieldInfo.efConstruction());
                    }
                }
            }
        }
        fieldsInitBuilder.add("\n)");

        FieldSpec metadataField = FieldSpec.builder(indexSchemaClass, "METADATA", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer(
                        "new $T($S, $T.$L, $L, $L)",
                        indexSchemaClass,
                        indexName,
                        storageTypeClass,
                        storageType.name(),
                        prefixesBuilder.build(),
                        fieldsInitBuilder.build())
                .build();

        queryTypeBuilder.addField(metadataField);

        JavaFile javaFile = JavaFile.builder(packageElement.getQualifiedName().toString(), queryTypeBuilder.build()).build();
        try {
            javaFile.writeTo(processingEnv.getFiler());
        } catch (IOException exception) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, exception.getMessage(), typeElement);
        }
    }

    @SuppressWarnings("deprecation")
    private List<String> resolvePrefixes(ValkeyDocument document, String originalClassName) {
        if (document.prefixes().length > 0) {
            return List.of(document.prefixes());
        }
        if (!document.prefix().isEmpty()) {
            return List.of(document.prefix());
        }
        return List.of(originalClassName.toLowerCase(Locale.ROOT) + ":");
    }

    private String resolveQuerySuffix(ValkeyDocument document) {
        String querySuffix = document.querySuffix();
        if (querySuffix == null || querySuffix.isBlank()) {
            return ValkeyDocument.DEFAULT_QUERY_SUFFIX;
        }
        return querySuffix;
    }

    private List<QueryNode> collectQueryNodes(
            TypeElement typeElement,
            StorageType storageType,
            List<String> propertySegments,
            List<String> aliasSegments,
            Set<String> visitedTypes) {
        List<QueryNode> nodes = new ArrayList<>();
        List<VariableElement> fields = new ArrayList<>();
        collectFields(typeElement, fields);
        for (VariableElement field : fields) {
            QueryNode node = resolveQueryNode(field, storageType, propertySegments, aliasSegments, visitedTypes);
            if (node != null) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    private QueryNode resolveQueryNode(
            VariableElement field,
            StorageType storageType,
            List<String> propertySegments,
            List<String> aliasSegments,
            Set<String> visitedTypes) {
        ValkeyId id = field.getAnnotation(ValkeyId.class);
        ValkeySearchable searchable = field.getAnnotation(ValkeySearchable.class);
        ValkeyIndexed indexed = field.getAnnotation(ValkeyIndexed.class);
        ValkeyVector vector = field.getAnnotation(ValkeyVector.class);

        int annotationCount = countNonNull(id, searchable, indexed, vector);
        if (annotationCount == 0) {
            return null;
        }
        if (annotationCount > 1) {
            processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "同一个字段只能声明一个 Valkey 索引注解",
                    field);
            return null;
        }

        String javaFieldName = field.getSimpleName().toString();

        if (indexed != null && storageType == StorageType.JSON && isNestedObjectType(field.asType())) {
            Element nestedElement = processingEnv.getTypeUtils().asElement(field.asType());
            if (!(nestedElement instanceof TypeElement nestedType)) {
                return null;
            }

            String qualifiedName = nestedType.getQualifiedName().toString();
            if (!visitedTypes.add(qualifiedName)) {
                processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "检测到循环嵌套索引定义: " + qualifiedName,
                        field);
                return null;
            }

            List<QueryNode> children = collectQueryNodes(
                    nestedType,
                    storageType,
                    append(propertySegments, javaFieldName),
                    append(aliasSegments, javaFieldName),
                    new LinkedHashSet<>(visitedTypes));

            if (children.isEmpty()) {
                processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "嵌套对象字段未发现可索引子字段: " + javaFieldName,
                        field);
                return null;
            }
            return new ObjectNode(javaFieldName, toGeneratedTypeName(javaFieldName), children);
        }

        FieldInfo fieldInfo = resolveLeafFieldInfo(field, storageType, propertySegments, aliasSegments);
        if (fieldInfo == null) {
            return null;
        }
        return new LeafNode(javaFieldName, fieldInfo);
    }

    private FieldInfo resolveLeafFieldInfo(
            VariableElement field,
            StorageType storageType,
            List<String> propertySegments,
            List<String> aliasSegments) {
        ValkeyId id = field.getAnnotation(ValkeyId.class);
        ValkeySearchable searchable = field.getAnnotation(ValkeySearchable.class);
        ValkeyIndexed indexed = field.getAnnotation(ValkeyIndexed.class);
        ValkeyVector vector = field.getAnnotation(ValkeyVector.class);

        String javaFieldName = field.getSimpleName().toString();

        if (id != null) {
            NameMapping mapping = resolveNameMapping(javaFieldName, "", field, storageType, propertySegments, aliasSegments);
            return new FieldInfo(mapping.alias(), mapping.jsonPath(), FieldType.TAG, true, 1.0d, false, 0, null, 0, 0);
        }
        if (searchable != null) {
            NameMapping mapping = resolveNameMapping(javaFieldName, searchable.value(), field, storageType, propertySegments, aliasSegments);
            return new FieldInfo(mapping.alias(), mapping.jsonPath(), FieldType.TEXT, searchable.sortable(), searchable.weight(), searchable.noStem(), 0, null, 0, 0);
        }
        if (indexed != null) {
            NameMapping mapping = resolveNameMapping(javaFieldName, indexed.value(), field, storageType, propertySegments, aliasSegments);
            FieldType fieldType = inferFieldType(field.asType());
            return new FieldInfo(mapping.alias(), mapping.jsonPath(), fieldType, indexed.sortable(), 1.0d, false, 0, null, 0, 0);
        }
        if (vector != null) {
            if (storageType != StorageType.JSON) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "向量字段当前仅支持 JSON 存储", field);
                return null;
            }
            if (!isSupportedVectorType(field.asType())) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "向量字段当前仅支持 float[] 或 double[]", field);
                return null;
            }
            NameMapping mapping = resolveNameMapping(javaFieldName, vector.value(), field, storageType, propertySegments, aliasSegments);
            return new FieldInfo(
                    mapping.alias(),
                    mapping.jsonPath(),
                    FieldType.VECTOR,
                    false,
                    1.0d,
                    false,
                    vector.dimension(),
                    vector.distanceMetric(),
                    vector.m(),
                    vector.efConstruction());
        }
        return null;
    }

    private FieldType inferFieldType(TypeMirror type) {
        if (isNumericType(type)) {
            return FieldType.NUMERIC;
        }
        return FieldType.TAG;
    }

    private NameMapping resolveNameMapping(
            String javaFieldName,
            String annotationValue,
            VariableElement field,
            StorageType storageType,
            List<String> propertySegments,
            List<String> aliasSegments) {
        String normalizedAnnotationValue = normalizePath(annotationValue);
        String aliasSegment = normalizedAnnotationValue.isEmpty()
                ? javaFieldName
                : normalizedAnnotationValue;

        if (storageType != StorageType.JSON) {
            String alias = buildAlias(append(aliasSegments, aliasSegment));
            return new NameMapping(alias, alias);
        }

        if (isExplicitJsonPath(normalizedAnnotationValue)) {
            String jsonPath = joinPath(append(propertySegments, normalizedAnnotationValue));
            String alias = buildAlias(append(aliasSegments, javaFieldName));
            return new NameMapping(alias, jsonPath);
        }

        String alias = buildAlias(append(aliasSegments, aliasSegment));
        String propertySegment = isCollectionType(field.asType())
                ? javaFieldName + "[*]"
                : javaFieldName;
        if (isCollectionType(field.asType())) {
            return new NameMapping(alias, joinPath(append(propertySegments, propertySegment)));
        }
        return new NameMapping(alias, joinPath(append(propertySegments, propertySegment)));
    }

    private void addQueryMembers(
            TypeSpec.Builder typeBuilder,
            List<QueryNode> queryNodes,
            ClassName textFieldBuilderClass,
            ClassName numericFieldBuilderClass,
            ClassName tagFieldBuilderClass,
            ClassName vectorFieldBuilderClass) {
        for (QueryNode queryNode : queryNodes) {
            if (queryNode instanceof LeafNode leafNode) {
                ClassName builderClass = switch (leafNode.fieldInfo().fieldType()) {
                    case TAG -> tagFieldBuilderClass;
                    case NUMERIC -> numericFieldBuilderClass;
                    case TEXT -> textFieldBuilderClass;
                    case VECTOR -> vectorFieldBuilderClass;
                };
                typeBuilder.addField(FieldSpec.builder(builderClass, leafNode.memberName(), Modifier.PUBLIC, Modifier.FINAL)
                        .initializer("new $T($S)", builderClass, leafNode.fieldInfo().alias())
                        .build());
                continue;
            }

            ObjectNode objectNode = (ObjectNode) queryNode;
            TypeSpec.Builder nestedTypeBuilder = TypeSpec.classBuilder(objectNode.typeName())
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                    .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).build());
            addQueryMembers(nestedTypeBuilder, objectNode.children(), textFieldBuilderClass, numericFieldBuilderClass, tagFieldBuilderClass, vectorFieldBuilderClass);
            typeBuilder.addType(nestedTypeBuilder.build());
            ClassName nestedTypeClass = ClassName.bestGuess(objectNode.typeName());
            typeBuilder.addField(FieldSpec.builder(nestedTypeClass, objectNode.memberName(), Modifier.PUBLIC, Modifier.FINAL)
                    .initializer("new $T()", nestedTypeClass)
                    .build());
        }
    }

    private List<FieldInfo> flattenFieldInfos(List<QueryNode> queryNodes) {
        List<FieldInfo> fieldInfos = new ArrayList<>();
        for (QueryNode queryNode : queryNodes) {
            if (queryNode instanceof LeafNode leafNode) {
                fieldInfos.add(leafNode.fieldInfo());
                continue;
            }
            fieldInfos.addAll(flattenFieldInfos(((ObjectNode) queryNode).children()));
        }
        return fieldInfos;
    }

    private int countNonNull(Object... values) {
        int count = 0;
        for (Object value : values) {
            if (value != null) {
                count++;
            }
        }
        return count;
    }

    private void collectFields(TypeElement typeElement, List<VariableElement> fields) {
        for (Element enclosedElement : typeElement.getEnclosedElements()) {
            if (enclosedElement instanceof VariableElement variableElement && hasIndexedAnnotation(variableElement)) {
                fields.add(variableElement);
            }
        }

        TypeMirror superclass = typeElement.getSuperclass();
        if (superclass.getKind() != TypeKind.NONE) {
            Element superElement = processingEnv.getTypeUtils().asElement(superclass);
            if (superElement instanceof TypeElement superTypeElement) {
                collectFields(superTypeElement, fields);
            }
        }
    }

    private boolean hasIndexedAnnotation(VariableElement field) {
        return field.getAnnotation(ValkeyId.class) != null
                || field.getAnnotation(ValkeySearchable.class) != null
                || field.getAnnotation(ValkeyIndexed.class) != null
                || field.getAnnotation(ValkeyVector.class) != null;
    }

    private boolean isSupportedVectorType(TypeMirror type) {
        String typeName = type.toString();
        return "float[]".equals(typeName) || "double[]".equals(typeName);
    }

    private boolean isNumericType(TypeMirror type) {
        String typeName = type.toString();
        return typeName.equals("int") || typeName.equals("java.lang.Integer")
                || typeName.equals("long") || typeName.equals("java.lang.Long")
                || typeName.equals("short") || typeName.equals("java.lang.Short")
                || typeName.equals("byte") || typeName.equals("java.lang.Byte")
                || typeName.equals("double") || typeName.equals("java.lang.Double")
                || typeName.equals("float") || typeName.equals("java.lang.Float")
                || typeName.equals("java.math.BigDecimal")
                || typeName.equals("java.math.BigInteger");
    }

    private boolean isCollectionType(TypeMirror type) {
        String typeName = type.toString();
        return typeName.startsWith("java.util.List<")
                || typeName.startsWith("java.util.Set<")
                || typeName.startsWith("java.util.Collection<");
    }

    private boolean isNestedObjectType(TypeMirror type) {
        if (type.getKind().isPrimitive() || isCollectionType(type) || isNumericType(type)) {
            return false;
        }
        String typeName = type.toString();
        if (typeName.equals("java.lang.String")
                || typeName.equals("java.lang.Boolean")
                || typeName.equals("boolean")
                || typeName.equals("java.lang.Character")
                || typeName.equals("char")) {
            return false;
        }

        Element element = processingEnv.getTypeUtils().asElement(type);
        if (!(element instanceof TypeElement typeElement)) {
            return false;
        }
        if (typeElement.getKind() == ElementKind.ENUM) {
            return false;
        }
        String qualifiedName = typeElement.getQualifiedName().toString();
        return !qualifiedName.startsWith("java.");
    }

    private List<String> append(List<String> segments, String value) {
        List<String> appended = new ArrayList<>(segments);
        appended.add(value);
        return appended;
    }

    private String buildAlias(List<String> segments) {
        return String.join("_", segments);
    }

    private String joinPath(List<String> segments) {
        return String.join(".", segments);
    }

    private boolean isExplicitJsonPath(String annotationValue) {
        return annotationValue != null
                && !annotationValue.isEmpty()
                && (annotationValue.contains(".") || annotationValue.contains("[") || annotationValue.contains("*"));
    }

    private String normalizePath(String annotationValue) {
        if (annotationValue == null || annotationValue.isEmpty()) {
            return "";
        }
        return annotationValue.startsWith("$.") ? annotationValue.substring(2) : annotationValue;
    }

    private String toGeneratedTypeName(String memberName) {
        if (memberName == null || memberName.isEmpty()) {
            return "NestedFields";
        }
        return Character.toUpperCase(memberName.charAt(0)) + memberName.substring(1) + "Fields";
    }

    private record NameMapping(String alias, String jsonPath) {
    }

    private record FieldInfo(
            String alias,
            String jsonPath,
            FieldType fieldType,
            boolean sortable,
            double weight,
            boolean noStem,
            int dimension,
            com.momao.valkey.annotation.DistanceMetric distanceMetric,
            int m,
            int efConstruction) {
    }

    private sealed interface QueryNode permits LeafNode, ObjectNode {

        String memberName();
    }

    private record LeafNode(String memberName, FieldInfo fieldInfo) implements QueryNode {
    }

    private record ObjectNode(String memberName, String typeName, List<QueryNode> children) implements QueryNode {
    }
}
