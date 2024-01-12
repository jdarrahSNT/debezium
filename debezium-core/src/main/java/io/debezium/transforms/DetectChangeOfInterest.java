package io.debezium.transforms;
/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.transforms.SmtManager;
import io.debezium.util.BoundedConcurrentHashMap;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class DetectChangeOfInterest<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DetectChangeOfInterest.class);
    public static final String FIELDS_CONF = "fields.of.interest";
    public static final String HEADER_KEY_CONF = "header.changed.name";
    public static final String CHANGE_FIELD_NAME = "change.field.name";
    private static final int CACHE_SIZE = 64;
    public static final String ROOT_FIELD_NAME = "payload";
    public static final String FIELD_PREFIX_NAME = "change.field.prefix";

    public static final Field HEADER_KEY = Field.create(HEADER_KEY_CONF)
            .withDisplayName("Header change name")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Header key containing the changed columns provided in message header. In most cases it should match the name used in the ExtractChangeRecordState transformation")
            .required();

    public static final Field FIELDS_FIELD = Field.create(FIELDS_CONF)
            .withDisplayName("Fields names list")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(
                    Field::notContainSpaceInAnyElement,
                    Field::notContainEmptyElements)
            .withDescription(
                    "Field names, of the columns we care about being updated (fields of interest)")
            .required();
    public static final Field CHANGE_FIELD_NAME_FIELD = Field.create(CHANGE_FIELD_NAME)
            .withDisplayName("Change field name")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Name of field that holds a 1 (change detected) or 0 (no change detected)")
            .withDefault("change_of_interest");

    public static final Field FIELD_PREFIX = Field.create(FIELD_PREFIX_NAME).withDisplayName("change field prefix")
            .withType(ConfigDef.Type.STRING).withImportance(ConfigDef.Importance.LOW)
            .withDescription("Prefix for change field name").withDefault("__");

    private List<String> fields;
    private String headerKey;
    private String fieldPrefix = "__";
    private String changeFieldName = "change_of_interest";

    private final BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache = new BoundedConcurrentHashMap<>(CACHE_SIZE);
    private final BoundedConcurrentHashMap<Headers, Headers> headersUpdateCache = new BoundedConcurrentHashMap<>(CACHE_SIZE);

    @Override
    public ConfigDef config() {

        final ConfigDef config = new ConfigDef();
        Field.group(config, null, HEADER_KEY, FIELDS_FIELD, CHANGE_FIELD_NAME_FIELD, FIELD_PREFIX);
        return config;
    }

    @Override
    public void configure(Map<String, ?> props) {

        final Configuration config = Configuration.from(props);
        SmtManager<R> smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(FIELDS_FIELD, CHANGE_FIELD_NAME_FIELD, HEADER_KEY, CHANGE_FIELD_NAME_FIELD, FIELD_PREFIX));

        fields = config.getList(FIELDS_FIELD);
        headerKey = config.getString(HEADER_KEY);
        changeFieldName = config.getString(CHANGE_FIELD_NAME_FIELD);
        fieldPrefix = config.getString(FIELD_PREFIX);

    }

    private boolean compareFields(List<String> headerFields, List<String> fields) {
        return headerFields.stream().anyMatch(fields::contains);
    }

    @Override
    public R apply(R record) {

        if (record.value() == null) {
            LOGGER.trace("Tombstone {} arrived and will be skipped", record.key());
            return record;
        }

        final Struct value = requireStruct(record.value(), "Header field insertion");
        Header head = record.headers().lastWithName(headerKey);

        LOGGER.trace("Processing record {}", value);

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Header to be processed: {}", headersToString(head));
        }

        if (head == null) {
            return record;
        }

        LOGGER.trace("Header value: {}", head.value());

        Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(), valueSchema -> makeNewSchema(valueSchema, head));

        LOGGER.trace("Updated schema fields: {}", updatedSchema.fields());

        Struct updatedValue = makeUpdatedValue(value, head, updatedSchema);

        LOGGER.trace("Updated value: {}", updatedValue);

        Headers updatedHeaders = record.headers();

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp(),
                updatedHeaders);
    }

    private Struct makeUpdatedValue(Struct originalValue, Header headerToProcess, Schema updatedSchema) {

        return buildUpdatedValue(originalValue, headerToProcess, updatedSchema);
    }

    private int compareHeaderWithFields(Header headerToProcess, List<String> fields) {
        String changedColumns = (String) headerToProcess.value();
        JSONArray changedColsArr = new JSONArray(changedColumns);
        for (Object changed : changedColsArr) {
            if (fields.contains(changed.toString())) {
                return 1;
            }
        }
        return 0;
    }

    private Struct buildUpdatedValue(Struct originalValue, Header headerToProcess, Schema updatedSchema) {

        Struct updatedValue = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : originalValue.schema().fields()) {
            if (originalValue.get(field) != null) {
                updatedValue.put(field.name(), originalValue.get(field));
            }
        }

        updatedValue.put(fieldPrefix + changeFieldName, compareHeaderWithFields(headerToProcess, fields));

        return updatedValue;
    }

    private Schema makeNewSchema(Schema oldSchema, Header headerToProcess) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(oldSchema, SchemaBuilder.struct());
        for (org.apache.kafka.connect.data.Field field : oldSchema.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field(fieldPrefix + changeFieldName, Schema.INT32_SCHEMA);
        return builder.build();
    }

    private String headersToString(Header header) {
        return header.key() + "=" + header.value();
    }

    @Override
    public void close() {
    }

}