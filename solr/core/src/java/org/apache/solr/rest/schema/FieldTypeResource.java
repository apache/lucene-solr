package org.apache.solr.rest.schema;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.rest.GETable;
import org.apache.solr.rest.PUTable;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.noggit.ObjectBuilder;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class responds to requests at /solr/(corename)/schema/fieldtype/(typename)
 * where "typename" is the name of a field type in the schema.
 * 
 * The GET method returns properties for the named field type.
 */
public class FieldTypeResource extends BaseFieldTypeResource implements GETable, PUTable {
  private static final Logger log = LoggerFactory.getLogger(FieldTypeResource.class);

  private String typeName;

  public FieldTypeResource() {
    super();
  }

  @Override
  public void doInit() throws ResourceException {
    super.doInit();
    if (isExisting()) {
      typeName = (String)getRequestAttributes().get(IndexSchema.NAME);
      try {
        typeName = null == typeName ? "" : urlDecode(typeName.trim()).trim();
      } catch (UnsupportedEncodingException e) {
        throw new ResourceException(e);
      }
    }
  }

  @Override
  public Representation get() {
    try {
      if (typeName.isEmpty()) {
        final String message = "Field type name is missing";
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      } else {
        FieldType fieldType = getSchema().getFieldTypes().get(typeName);
        if (null == fieldType) {
          final String message = "Field type '" + typeName + "' not found.";
          throw new SolrException(ErrorCode.NOT_FOUND, message);
        }
        getSolrResponse().add(IndexSchema.FIELD_TYPE, getFieldTypeProperties(fieldType));
      }
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }

  /** 
   * Returns a field list using the given field type by iterating over all fields
   * defined in the schema.
   */
  @Override
  protected List<String> getFieldsWithFieldType(FieldType fieldType) {
    List<String> fields = new ArrayList<>();
    for (SchemaField schemaField : getSchema().getFields().values()) {
      if (schemaField.getType().getTypeName().equals(fieldType.getTypeName())) {
        fields.add(schemaField.getName());
      }
    }
    Collections.sort(fields);
    return fields;
  }

  /**
   * Returns a dynamic field list using the given field type by iterating over all
   * dynamic fields defined in the schema. 
   */
  @Override
  protected List<String> getDynamicFieldsWithFieldType(FieldType fieldType) {
    List<String> dynamicFields = new ArrayList<>();
    for (SchemaField prototype : getSchema().getDynamicFieldPrototypes()) {
      if (prototype.getType().getTypeName().equals(fieldType.getTypeName())) {
        dynamicFields.add(prototype.getName());
      }
    }
    return dynamicFields; // Don't sort these - they're matched in order
  }
  
  /**
   * Accepts JSON add fieldtype request, to URL
   */
  @SuppressWarnings("unchecked")
  @Override
  public Representation put(Representation entity) {
    try {
      if (!getSchema().isMutable()) {
        final String message = "This IndexSchema is not mutable.";
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      }
      
      if (null == entity.getMediaType())
        entity.setMediaType(MediaType.APPLICATION_JSON);
      
      if (!entity.getMediaType().equals(MediaType.APPLICATION_JSON, true)) {
        String message = "Only media type " + MediaType.APPLICATION_JSON.toString() + " is accepted."
            + "  Request has media type " + entity.getMediaType().toString() + ".";
        log.error(message);
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      }
      
      Object object = ObjectBuilder.fromJSON(entity.getText());
      if (!(object instanceof Map)) {
        String message = "Invalid JSON type " + object.getClass().getName() + ", expected Map of the form"
            + " (ignore the backslashes): {\"name\":\"text_general\", \"class\":\"solr.TextField\" ...},"
            + " either with or without a \"name\" mapping.  If the \"name\" is specified, it must match the"
            + " name given in the request URL: /schema/fieldtypes/(name)";
        log.error(message);
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      }
      
      // basic validation passed, let's try to create it!
      addOrUpdateFieldType((Map<String, Object>)object);
      
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }
  
  protected void addOrUpdateFieldType(Map<String,Object> fieldTypeJson) {
    ManagedIndexSchema oldSchema = (ManagedIndexSchema) getSchema();
    FieldType newFieldType = buildFieldTypeFromJson(oldSchema, typeName, fieldTypeJson);
    addNewFieldTypes(Collections.singletonList(newFieldType), oldSchema);
  }

  /**
   * Builds a FieldType definition from a JSON object.
   */
  @SuppressWarnings("unchecked")
  static FieldType buildFieldTypeFromJson(ManagedIndexSchema oldSchema, String fieldTypeName, Map<String,Object> fieldTypeJson) {
    if (1 == fieldTypeJson.size() && fieldTypeJson.containsKey(IndexSchema.FIELD_TYPE)) {
      fieldTypeJson = (Map<String, Object>)fieldTypeJson.get(IndexSchema.FIELD_TYPE);
    }

    String bodyTypeName = (String) fieldTypeJson.get(IndexSchema.NAME);
    if (bodyTypeName == null) {
      // must provide the name in the JSON for converting to the XML format needed
      // to create FieldType objects using the FieldTypePluginLoader
      fieldTypeJson.put(IndexSchema.NAME, fieldTypeName);
    } else {
      // if they provide it in the JSON, then it must match the value from the path
      if (!fieldTypeName.equals(bodyTypeName)) {
        String message = "Field type name in the request body '" + bodyTypeName
            + "' doesn't match field type name in the request URL '" + fieldTypeName + "'";
        log.error(message);
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      }
    }

    String className = (String)fieldTypeJson.get(FieldType.CLASS_NAME);
    if (className == null) {
      String message = "Missing required '" + FieldType.CLASS_NAME + "' property!";
      log.error(message);
      throw new SolrException(ErrorCode.BAD_REQUEST, message);
    }

    return oldSchema.newFieldType(fieldTypeName, className, fieldTypeJson);
  }
}
