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
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.rest.GETable;
import org.apache.solr.rest.POSTable;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class responds to requests at /solr/(corename)/schema/fieldtypes
 * 
 * The GET method returns properties for all field types defined in the schema.
 */
public class FieldTypeCollectionResource extends BaseFieldTypeResource implements GETable, POSTable {
  private static final Logger log = LoggerFactory.getLogger(FieldTypeCollectionResource.class);
  
  private Map<String,List<String>> fieldsByFieldType;
  private Map<String,List<String>> dynamicFieldsByFieldType;

  public FieldTypeCollectionResource() {
    super();
  }

  @Override
  public void doInit() throws ResourceException {
    super.doInit();
    if (isExisting()) {
      fieldsByFieldType = getFieldsByFieldType();
      dynamicFieldsByFieldType = getDynamicFieldsByFieldType();
    }
  }
  
  @Override
  public Representation get() {
    try {
      List<SimpleOrderedMap<Object>> props = new ArrayList<>();
      Map<String,FieldType> sortedFieldTypes = new TreeMap<>(getSchema().getFieldTypes());
      for (FieldType fieldType : sortedFieldTypes.values()) {
        props.add(getFieldTypeProperties(fieldType));
      }
      getSolrResponse().add(IndexSchema.FIELD_TYPES, props);
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }

  /** Returns field lists from the map constructed in doInit() */
  @Override
  protected List<String> getFieldsWithFieldType(FieldType fieldType) {
    List<String> fields = fieldsByFieldType.get(fieldType.getTypeName());
    if (null == fields) {
      fields = Collections.emptyList();
    }
    return fields;
  }

  /** Returns dynamic field lists from the map constructed in doInit() */
  @Override
  protected List<String> getDynamicFieldsWithFieldType(FieldType fieldType) {
    List<String> dynamicFields = dynamicFieldsByFieldType.get(fieldType.getTypeName());
    if (null == dynamicFields) {
      dynamicFields = Collections.emptyList();
    }
    return dynamicFields;
  }

  /**
   * Returns a map from field type names to a sorted list of fields that use the field type.
   * The map only includes field types that are used by at least one field.  
   */
  private Map<String,List<String>> getFieldsByFieldType() {
    Map<String,List<String>> fieldsByFieldType = new HashMap<>();
    for (SchemaField schemaField : getSchema().getFields().values()) {
      final String fieldType = schemaField.getType().getTypeName();
      List<String> fields = fieldsByFieldType.get(fieldType);
      if (null == fields) {
        fields = new ArrayList<>();
        fieldsByFieldType.put(fieldType, fields);
      }
      fields.add(schemaField.getName());
    }
    for (List<String> fields : fieldsByFieldType.values()) {
      Collections.sort(fields);
    }
    return fieldsByFieldType;
  }

  /**
   * Returns a map from field type names to a list of dynamic fields that use the field type.
   * The map only includes field types that are used by at least one dynamic field.  
   */
  private Map<String,List<String>> getDynamicFieldsByFieldType() {
    Map<String,List<String>> dynamicFieldsByFieldType = new HashMap<>();
    for (SchemaField schemaField : getSchema().getDynamicFieldPrototypes()) {
      final String fieldType = schemaField.getType().getTypeName();
      List<String> dynamicFields = dynamicFieldsByFieldType.get(fieldType);
      if (null == dynamicFields) {
        dynamicFields = new ArrayList<>();
        dynamicFieldsByFieldType.put(fieldType, dynamicFields);
      }
      dynamicFields.add(schemaField.getName());
    }
    return dynamicFieldsByFieldType;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Representation post(Representation entity) {
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
      if (!(object instanceof List)) {
        String message = "Invalid JSON type " + object.getClass().getName() 
            + ", expected List of field type definitions in the form of"
            + " (ignore the backslashes): [{\"name\":\"text_general\",\"class\":\"solr.TextField\", ...}, {...}, ...]";
        log.error(message);
        throw new SolrException(ErrorCode.BAD_REQUEST, message);
      }
      
      List<Map<String, Object>> fieldTypeList = (List<Map<String, Object>>) object;
      if (fieldTypeList.size() > 0)
        addOrUpdateFieldTypes(fieldTypeList);
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }  
  
  @SuppressWarnings("unchecked")
  protected void addOrUpdateFieldTypes(List<Map<String, Object>> fieldTypeList) throws Exception {
    List<FieldType> newFieldTypes = new ArrayList<>(fieldTypeList.size());
    ManagedIndexSchema oldSchema = (ManagedIndexSchema) getSchema();
    for (Map<String,Object> fieldTypeJson : fieldTypeList) {
      if (1 == fieldTypeJson.size() && fieldTypeJson.containsKey(IndexSchema.FIELD_TYPE)) {
        fieldTypeJson = (Map<String, Object>) fieldTypeJson.get(IndexSchema.FIELD_TYPE);
      }
      FieldType newFieldType = 
          FieldTypeResource.buildFieldTypeFromJson(oldSchema, 
              (String)fieldTypeJson.get(IndexSchema.NAME), fieldTypeJson);
      newFieldTypes.add(newFieldType);
    }
    // now deploy the added types (all or nothing)
    addNewFieldTypes(newFieldTypes, oldSchema);
  }
}
