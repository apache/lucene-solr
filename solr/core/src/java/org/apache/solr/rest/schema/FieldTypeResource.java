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
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class responds to requests at /solr/(corename)/schema/fieldtype/(typename)
 * where "typename" is the name of a field type in the schema.
 * 
 * The GET method returns properties for the named field type.
 */
public class FieldTypeResource extends BaseFieldTypeResource implements GETable {
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
    List<String> fields = new ArrayList<String>();
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
    List<String> dynamicFields = new ArrayList<String>();
    for (SchemaField prototype : getSchema().getDynamicFieldPrototypes()) {
      if (prototype.getType().getTypeName().equals(fieldType.getTypeName())) {
        dynamicFields.add(prototype.getName());
      }
    }
    return dynamicFields; // Don't sort these - they're matched in order
  }
}
