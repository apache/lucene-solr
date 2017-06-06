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
package org.apache.solr.client.solrj.response.schema;

import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;

public class SchemaRepresentation {

  private String name;

  private float version;

  private String uniqueKey;

  private Map<String, Object> similarity;

  private List<Map<String, Object>> fields;

  private List<Map<String, Object>> dynamicFields;

  private List<FieldTypeDefinition> fieldTypes;

  private List<Map<String, Object>> copyFields;


  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public float getVersion() {
    return version;
  }

  public void setVersion(float version) {
    this.version = version;
  }

  public String getUniqueKey() {
    return uniqueKey;
  }

  public void setUniqueKey(String uniqueKey) {
    this.uniqueKey = uniqueKey;
  }

  public Map<String, Object> getSimilarity() {
    return similarity;
  }

  public void setSimilarity(Map<String, Object> similarity) {
    this.similarity = similarity;
  }

  public List<Map<String, Object>> getFields() {
    return fields;
  }

  public void setFields(List<Map<String, Object>> fields) {
    this.fields = fields;
  }

  public List<Map<String, Object>> getDynamicFields() {
    return dynamicFields;
  }

  public void setDynamicFields(List<Map<String, Object>> dynamicFields) {
    this.dynamicFields = dynamicFields;
  }

  public List<FieldTypeDefinition> getFieldTypes() {
    return fieldTypes;
  }

  public void setFieldTypes(List<FieldTypeDefinition> fieldTypes) {
    this.fieldTypes = fieldTypes;
  }

  public List<Map<String, Object>> getCopyFields() {
    return copyFields;
  }

  public void setCopyFields(List<Map<String, Object>> copyFields) {
    this.copyFields = copyFields;
  }

}
