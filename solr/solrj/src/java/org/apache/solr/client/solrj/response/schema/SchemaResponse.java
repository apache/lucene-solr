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

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.request.schema.AnalyzerDefinition;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.util.NamedList;

/**
 * This class is used to wrap the response messages retrieved from Solr Schema API.
 *
 * @see <a href="https://lucene.apache.org/solr/guide/schema-api.html">Solr Schema API</a>
 * @since solr 5.3
 */
public class SchemaResponse extends SolrResponseBase {
  private SchemaRepresentation schemaRepresentation;

  private static <T> Map<String, T> extractAttributeMap(NamedList<T> namedList) {
    if (namedList == null) return null;
    LinkedHashMap<String, T> result = new LinkedHashMap<>();
    for (int i = 0; i < namedList.size(); i++) {
      T val = namedList.getVal(i);
      String name = namedList.getName(i);
      if (!(val instanceof NamedList) && !(val instanceof List)) {
        result.put(name, val);
      }
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  private static AnalyzerDefinition createAnalyzerDefinition(NamedList<Object> analyzerNamedList) {
    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    Map<String, Object> analyzerAttributes = extractAttributeMap(analyzerNamedList);
    analyzerDefinition.setAttributes(analyzerAttributes);
    List<NamedList<Object>> charFiltersList = (List<NamedList<Object>>) analyzerNamedList.get("charFilters");
    if (charFiltersList != null) {
      List<Map<String, Object>> charFiltersAttributesList = new LinkedList<>();
      for (NamedList<Object> charFilterNamedList : charFiltersList) {
        Map<String, Object> charFilterAttributes = extractAttributeMap(charFilterNamedList);
        charFiltersAttributesList.add(charFilterAttributes);
      }
      analyzerDefinition.setCharFilters(charFiltersAttributesList);
    }
    NamedList<Object> tokenizerNamedList = (NamedList<Object>) analyzerNamedList.get("tokenizer");
    if (tokenizerNamedList != null) {
      Map<String, Object> tokenizerAttributes = extractAttributeMap(tokenizerNamedList);
      analyzerDefinition.setTokenizer(tokenizerAttributes);
    }
    List<NamedList<Object>> filtersList = (List<NamedList<Object>>) analyzerNamedList.get("filters");
    List<Map<String, Object>> filtersAttributesList = new LinkedList<>();
    if (filtersList != null) {
      for (NamedList<Object> filterNamedList : filtersList) {
        Map<String, Object> filterAttributes = extractAttributeMap(filterNamedList);
        filtersAttributesList.add(filterAttributes);
      }
      analyzerDefinition.setFilters(filtersAttributesList);
    }

    return analyzerDefinition;
  }

  @SuppressWarnings("unchecked")
  private static FieldTypeDefinition createFieldTypeDefinition(NamedList<Object> fieldTypeNamedList) {
    FieldTypeDefinition fieldTypeDefinition = new FieldTypeDefinition();
    fillFieldTypeDefinition(fieldTypeDefinition, fieldTypeNamedList);
    return fieldTypeDefinition;
  }

  @SuppressWarnings("unchecked")
  private static FieldTypeRepresentation createFieldTypeRepresentation(NamedList<Object> fieldTypeNamedList) {
    FieldTypeRepresentation fieldTypeRepresentation = new FieldTypeRepresentation();
    fillFieldTypeDefinition(fieldTypeRepresentation, fieldTypeNamedList);
    List<String> fields = (List<String>) fieldTypeNamedList.get("fields");
    if (fields != null) fieldTypeRepresentation.setFields(fields);
    List<String> dynamicFields = (List<String>) fieldTypeNamedList.get("dynamicFields");
    if (dynamicFields != null) fieldTypeRepresentation.setDynamicFields(dynamicFields);
    return fieldTypeRepresentation;
  }

  @SuppressWarnings("unchecked")
  private static void fillFieldTypeDefinition(FieldTypeDefinition fieldTypeDefinition, NamedList<Object> fieldTypeNamedList) {
    Map<String, Object> fieldTypeAttributes = extractAttributeMap(fieldTypeNamedList);
    fieldTypeDefinition.setAttributes(fieldTypeAttributes);
    NamedList<Object> analyzerNamedList = (NamedList<Object>) fieldTypeNamedList.get("analyzer");
    if (analyzerNamedList != null) {
      AnalyzerDefinition analyzerDefinition = createAnalyzerDefinition(analyzerNamedList);
      fieldTypeDefinition.setAnalyzer(analyzerDefinition);
    }
    NamedList<Object> indexAnalyzerNamedList = (NamedList<Object>) fieldTypeNamedList.get("indexAnalyzer");
    if (indexAnalyzerNamedList != null) {
      AnalyzerDefinition indexAnalyzerDefinition =
          createAnalyzerDefinition(indexAnalyzerNamedList);
      fieldTypeDefinition.setIndexAnalyzer(indexAnalyzerDefinition);
    }
    NamedList<Object> queryAnalyzerNamedList = (NamedList<Object>) fieldTypeNamedList.get("queryAnalyzer");
    if (queryAnalyzerNamedList != null) {
      AnalyzerDefinition queryAnalyzerDefinition = createAnalyzerDefinition(queryAnalyzerNamedList);
      fieldTypeDefinition.setQueryAnalyzer(queryAnalyzerDefinition);
    }
    NamedList<Object> multiTermAnalyzerNamedList = (NamedList<Object>) fieldTypeNamedList.get("multiTermAnalyzer");
    if (multiTermAnalyzerNamedList != null) {
      AnalyzerDefinition multiTermAnalyzerDefinition =
          createAnalyzerDefinition(multiTermAnalyzerNamedList);
      fieldTypeDefinition.setMultiTermAnalyzer(multiTermAnalyzerDefinition);
    }
    NamedList<Object> similarityNamedList = (NamedList<Object>) fieldTypeNamedList.get("similarity");
    if (similarityNamedList != null) {
      Map<String, Object> similarityAttributes = extractAttributeMap(similarityNamedList);
      fieldTypeDefinition.setSimilarity(similarityAttributes);
    }
  }

  private static SchemaRepresentation createSchemaConfiguration(Map schemaObj) {
    SchemaRepresentation schemaRepresentation = new SchemaRepresentation();
    schemaRepresentation.setName(getSchemaName(schemaObj));
    schemaRepresentation.setVersion(getSchemaVersion(schemaObj));
    schemaRepresentation.setUniqueKey(getSchemaUniqueKey(schemaObj));
    schemaRepresentation.setSimilarity(getSimilarity(schemaObj));
    schemaRepresentation.setFields(getFields(schemaObj));
    schemaRepresentation.setDynamicFields(getDynamicFields(schemaObj));
    schemaRepresentation.setFieldTypes(getFieldTypeDefinitions(schemaObj));
    schemaRepresentation.setCopyFields(getCopyFields(schemaObj));
    return schemaRepresentation;
  }

  private static String getSchemaName(Map schemaNamedList) {
    return (String) schemaNamedList.get("name");
  }

  private static Float getSchemaVersion(Map schemaNamedList) {
    return (Float) schemaNamedList.get("version");
  }

  private static String getSchemaUniqueKey(Map schemaNamedList) {
    return (String) schemaNamedList.get("uniqueKey");
  }

  private static Map<String, Object> getSimilarity(Map schemaNamedList) {
    NamedList<Object> similarityNamedList = (NamedList<Object>) schemaNamedList.get("similarity");
    Map<String, Object> similarity = null;
    if (similarityNamedList != null) similarity = extractAttributeMap(similarityNamedList);
    return similarity;
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> getFields(Map schemaNamedList) {
    List<Map<String, Object>> fieldsAttributes = new LinkedList<>();
    List<NamedList<Object>> fieldsResponse = (List<NamedList<Object>>) schemaNamedList.get("fields");
    for (NamedList<Object> fieldNamedList : fieldsResponse) {
      Map<String, Object> fieldAttributes = new LinkedHashMap<>(extractAttributeMap(fieldNamedList));
      fieldsAttributes.add(fieldAttributes);
    }

    return fieldsAttributes;
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> getDynamicFields(Map schemaNamedList) {
    List<Map<String, Object>> dynamicFieldsAttributes = new LinkedList<>();
    List<NamedList<Object>> dynamicFieldsResponse = (List<NamedList<Object>>) schemaNamedList.get("dynamicFields");
    for (NamedList<Object> fieldNamedList : dynamicFieldsResponse) {
      Map<String, Object> dynamicFieldAttributes = new LinkedHashMap<>(extractAttributeMap(fieldNamedList));
      dynamicFieldsAttributes.add(dynamicFieldAttributes);
    }

    return dynamicFieldsAttributes;
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> getCopyFields(Map schemaNamedList) {
    List<Map<String, Object>> copyFieldsAttributes = new LinkedList<>();
    List<NamedList<Object>> copyFieldsResponse = (List<NamedList<Object>>) schemaNamedList.get("copyFields");
    for (NamedList<Object> copyFieldNamedList : copyFieldsResponse) {
      Map<String, Object> copyFieldAttributes = new LinkedHashMap<>(extractAttributeMap(copyFieldNamedList));
      copyFieldsAttributes.add(copyFieldAttributes);
    }

    return copyFieldsAttributes;
  }

  @SuppressWarnings("unchecked")
  private static List<FieldTypeDefinition> getFieldTypeDefinitions(Map schemaNamedList) {
    List<FieldTypeDefinition> fieldTypeDefinitions = new LinkedList<>();
    List<NamedList<Object>> fieldsResponse = (List<NamedList<Object>>) schemaNamedList.get("fieldTypes");
    for (NamedList<Object> fieldNamedList : fieldsResponse) {
      FieldTypeDefinition fieldTypeDefinition = createFieldTypeDefinition(fieldNamedList);
      fieldTypeDefinitions.add(fieldTypeDefinition);
    }

    return fieldTypeDefinitions;
  }

  @SuppressWarnings("unchecked")
  private static List<FieldTypeRepresentation> getFieldTypeRepresentations(Map schemaNamedList) {
    List<FieldTypeRepresentation> fieldTypeRepresentations = new LinkedList<>();
    List<NamedList<Object>> fieldsResponse = (List<NamedList<Object>>) schemaNamedList.get("fieldTypes");
    for (NamedList<Object> fieldNamedList : fieldsResponse) {
      FieldTypeRepresentation fieldTypeRepresentation = createFieldTypeRepresentation(fieldNamedList);
      fieldTypeRepresentations.add(fieldTypeRepresentation);
    }

    return fieldTypeRepresentations;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setResponse(NamedList<Object> response) {
    super.setResponse(response);

    Map schemaObj = (Map) response.get("schema");
    schemaRepresentation = createSchemaConfiguration(schemaObj);
  }

  public SchemaRepresentation getSchemaRepresentation() {
    return schemaRepresentation;
  }

  public static class SchemaNameResponse extends SolrResponseBase {
    private String schemaName;

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      schemaName = SchemaResponse.getSchemaName(response.asShallowMap());
    }

    public String getSchemaName() {
      return schemaName;
    }

  }

  public static class SchemaVersionResponse extends SolrResponseBase {
    private float schemaVersion;

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      schemaVersion = SchemaResponse.getSchemaVersion(response.asShallowMap());
    }

    public float getSchemaVersion() {
      return schemaVersion;
    }

  }

  public static class FieldResponse extends SolrResponseBase {
    Map<String, Object> field = new LinkedHashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      NamedList<Object> fieldResponse = (NamedList<Object>) response.get("field");
      field.putAll(extractAttributeMap(fieldResponse));
    }

    public Map<String, Object> getField() {
      return field;
    }

  }

  public static class FieldsResponse extends SolrResponseBase {
    List<Map<String, Object>> fields;

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      fields = SchemaResponse.getFields(response.asShallowMap());
    }

    public List<Map<String, Object>> getFields() {
      return fields;
    }
  }

  public static class DynamicFieldResponse extends SolrResponseBase {
    Map<String, Object> dynamicField = new LinkedHashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      NamedList<Object> dynamicFieldResponse = (NamedList<Object>) response.get("dynamicField");
      dynamicField.putAll(extractAttributeMap(dynamicFieldResponse));
    }

    public Map<String, Object> getDynamicField() {
      return dynamicField;
    }

  }

  public static class DynamicFieldsResponse extends SolrResponseBase {
    List<Map<String, Object>> dynamicFields;

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      dynamicFields = SchemaResponse.getDynamicFields(response.asMap(3));
    }

    public List<Map<String, Object>> getDynamicFields() {
      return dynamicFields;
    }
  }

  public static class UniqueKeyResponse extends SolrResponseBase {
    private String uniqueKey;

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      uniqueKey = SchemaResponse.getSchemaUniqueKey(response.asShallowMap());
    }

    public String getUniqueKey() {
      return uniqueKey;
    }
  }

  public static class GlobalSimilarityResponse extends SolrResponseBase {
    Map<String, Object> similarity;

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      similarity = SchemaResponse.getSimilarity(response.asShallowMap());
    }

    public Map<String, Object> getSimilarity() {
      return similarity;
    }

  }

  public static class CopyFieldsResponse extends SolrResponseBase {
    List<Map<String, Object>> copyFields;

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      copyFields = SchemaResponse.getCopyFields(response.asShallowMap());
    }

    public List<Map<String, Object>> getCopyFields() {
      return copyFields;
    }
  }

  public static class FieldTypeResponse extends SolrResponseBase {
    private FieldTypeRepresentation fieldType;

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      NamedList<Object> fieldTypeNamedList = (NamedList<Object>) response.get("fieldType");
      fieldType = createFieldTypeRepresentation(fieldTypeNamedList);
    }

    public FieldTypeRepresentation getFieldType() {
      return fieldType;
    }
  }


  public static class FieldTypesResponse extends SolrResponseBase {
    List<FieldTypeRepresentation> fieldTypes;

    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);

      fieldTypes = SchemaResponse.getFieldTypeRepresentations(response.asShallowMap());
    }

    public List<FieldTypeRepresentation> getFieldTypes() {
      return fieldTypes;
    }
  }

  public static class UpdateResponse extends SolrResponseBase {
    @Override
    @SuppressWarnings("unchecked")
    public void setResponse(NamedList<Object> response) {
      super.setResponse(response);
    }
  }
}
