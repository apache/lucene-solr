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
package org.apache.solr.client.solrj.request;

import java.util.LinkedList;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.FieldAnalysisResponse;
import org.apache.solr.common.params.AnalysisParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

/**
 * A request for the org.apache.solr.handler.FieldAnalysisRequestHandler.
 *
 *
 * @since solr.14
 */
public class FieldAnalysisRequest extends SolrRequest<FieldAnalysisResponse> {

  private String fieldValue;
  private String query;
  private boolean showMatch;
  private List<String> fieldNames;
  private List<String> fieldTypes;

  /**
   * Constructs a new FieldAnalysisRequest with a default uri of "/fieldanalysis".
   */
  public FieldAnalysisRequest() {
    super(METHOD.GET, "/analysis/field");
  }

  /**
   * Constructs a new FieldAnalysisRequest with a given uri.
   *
   * @param uri the uri of the request handler.
   */
  public FieldAnalysisRequest(String uri) {
    super(METHOD.GET, uri);
  }


  @Override
  protected FieldAnalysisResponse createResponse(SolrClient client) {
    if (fieldTypes == null && fieldNames == null) {
      throw new IllegalStateException("At least one field type or field name need to be specified");
    }
    if (fieldValue == null) {
      throw new IllegalStateException("The field value must be set");
    }
    return new FieldAnalysisResponse();
  }

  @Override
  public SolrParams getParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AnalysisParams.FIELD_VALUE, fieldValue);
    if (query != null) {
      params.add(AnalysisParams.QUERY, query);
      params.add(AnalysisParams.SHOW_MATCH, String.valueOf(showMatch));
    }
    if (fieldNames != null) {
      String fieldNameValue = listToCommaDelimitedString(fieldNames);
      params.add(AnalysisParams.FIELD_NAME, fieldNameValue);
    }
    if (fieldTypes != null) {
      String fieldTypeValue = listToCommaDelimitedString(fieldTypes);
      params.add(AnalysisParams.FIELD_TYPE, fieldTypeValue);
    }
    return params;
  }

  //================================================ Helper Methods ==================================================

  /**
   * Convers the given list of string to a comma-separated string.
   *
   * @param list The list of string.
   *
   * @return The comma-separated string.
   */
  static String listToCommaDelimitedString(List<String> list) {
    StringBuilder result = new StringBuilder();
    for (String str : list) {
      if (result.length() > 0) {
        result.append(",");
      }
      result.append(str);
    }
    return result.toString();
  }


  //============================================ Setter/Getter Methods ===============================================

  /**
   * Sets the field value to be analyzed.
   *
   * @param fieldValue The field value to be analyzed.
   *
   * @return This FieldAnalysisRequest (fluent interface support).
   */
  public FieldAnalysisRequest setFieldValue(String fieldValue) {
    this.fieldValue = fieldValue;
    return this;
  }

  /**
   * Returns the field value that will be analyzed when this request is processed.
   *
   * @return The field value that will be analyzed when this request is processed.
   */
  public String getFieldValue() {
    return fieldValue;
  }

  /**
   * Sets the query to be analyzed. May be {@code null} indicated that no query analysis should take place.
   *
   * @param query The query to be analyzed.
   *
   * @return This FieldAnalysisRequest (fluent interface support).
   */
  public FieldAnalysisRequest setQuery(String query) {
    this.query = query;
    return this;
  }

  /**
   * Returns the query that will be analyzed. May return {@code null} indicating that no query analysis will be
   * performed.
   *
   * @return The query that will be analyzed. May return {@code null} indicating that no query analysis will be
   *         performed.
   */
  public String getQuery() {
    return query;
  }

  /**
   * Sets whether index time tokens that match query time tokens should be marked as a "match". By default this is set
   * to {@code false}. Obviously, this flag is ignored if when the query is set to {@code null}.
   *
   * @param showMatch Sets whether index time tokens that match query time tokens should be marked as a "match".
   *
   * @return This FieldAnalysisRequest (fluent interface support).
   */
  public FieldAnalysisRequest setShowMatch(boolean showMatch) {
    this.showMatch = showMatch;
    return this;
  }

  /**
   * Returns whether index time tokens that match query time tokens should be marked as a "match".
   *
   * @return Whether index time tokens that match query time tokens should be marked as a "match".
   *
   * @see #setShowMatch(boolean)
   */
  public boolean isShowMatch() {
    return showMatch;
  }

  /**
   * Adds the given field name for analysis.
   *
   * @param fieldName A field name on which the analysis should be performed.
   *
   * @return this FieldAnalysisRequest (fluent interface support).
   */
  public FieldAnalysisRequest addFieldName(String fieldName) {
    if (fieldNames == null) {
      fieldNames = new LinkedList<>();
    }
    fieldNames.add(fieldName);
    return this;
  }

  /**
     * Sets the field names on which the analysis should be performed.
     *
     * @param fieldNames The field names on which the analysis should be performed.
     *
     * @return this FieldAnalysisRequest (fluent interface support).
     */
  public FieldAnalysisRequest setFieldNames(List<String> fieldNames) {
    this.fieldNames = fieldNames;
    return this;
  }

  /**
   * Returns a list of field names the analysis should be performed on. May return {@code null} indicating that no
   * analysis will be performed on field names.
   *
   * @return The field names the analysis should be performed on.
   */
  public List<String> getFieldNames() {
    return fieldNames;
  }

  /**
   * Adds the given field type for analysis.
   *
   * @param fieldTypeName A field type name on which analysis should be performed.
   *
   * @return This FieldAnalysisRequest (fluent interface support).
   */
  public FieldAnalysisRequest addFieldType(String fieldTypeName) {
    if (fieldTypes == null) {
      fieldTypes = new LinkedList<>();
    }
    fieldTypes.add(fieldTypeName);
    return this;
  }

/**
   * Sets the field types on which analysis should be performed.
   *
   * @param fieldTypes The field type names on which analysis should be performed.
   *
   * @return This FieldAnalysisRequest (fluent interface support).
   */
  public FieldAnalysisRequest setFieldTypes(List<String> fieldTypes) {
    this.fieldTypes = fieldTypes;
    return this;
  }


  /**
   * Returns a list of field types the analysis should be performed on. May return {@code null} indicating that no
   * analysis will be peformed on field types.
   *
   * @return The field types the analysis should be performed on.
   */
  public List<String> getFieldTypes() {
    return fieldTypes;
  }

}
