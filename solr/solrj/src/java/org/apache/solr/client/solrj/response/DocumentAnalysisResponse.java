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
package org.apache.solr.client.solrj.response;

import org.apache.solr.common.util.NamedList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A response that is returned by processing the {@link org.apache.solr.client.solrj.request.DocumentAnalysisRequest}.
 * Holds a map of {@link DocumentAnalysis} objects by a document id (unique key).
 *
 *
 * @since solr 1.4
 */
public class DocumentAnalysisResponse extends AnalysisResponseBase implements Iterable<Map.Entry<String, DocumentAnalysisResponse.DocumentAnalysis>> {

  private final Map<String, DocumentAnalysis> documentAnalysisByKey = new HashMap<>();

  @Override
  public void setResponse(NamedList<Object> response) {
    super.setResponse(response);

    @SuppressWarnings("unchecked")
    NamedList<NamedList<NamedList<Object>>> analysis 
      = (NamedList<NamedList<NamedList<Object>>>) response.get("analysis");
    for (Map.Entry<String, NamedList<NamedList<Object>>> document : analysis) {
      DocumentAnalysis documentAnalysis = new DocumentAnalysis(document.getKey());
      for (Map.Entry<String, NamedList<Object>> fieldEntry : document.getValue()) {
        FieldAnalysis fieldAnalysis = new FieldAnalysis(fieldEntry.getKey());

        NamedList<Object> field = fieldEntry.getValue();

        @SuppressWarnings("unchecked")
        NamedList<Object> query
          = (NamedList<Object>) field.get("query");
        if (query != null) {
          List<AnalysisPhase> phases = buildPhases(query);
          fieldAnalysis.setQueryPhases(phases);
        }
        
        @SuppressWarnings("unchecked")
        NamedList<NamedList<Object>> index
          = (NamedList<NamedList<Object>>) field.get("index");
        for (Map.Entry<String, NamedList<Object>> valueEntry : index) {
          String fieldValue = valueEntry.getKey();
          NamedList<Object> valueNL = valueEntry.getValue();
          List<AnalysisPhase> phases = buildPhases(valueNL);
          fieldAnalysis.setIndexPhases(fieldValue, phases);
        }

        documentAnalysis.addFieldAnalysis(fieldAnalysis);
      }

      documentAnalysisByKey.put(documentAnalysis.getDocumentKey(), documentAnalysis);
    }
  }

  /**
   * Returns the number of document analyses in this response.
   *
   * @return The number of document analyses in this response.
   */
  public int getDocumentAnalysesCount() {
    return documentAnalysisByKey.size();
  }

  /**
   * Returns the document analysis for the document associated with the given unique key (id), {@code null} if no such
   * association exists.
   *
   * @param documentKey The document unique key.
   *
   * @return The document analysis for the document associated with the given unique key (id).
   */
  public DocumentAnalysis getDocumentAnalysis(String documentKey) {
    return documentAnalysisByKey.get(documentKey);
  }

  /**
   * Returns an iterator over the document analyses map.
   *
   * @return An iterator over the document analyses map.
   */
  @Override
  public Iterator<Map.Entry<String, DocumentAnalysis>> iterator() {
    return documentAnalysisByKey.entrySet().iterator();
  }

  //================================================= Inner Classes ==================================================

  /**
   * An analysis process breakdown of a document. Holds a map of field analyses by the field name.
   */
  public static class DocumentAnalysis implements Iterable<Map.Entry<String, FieldAnalysis>> {

    private final String documentKey;
    private Map<String, FieldAnalysis> fieldAnalysisByFieldName = new HashMap<>();

    private DocumentAnalysis(String documentKey) {
      this.documentKey = documentKey;
    }

    private void addFieldAnalysis(FieldAnalysis fieldAnalysis) {
      fieldAnalysisByFieldName.put(fieldAnalysis.getFieldName(), fieldAnalysis);
    }

    /**
     * Returns the unique key of the analyzed document.
     *
     * @return The unique key of the analyzed document.
     */
    public String getDocumentKey() {
      return documentKey;
    }

    /**
     * Returns the number of field analyses for the documents.
     *
     * @return The number of field analyses for the documents.
     */
    public int getFieldAnalysesCount() {
      return fieldAnalysisByFieldName.size();
    }

    public FieldAnalysis getFieldAnalysis(String fieldName) {
      return fieldAnalysisByFieldName.get(fieldName);
    }

    /**
     * Returns an iterator over the field analyses map.
     *
     * @return An iterator over the field analyses map.
     */
    @Override
    public Iterator<Map.Entry<String, FieldAnalysis>> iterator() {
      return fieldAnalysisByFieldName.entrySet().iterator();
    }
  }

  /**
   * An analysis process breakdown for a specific field. Holds a list of query time analysis phases (that is, if a
   * query analysis was requested in the first place) and a list of index time analysis phases for each field value (a
   * field can be multi-valued).
   */
  public static class FieldAnalysis {

    private final String fieldName;
    private List<AnalysisPhase> queryPhases;
    private Map<String, List<AnalysisPhase>> indexPhasesByFieldValue = new HashMap<>();

    private FieldAnalysis(String fieldName) {
      this.fieldName = fieldName;
    }

    public void setQueryPhases(List<AnalysisPhase> queryPhases) {
      this.queryPhases = queryPhases;
    }

    public void setIndexPhases(String fieldValue, List<AnalysisPhase> indexPhases) {
      indexPhasesByFieldValue.put(fieldValue, indexPhases);
    }

    /**
     * Returns the field name.
     *
     * @return The name of the field.
     */
    public String getFieldName() {
      return fieldName;
    }

    /**
     * Returns the number of query time analysis phases or {@code -1} if 
     * this field analysis doesn't hold a query time analysis.
     *
     * @return Returns the number of query time analysis phases or {@code -1} 
     *         if this field analysis doesn't hold a query time analysis.
     */
    public int getQueryPhasesCount() {
      return queryPhases == null ? -1 : queryPhases.size();
    }

    /**
     * Returns the query time analysis phases for the field or {@code null} 
     * if this field doesn't hold a query time analysis.
     *
     * @return Returns the query time analysis phases for the field or 
     *         {@code null} if this field doesn't hold a query time analysis.
     */
    public Iterable<AnalysisPhase> getQueryPhases() {
      return queryPhases;
    }

    /**
     * Returns the number of values the field has.
     *
     * @return The number of values the field has.
     */
    public int getValueCount() {
      return indexPhasesByFieldValue.entrySet().size();
    }

    /**
     * Returns the number of index time analysis phases the given field value has.
     *
     * @param fieldValue The field value.
     *
     * @return The number of index time analysis phases the given field value has.
     */
    public int getIndexPhasesCount(String fieldValue) {
      return indexPhasesByFieldValue.get(fieldValue).size();
    }

    /**
     * Returns the index time analysis phases for the given field value.
     *
     * @param fieldValue The field value.
     *
     * @return The index time analysis phases for the given field value.
     */
    public Iterable<AnalysisPhase> getIndexPhases(String fieldValue) {
      return indexPhasesByFieldValue.get(fieldValue);
    }

    /**
     * Returns the index time analysis phases for all field values.
     *
     * @return Returns the index time analysis phases for all field value.
     */
    public Iterable<Map.Entry<String, List<AnalysisPhase>>> getIndexPhasesByFieldValue() {
      return indexPhasesByFieldValue.entrySet();
    }

  }

}
