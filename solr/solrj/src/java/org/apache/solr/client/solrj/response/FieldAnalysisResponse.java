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
import java.util.List;
import java.util.Map;

/**
 * A response that is returned by processing the {@link org.apache.solr.client.solrj.request.FieldAnalysisRequest}.
 * Holds a map of {@link Analysis} objects per field name as well as a map of {@link Analysis} objects per field type.
 *
 *
 * @since solr 1.4
 */
public class FieldAnalysisResponse extends AnalysisResponseBase {

  private Map<String, Analysis> analysisByFieldTypeName = new HashMap<>();
  private Map<String, Analysis> analysisByFieldName = new HashMap<>();

  @Override
  public void setResponse(NamedList<Object> response) {
    super.setResponse(response);

    @SuppressWarnings("unchecked")
    NamedList<NamedList<NamedList<NamedList<Object>>>> analysisNL
      = (NamedList<NamedList<NamedList<NamedList<Object>>>>) response.get("analysis");

    for (Map.Entry<String, NamedList<NamedList<Object>>> entry
           : analysisNL.get("field_types")) {

      analysisByFieldTypeName.put(entry.getKey(), buildAnalysis(entry.getValue()));
    }

    for (Map.Entry<String, NamedList<NamedList<Object>>> entry
           : analysisNL.get("field_names")) {

      analysisByFieldName.put(entry.getKey(), buildAnalysis(entry.getValue()));
    }
  }

  private Analysis buildAnalysis(NamedList<NamedList<Object>> value) {
      Analysis analysis = new Analysis();
      
      NamedList<Object> queryNL = value.get("query");
      List<AnalysisPhase> phases = (queryNL == null) ? null : buildPhases(queryNL);
      analysis.setQueryPhases(phases);

      NamedList<Object> indexNL = value.get("index");
      phases = buildPhases(indexNL);
      analysis.setIndexPhases(phases);
      
      return analysis;
  }

  /**
   * Returns the number of field type analyses.
   *
   * @return The number of field type analyses.
   */
  public int getFieldTypeAnalysisCount() {
    return analysisByFieldTypeName.size();
  }

  /**
   * Returns the analysis for the given field type or {@code null} if no such analysis exists.
   *
   * @param fieldTypeName The name of the field type.
   *
   * @return The analysis for the given field type.
   */
  public Analysis getFieldTypeAnalysis(String fieldTypeName) {
    return analysisByFieldTypeName.get(fieldTypeName);
  }

  /**
   * Returns all field type analyses with their associated field types.
   *
   * @return All field type analyses with their associated field types.
   */
  public Iterable<Map.Entry<String, Analysis>> getAllFieldTypeAnalysis() {
    return analysisByFieldTypeName.entrySet();
  }

  /**
   * Returns the number of field name analyses.
   *
   * @return The number of field name analyses.
   */
  public int getFieldNameAnalysisCount() {
    return analysisByFieldName.size();
  }

  /**
   * Returns the analysis for the given field name or {@code null} if no such analysis exists.
   *
   * @param fieldName The field name.
   *
   * @return The analysis for the given field name.
   */
  public Analysis getFieldNameAnalysis(String fieldName) {
    return analysisByFieldName.get(fieldName);
  }

  /**
   * Returns all field name analysese with their associated field names.
   *
   * @return all field name analysese with their associated field names.
   */
  public Iterable<Map.Entry<String, Analysis>> getAllFieldNameAnalysis() {
    return analysisByFieldName.entrySet();
  }


  //================================================= Inner Classes ==================================================

  /**
   * The analysis of a field. Holds a list of all the query time analysis phases (if a query analysis was requested)
   * as well as index time phases.
   */
  public static class Analysis {

    private List<AnalysisPhase> queryPhases;
    private List<AnalysisPhase> indexPhases;

    /**
     * This class should only be instantiated internally.
     */
    private Analysis() {
    }

    /**
     * Returns the number of query time analysis phases in this analysis or 
     * {@code -1} if query time analysis doesn't exist.
     *
     * @return Returns the number of query time analysis phases in this 
     *         analysis or {@code -1} if query time analysis doesn't exist.
     */
    public int getQueryPhasesCount() {
      return queryPhases == null ? -1 : queryPhases.size();
    }

    /**
     * Returns the query time analysis phases for this analysis or {@code null}
     * if query time analysis doesn't exist.
     * 
     *
     * @return The query time analysis phases for this analysis or {@code null}
     *         if query time analysis doesn't exist.
     *         
     */
    public Iterable<AnalysisPhase> getQueryPhases() {
      return queryPhases;
    }

    /**
     * Returns the index time analysis phases for this analysis.
     *
     * @return The index time analysis phases for this analysis.
     */
    public int getIndexPhasesCount() {
      return indexPhases.size();
    }

    /**
     * Returns the index time analysis phases for this analysis.
     *
     * @return The index time analysis phases for this analysis.
     */
    public Iterable<AnalysisPhase> getIndexPhases() {
      return indexPhases;
    }

    private void setQueryPhases(List<AnalysisPhase> queryPhases) {
      this.queryPhases = queryPhases;
    }

    private void setIndexPhases(List<AnalysisPhase> indexPhases) {
      this.indexPhases = indexPhases;
    }

  }

}
