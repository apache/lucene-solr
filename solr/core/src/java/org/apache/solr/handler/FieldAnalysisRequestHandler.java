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
package org.apache.solr.handler;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.request.FieldAnalysisRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.AnalysisParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.commons.io.IOUtils;

import java.io.Reader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 * Provides the ability to specify multiple field types and field names in the same request. Expected parameters:
 * <table border="1" summary="table of parameters">
 * <tr>
 * <th align="left">Name</th>
 * <th align="left">Type</th>
 * <th align="left">required</th>
 * <th align="left">Description</th>
 * <th align="left">Multi-valued</th>
 * </tr>
 * <tr>
 * <td>analysis.fieldname</td>
 * <td>string</td>
 * <td>no</td>
 * <td>When present, the text will be analyzed based on the type of this field name.</td>
 * <td>Yes, this parameter may hold a comma-separated list of values and the analysis will be performed for each of the specified fields</td>
 * </tr>
 * <tr>
 * <td>analysis.fieldtype</td>
 * <td>string</td>
 * <td>no</td>
 * <td>When present, the text will be analyzed based on the specified type</td>
 * <td>Yes, this parameter may hold a comma-separated list of values and the analysis will be performed for each of the specified field types</td>
 * </tr>
 * <tr>
 * <td>analysis.fieldvalue</td>
 * <td>string</td>
 * <td>no</td>
 * <td>The text that will be analyzed. The analysis will mimic the index-time analysis.</td>
 * <td>No</td>
 * </tr>
 * <tr>
 * <td>{@code analysis.query} OR {@code q}</td>
 * <td>string</td>
 * <td>no</td>
 * <td>When present, the text that will be analyzed. The analysis will mimic the query-time analysis. Note that the
 * {@code analysis.query} parameter as precedes the {@code q} parameters.</td>
 * <td>No</td>
 * </tr>
 * <tr>
 * <td>analysis.showmatch</td>
 * <td>boolean</td>
 * <td>no</td>
 * <td>When set to {@code true} and when query analysis is performed, the produced tokens of the field value
 * analysis will be marked as "matched" for every token that is produces by the query analysis</td>
 * <td>No</td>
 * </tr>
 * </table>
 * <p>Note that if neither analysis.fieldname and analysis.fieldtype is specified, then the default search field's
 * analyzer is used.</p>
 * <p>Note that if one of analysis.value or analysis.query or q must be specified</p>
 *
 * @since solr 1.4 
 */
public class FieldAnalysisRequestHandler extends AnalysisRequestHandlerBase {

  @Override
  @SuppressWarnings({"rawtypes"})
  protected NamedList doAnalysis(SolrQueryRequest req) throws Exception {
    FieldAnalysisRequest analysisRequest = resolveAnalysisRequest(req);
    IndexSchema indexSchema = req.getSchema();
    return handleAnalysisRequest(analysisRequest, indexSchema);
  }

  @Override
  public String getDescription() {
    return "Provide a breakdown of the analysis process of field/query text";
  }

  // ================================================= Helper methods ================================================

  /**
   * Resolves the AnalysisRequest based on the parameters in the given SolrParams.
   *
   * @param req the request
   *
   * @return AnalysisRequest containing all the information about what needs to be analyzed, and using what
   *         fields/types
   */
  FieldAnalysisRequest resolveAnalysisRequest(SolrQueryRequest req) throws SolrException {
    SolrParams solrParams = req.getParams();
    FieldAnalysisRequest analysisRequest = new FieldAnalysisRequest();

    boolean useDefaultSearchField = true;
    if (solrParams.get(AnalysisParams.FIELD_TYPE) != null) {
      analysisRequest.setFieldTypes(Arrays.asList(solrParams.get(AnalysisParams.FIELD_TYPE).split(",")));
      useDefaultSearchField = false;
    }
    if (solrParams.get(AnalysisParams.FIELD_NAME) != null) {
      analysisRequest.setFieldNames(Arrays.asList(solrParams.get(AnalysisParams.FIELD_NAME).split(",")));
      useDefaultSearchField = false;
    }
    if (useDefaultSearchField) {
      if (solrParams.get(CommonParams.DF) != null) {
        analysisRequest.addFieldName(solrParams.get(CommonParams.DF));
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Field analysis request must contain one of analysis.fieldtype, analysis.fieldname or df.");
      }
    }
    analysisRequest.setQuery(solrParams.get(AnalysisParams.QUERY, solrParams.get(CommonParams.Q)));

    String value = solrParams.get(AnalysisParams.FIELD_VALUE);
    if (analysisRequest.getQuery() == null && value == null)  {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "One of analysis.fieldvalue, q, or analysis.query parameters must be specified");
    }

    Iterable<ContentStream> streams = req.getContentStreams();
    if (streams != null) {
      // NOTE: Only the first content stream is currently processed
      for (ContentStream stream : streams) {
        Reader reader = null;
        try {
          reader = stream.getReader();
          value = IOUtils.toString(reader);
        } catch (IOException e) {
          // do nothing, leave value set to the request parameter
        }
        finally {
          IOUtils.closeQuietly(reader);
        }
        break;
      }
    }

    analysisRequest.setFieldValue(value);
    analysisRequest.setShowMatch(solrParams.getBool(AnalysisParams.SHOW_MATCH, false));
    return analysisRequest;
  }

  /**
   * Handles the resolved analysis request and returns the analysis breakdown response as a named list.
   *
   * @param request The request to handle.
   * @param schema  The index schema.
   *
   * @return The analysis breakdown as a named list.
   */
  @SuppressWarnings({"rawtypes"})
  protected NamedList<NamedList> handleAnalysisRequest(FieldAnalysisRequest request, IndexSchema schema) {
    NamedList<NamedList> analysisResults = new SimpleOrderedMap<>();

    NamedList<NamedList> fieldTypeAnalysisResults = new SimpleOrderedMap<>();
    if (request.getFieldTypes() != null)  {
      for (String fieldTypeName : request.getFieldTypes()) {
        FieldType fieldType = schema.getFieldTypes().get(fieldTypeName);
        fieldTypeAnalysisResults.add(fieldTypeName, analyzeValues(request, fieldType, null));
      }
    }

    NamedList<NamedList> fieldNameAnalysisResults = new SimpleOrderedMap<>();
    if (request.getFieldNames() != null)  {
      for (String fieldName : request.getFieldNames()) {
        FieldType fieldType = schema.getFieldType(fieldName);
        fieldNameAnalysisResults.add(fieldName, analyzeValues(request, fieldType, fieldName));
      }
    }

    analysisResults.add("field_types", fieldTypeAnalysisResults);
    analysisResults.add("field_names", fieldNameAnalysisResults);

    return analysisResults;
  }

  /**
   * Analyzes the index value (if it exists) and the query value (if it exists) in the given AnalysisRequest, using
   * the Analyzers of the given field type.
   *
   * @param analysisRequest AnalysisRequest from where the index and query values will be taken
   * @param fieldType       Type of field whose analyzers will be used
   * @param fieldName       Name of the field to be analyzed.  Can be {@code null}
   *
   * @return NamedList containing the tokens produced by the analyzers of the given field, separated into an index and
   *         a query group
   */ // package access for testing
  @SuppressWarnings({"rawtypes"})
  NamedList<NamedList> analyzeValues(FieldAnalysisRequest analysisRequest, FieldType fieldType, String fieldName) {

    final String queryValue = analysisRequest.getQuery();
    final Set<BytesRef> termsToMatch = (queryValue != null && analysisRequest.isShowMatch())
      ? getQueryTokenSet(queryValue, fieldType.getQueryAnalyzer())
      : EMPTY_BYTES_SET;

    NamedList<NamedList> analyzeResults = new SimpleOrderedMap<>();
    if (analysisRequest.getFieldValue() != null) {
      AnalysisContext context = new AnalysisContext(fieldName, fieldType, fieldType.getIndexAnalyzer(), termsToMatch);
      NamedList analyzedTokens = analyzeValue(analysisRequest.getFieldValue(), context);
      analyzeResults.add("index", analyzedTokens);
    }
    if (analysisRequest.getQuery() != null) {
      AnalysisContext context = new AnalysisContext(fieldName, fieldType, fieldType.getQueryAnalyzer());
      NamedList analyzedTokens = analyzeValue(analysisRequest.getQuery(), context);
      analyzeResults.add("query", analyzedTokens);
    }

    return analyzeResults;
  }
}
