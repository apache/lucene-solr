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
package org.apache.solr.highlight;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.util.SolrPluginUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public abstract class SolrHighlighter
{

  public static int DEFAULT_MAX_CHARS = 51200;
  public static int DEFAULT_PHRASE_LIMIT = 5000;

  /**
   * Check whether Highlighting is enabled for this request.
   * @param params The params controlling Highlighting
   * @return <code>true</code> if highlighting enabled, <code>false</code> if not.
   */
  public boolean isHighlightingEnabled(SolrParams params) {
    return params.getBool(HighlightParams.HIGHLIGHT, false);
  }

  /**
   * Return a String array of the fields to be highlighted.
   * Falls back to the programmatic defaults, or the default search field if the list of fields
   * is not specified in either the handler configuration or the request.
   * @param query The current Query
   * @param request The current SolrQueryRequest
   * @param defaultFields Programmatic default highlight fields, used if nothing is specified in the handler config or the request.
   */
  public String[] getHighlightFields(Query query, SolrQueryRequest request, String[] defaultFields) {
    String fields[] = request.getParams().getParams(HighlightParams.FIELDS);

    // if no fields specified in the request, or the handler, fall back to programmatic default, or default search field.
    if(emptyArray(fields)) {
      // use default search field from request if highlight fieldlist not specified.
      if (emptyArray(defaultFields)) {
        String defaultSearchField = request.getParams().get(CommonParams.DF);
        fields = null == defaultSearchField ? new String[]{} : new String[]{defaultSearchField};
      } else {
        fields = defaultFields;
      }
    } else {
      Set<String> expandedFields = new LinkedHashSet<String>();
      Collection<String> storedHighlightFieldNames = request.getSearcher().getDocFetcher().getStoredHighlightFieldNames();
      for (String field : fields) {
        expandWildcardsInHighlightFields(
            expandedFields,
            storedHighlightFieldNames,
            SolrPluginUtils.split(field));
      }
      fields = expandedFields.toArray(new String[]{});
    }

    // Trim them now in case they haven't been yet.  Not needed for all code-paths above but do it here.
    for (int i = 0; i < fields.length; i++) {
      fields[i] = fields[i].trim();
    }
    return fields;
  }

  protected boolean emptyArray(String[] arr) {
    return (arr == null || arr.length == 0 || arr[0] == null || arr[0].trim().length() == 0);
  }

  static private void expandWildcardsInHighlightFields (
      Set<String> expandedFields,
      Collection<String> storedHighlightFieldNames,
      String... fields) {
    for (String field : fields) {
      if (field.contains("*")) {
        // create a Java regular expression from the wildcard string
        String fieldRegex = field.replaceAll("\\*", ".*");
        for (String storedFieldName : storedHighlightFieldNames) {
          if (storedFieldName.matches(fieldRegex)) {
            expandedFields.add(storedFieldName);
          }
        }
      } else {
        expandedFields.add(field);
      }
    }
  }

  /**
   * Generates a list of Highlighted query fragments for each item in a list
   * of documents, or returns null if highlighting is disabled.
   *
   * @param docs query results
   * @param query the query
   * @param req the current request
   * @param defaultFields default list of fields to summarize
   *
   * @return NamedList containing a NamedList for each document, which in
   * turns contains sets (field, summary) pairs.
   */
  @SuppressWarnings("unchecked")
  public abstract NamedList<Object> doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields) throws IOException;
}
