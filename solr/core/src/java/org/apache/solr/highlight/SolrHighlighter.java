package org.apache.solr.highlight;
/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.util.SolrPluginUtils;

public abstract class SolrHighlighter
{
  public static Logger log = LoggerFactory.getLogger(SolrHighlighter.class);

  // Thread safe registry
  protected final Map<String,SolrFormatter> formatters =
    new HashMap<String, SolrFormatter>();

  // Thread safe registry
  protected final Map<String,SolrEncoder> encoders =
    new HashMap<String, SolrEncoder>();

  // Thread safe registry
  protected final Map<String,SolrFragmenter> fragmenters =
    new HashMap<String, SolrFragmenter>() ;

  // Thread safe registry
  protected final Map<String, SolrFragListBuilder> fragListBuilders =
    new HashMap<String, SolrFragListBuilder>() ;

  // Thread safe registry
  protected final Map<String, SolrFragmentsBuilder> fragmentsBuilders =
    new HashMap<String, SolrFragmentsBuilder>() ;

  @Deprecated
  public abstract void initalize( SolrConfig config );


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
   * Falls back to the programatic defaults, or the default search field if the list of fields
   * is not specified in either the handler configuration or the request.
   * @param query The current Query
   * @param request The current SolrQueryRequest
   * @param defaultFields Programmatic default highlight fields, used if nothing is specified in the handler config or the request.
   */
  public String[] getHighlightFields(Query query, SolrQueryRequest request, String[] defaultFields) {
    String fields[] = request.getParams().getParams(HighlightParams.FIELDS);

    // if no fields specified in the request, or the handler, fall back to programmatic default, or default search field.
    if(emptyArray(fields)) {
      // use default search field if highlight fieldlist not specified.
      if (emptyArray(defaultFields)) {
        String defaultSearchField = request.getSchema().getDefaultSearchFieldName();
        fields = null == defaultSearchField ? new String[]{} : new String[]{defaultSearchField};
      }
      else {
        fields = defaultFields;
      }
    }
    else if (fields.length == 1) {
      if (fields[0].contains("*")) {
        // create a Java regular expression from the wildcard string
        String fieldRegex = fields[0].replaceAll("\\*", ".*");
        Collection<String> storedHighlightFieldNames = request.getSearcher().getStoredHighlightFieldNames();
        List<String> storedFieldsToHighlight = new ArrayList<String>();
        for (String storedFieldName: storedHighlightFieldNames) {
            if (storedFieldName.matches(fieldRegex)) {
              storedFieldsToHighlight.add(storedFieldName);
            }
        }
        fields = storedFieldsToHighlight.toArray(new String[] {});
      } else {
        // if there's a single request/handler value, it may be a space/comma separated list
        fields = SolrPluginUtils.split(fields[0]);
      }
    }

    return fields;
  }

  protected boolean emptyArray(String[] arr) {
    return (arr == null || arr.length == 0 || arr[0] == null || arr[0].trim().length() == 0);
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
