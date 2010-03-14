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
package org.apache.solr.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.ListIterator;

import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.*;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.highlight.DefaultSolrHighlighter;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.*;

/**
 * DEPRECATED Collection of Utility and Factory methods for Highlighting.
 *
 * @deprecated use DefaultSolrHighlighter
 * @see DefaultSolrHighlighter
 */
public class HighlightingUtils implements HighlightParams {

   static SolrParams DEFAULTS = null;
   static {
      Map<String,String> map = new HashMap<String,String>();
      map.put(SNIPPETS, "1");
      map.put(FRAGSIZE, "100");
      map.put(FORMATTER, SIMPLE);
      map.put(SIMPLE_PRE, "<em>");
      map.put(SIMPLE_POST, "</em>");
      
      DEFAULTS = new MapSolrParams(map);
   }
  
  private static SolrHighlighterX HIGHLIGHTER = new SolrHighlighterX();
  
   /** Combine request parameters with highlighting defaults. */
   static SolrParams getParams(SolrQueryRequest request) {
      return new DefaultSolrParams(request.getParams(), DEFAULTS);
   }
   
   /**
    * @deprecated use DefaultSolrHighlighter
    * @see DefaultSolrHighlighter#isHighlightingEnabled
    */
   public static boolean isHighlightingEnabled(SolrQueryRequest request) {
     return HIGHLIGHTER.isHighlightingEnabled(getParams(request));
   }
   
   /**
    * @deprecated use DefaultSolrHighlighter
    * @see DefaultSolrHighlighter
    */
   public static Highlighter getHighlighter(Query query, String fieldName, SolrQueryRequest request) {
     return HIGHLIGHTER.getHighlighterX(query, fieldName, request);
   }
   
   /**
    * @deprecated use DefaultSolrHighlighter
    * @see DefaultSolrHighlighter#getHighlightFields
    */
   public static String[] getHighlightFields(Query query, SolrQueryRequest request, String[] defaultFields) {
     return HIGHLIGHTER.getHighlightFields(query, request, defaultFields);
   }
   
   /**
    * @deprecated use DefaultSolrHighlighter
    * @see DefaultSolrHighlighter
    */
   public static int getMaxSnippets(String fieldName, SolrQueryRequest request) {
     return HIGHLIGHTER.getMaxSnippetsX(fieldName, request);
   }
   
   /**
    * @deprecated use DefaultSolrHighlighter
    * @see DefaultSolrHighlighter
    */
   public static Formatter getFormatter(String fieldName, SolrQueryRequest request) {
     return HIGHLIGHTER.getFormatterX(fieldName, request);
   }
   
   /**
    * @deprecated use DefaultSolrHighlighter
    * @see DefaultSolrHighlighter
    */
   public static Fragmenter getFragmenter(String fieldName, SolrQueryRequest request) {
     return HIGHLIGHTER.getFragmenterX(fieldName, request);
   }
   
   /**
    * @deprecated use DefaultSolrHighlighter
    * @see DefaultSolrHighlighter#doHighlighting
    */
   @SuppressWarnings("unchecked")
   public static NamedList doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields) throws IOException {
     return HIGHLIGHTER.doHighlighting(docs, query, req, defaultFields);
   }
}

/**
 * subclass containing package protected versions of some protected methods, used for proxying calls to deprecated methods that have been moved and made protected.
 */
class SolrHighlighterX extends DefaultSolrHighlighter {
  Highlighter getHighlighterX(Query query, String fieldName, SolrQueryRequest request) {
    return getHighlighter(query, fieldName, request);
  }
  int getMaxSnippetsX(String fieldName, SolrQueryRequest request) {
    return getMaxSnippets(fieldName, HighlightingUtils.getParams(request));
  }
  Formatter getFormatterX(String fieldName, SolrQueryRequest request) {
        return getFormatter(fieldName, HighlightingUtils.getParams(request));
  }
  Fragmenter getFragmenterX(String fieldName, SolrQueryRequest request) {
    return getFragmenter(fieldName, HighlightingUtils.getParams(request));
  }
}

