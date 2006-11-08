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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.request.*;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.*;

/**
 * Collection of Utility and Factory methods for Highlighting.
 */
public class HighlightingUtils
{
   private static final String SIMPLE = "simple";
   
   private static final String HIGHLIGHT = "hl";
   private static final String PREFIX = "hl.";
   private static final String FIELDS = PREFIX+"fl";
   private static final String SNIPPETS = PREFIX+"snippets";
   private static final String FRAGSIZE = PREFIX+"fragsize";
   private static final String FORMATTER = PREFIX+"formatter";
   private static final String SIMPLE_PRE = PREFIX+SIMPLE+".pre";
   private static final String SIMPLE_POST = PREFIX+SIMPLE+".post";
   private static final String FIELD_MATCH = PREFIX+"requireFieldMatch";
   
   private static SolrParams DEFAULTS = null;
   static
   {
      Map<String,String> map = new HashMap<String,String>();
      map.put(SNIPPETS, "1");
      map.put(FRAGSIZE, "100");
      map.put(FORMATTER, SIMPLE);
      map.put(SIMPLE_PRE, "<em>");
      map.put(SIMPLE_POST, "</em>");
      
      DEFAULTS = new MapSolrParams(map);
   }
   
   /** Combine request parameters with highlighting defaults. */
   private static SolrParams getParams(SolrQueryRequest request)
   {
      return new DefaultSolrParams(request.getParams(), DEFAULTS);
   }
   
   /**
    * Check whether Highlighting is enabled for this request.
    * @param request The current SolrQueryRequest
    * @return <code>true</code> if highlighting enabled, <code>false</code> if not.
    */
   public static boolean isHighlightingEnabled(SolrQueryRequest request)
   {
      return getParams(request).getBool(HIGHLIGHT, false);
   }
   
   /**
    * Return a Highlighter appropriate for this field.
    * @param query The current Query
    * @param fieldName The name of the field
    * @param request The current SolrQueryRequest
    */
   public static Highlighter getHighlighter(Query query, String fieldName, SolrQueryRequest request)
   {
      Highlighter highlighter = new Highlighter(
            getFormatter(fieldName, request), 
            getQueryScorer(query, fieldName, request));
      highlighter.setTextFragmenter(getFragmenter(fieldName, request));
      return highlighter;
   }
   
   /**
    * Return a QueryScorer suitable for this Query and field.
    * @param query The current query
    * @param fieldName The name of the field
    * @param request The SolrQueryRequest
    */
   public static QueryScorer getQueryScorer(Query query, String fieldName, SolrQueryRequest request)
   {
      boolean reqFieldMatch = getParams(request).getFieldBool(fieldName, FIELD_MATCH, false);
      if (reqFieldMatch)
      {
         return new QueryScorer(query, request.getSearcher().getReader(), fieldName);
      }
      else
      {
         return new QueryScorer(query);
      }
   }
   
   /**
    * Return a String array of the fields to be highlighted.
    * Falls back to the programatic defaults, or the default search field if the list of fields
    * is not specified in either the handler configuration or the request.
    * @param query The current Query
    * @param request The current SolrQueryRequest
    * @param defaultFields Programmatic default highlight fields, used if nothing is specified in the handler config or the request.
    */
   public static String[] getHighlightFields(Query query, SolrQueryRequest request, String[] defaultFields)
   {
      String fields[] = getParams(request).getParams(FIELDS);
      
      // if no fields specified in the request, or the handler, fall back to programmatic default, or default search field.
      if(emptyArray(fields)) 
      {
         // use default search field if highlight fieldlist not specified.
         if (emptyArray(defaultFields)) 
         {
            fields = new String[]{request.getSchema().getDefaultSearchFieldName()};
         }  
         else
         {
            fields = defaultFields;
         }
      }
      else if (fields.length == 1)
      {
         // if there's a single request/handler value, it may be a space/comma separated list
         fields = SolrPluginUtils.split(fields[0]);
      }
      
      return fields;
   }
   
   private static boolean emptyArray(String[] arr)
   {
      return (arr == null || arr.length == 0 || arr[0] == null || arr[0].trim().length() == 0);
   }
   
   /**
    * Return the max number of snippets for this field. If this has not
    * been configured for this field, fall back to the configured default
    * or the solr default.
    * @param fieldName The name of the field
    * @param request The current SolrQueryRequest
    */
   public static int getMaxSnippets(String fieldName, SolrQueryRequest request)
   {
      return Integer.parseInt(getParams(request).getFieldParam(fieldName, SNIPPETS));
   }
   
   /**
    * Return a formatter appropriate for this field. If a formatter
    * has not been configured for this field, fall back to the configured
    * default or the solr default (SimpleHTMLFormatter).
    * 
    * @param fieldName The name of the field
    * @param request The current SolrQueryRequest
    * @return An appropriate Formatter.
    */
   public static Formatter getFormatter(String fieldName, SolrQueryRequest request)
   {
      SolrParams p = getParams(request);
      
      // SimpleHTMLFormatter is the only supported Formatter at the moment
      return new SimpleHTMLFormatter(p.getFieldParam(fieldName, SIMPLE_PRE), p.getFieldParam(fieldName, SIMPLE_POST));
   }
   
   /**
    * Return a fragmenter appropriate for this field. If a fragmenter
    * has not been configured for this field, fall back to the configured
    * default or the solr default (GapFragmenter).
    * 
    * @param fieldName The name of the field
    * @param request The current SolrQueryRequest
    * @return An appropriate Fragmenter.
    */
   public static Fragmenter getFragmenter(String fieldName, SolrQueryRequest request)
   {
      int fragsize = Integer.parseInt(getParams(request).getFieldParam(fieldName, FRAGSIZE)); 
      return (fragsize <= 0) ? new NullFragmenter() : new GapFragmenter(fragsize);
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
   public static NamedList doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields) throws IOException
   {
      if (!isHighlightingEnabled(req))
         return null;
      
      SolrIndexSearcher searcher = req.getSearcher();
      NamedList fragments = new NamedList();
      String[] fieldNames = getHighlightFields(query, req, defaultFields);

      DocIterator iterator = docs.iterator();
      for (int i = 0; i < docs.size(); i++)
      {
         int docId = iterator.nextDoc();
         // use the Searcher's doc cache
         Document doc = searcher.doc(docId);
         NamedList docSummaries = new NamedList();
         for (String fieldName : fieldNames)
         {
            fieldName = fieldName.trim();
            String[] docTexts = doc.getValues(fieldName);
            if (docTexts == null) continue;

            // get highlighter, and number of fragments for this field
            Highlighter highlighter = getHighlighter(query, fieldName, req);
            int numFragments = getMaxSnippets(fieldName, req);

            String[] summaries;
            TextFragment[] frag;
            if (docTexts.length == 1)
            {
               // single-valued field
               TokenStream tstream;
               try
               {
                  // attempt term vectors
                  tstream = TokenSources.getTokenStream(searcher.getReader(), docId, fieldName);
               }
               catch (IllegalArgumentException e)
               {
                  // fall back to analyzer
                  tstream = new TokenOrderingFilter(searcher.getSchema().getAnalyzer().tokenStream(fieldName, new StringReader(docTexts[0])), 10);
               }
               frag = highlighter.getBestTextFragments(tstream, docTexts[0], false, numFragments);
            }
            else
            {
               // multi-valued field
               MultiValueTokenStream tstream;
               tstream = new MultiValueTokenStream(fieldName, docTexts, searcher.getSchema().getAnalyzer(), true);
               frag = highlighter.getBestTextFragments(tstream, tstream.asSingleValue(), false, numFragments);
            }
            // convert fragments back into text
            // TODO: we can include score and position information in output as snippet attributes
            if (frag.length > 0)
            {
               ArrayList<String> fragTexts = new ArrayList<String>();
               for (int j = 0; j < frag.length; j++)
               {
                  if ((frag[j] != null) && (frag[j].getScore() > 0))
                  {
                     fragTexts.add(frag[j].toString());
                  }
               }
               summaries = fragTexts.toArray(new String[0]);
               if (summaries.length > 0) docSummaries.add(fieldName, summaries);
            }
         }
         String printId = searcher.getSchema().printableUniqueKey(doc);
         fragments.add(printId == null ? null : printId, docSummaries);
      }
      return fragments;
   }
}
