package org.apache.solr.highlight;

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

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.postingshighlight.PassageFormatter;
import org.apache.lucene.search.postingshighlight.PassageScorer;
import org.apache.lucene.search.postingshighlight.PostingsHighlighter;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.PluginInfoInitialized;

/** 
 * Highlighter impl that uses {@link PostingsHighlighter}
 * <p>
 * Example configuration:
 * <pre class="prettyprint">
 *   &lt;searchComponent class="solr.HighlightComponent" name="highlight"&gt;
 *     &lt;highlighting class="org.apache.solr.highlight.PostingsSolrHighlighter"
 *                      preTag="&amp;lt;em&amp;gt;"
 *                      postTag="&amp;lt;/em&amp;gt;"
 *                      ellipsis="... "
 *                      maxLength=10000/&gt;
 *   &lt;/searchComponent&gt;
 * </pre>
 * <p>
 * Notes:
 *  <ul>
 *    <li>fields to highlight must be configured with storeOffsetsWithPositions="true"
 *    <li>hl.fl specifies the field list.
 *    <li>hl.snippets specifies how many underlying sentence fragments form the resulting snippet.
 *  </ul>
 *  
 * @lucene.experimental 
 */
public class PostingsSolrHighlighter extends SolrHighlighter implements PluginInfoInitialized {
  protected PostingsHighlighter highlighter;

  @Override
  public void initalize(SolrConfig config) {}
  
  @Override
  public void init(PluginInfo info) {
    Map<String,String> attributes = info.attributes;
    BreakIterator breakIterator = BreakIterator.getSentenceInstance(Locale.ROOT);
    PassageScorer scorer = new PassageScorer();
    
    // formatter parameters: preTag/postTag/ellipsis
    String preTag = attributes.get("preTag");
    if (preTag == null) {
      preTag = "<em>";
    }
    String postTag = attributes.get("postTag");
    if (postTag == null) {
      postTag = "</em>";
    }
    String ellipsis = attributes.get("ellipsis");
    if (ellipsis == null) {
      ellipsis = "... ";
    }
    PassageFormatter formatter = new PassageFormatter(preTag, postTag, ellipsis);
    
    // maximum content size to process
    int maxLength = PostingsHighlighter.DEFAULT_MAX_LENGTH;
    if (attributes.containsKey("maxLength")) {
      maxLength = Integer.parseInt(attributes.get("maxLength"));
    }
    highlighter = new PostingsHighlighter(maxLength, breakIterator, scorer, formatter);
  }

  @Override
  public NamedList<Object> doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields) throws IOException {
    SolrParams params = req.getParams(); 
    
    // if highlighting isnt enabled, then why call doHighlighting?
    if (isHighlightingEnabled(params)) {
      SolrIndexSearcher searcher = req.getSearcher();
      TopDocs topDocs = toTopDocs(docs);
      
      // fetch the unique keys
      String[] keys = getUniqueKeys(searcher, topDocs);
      
      // query-time parameters
      String[] fieldNames = getHighlightFields(query, req, defaultFields);
      int numSnippets = params.getInt(HighlightParams.SNIPPETS, 1);
      
      Map<String,String[]> snippets = highlighter.highlightFields(fieldNames, query, searcher, topDocs, numSnippets);
      return encodeSnippets(keys, fieldNames, snippets);
    } else {
      return null;
    }
  }
  
  /** 
   * Encodes the resulting snippets into a namedlist
   * @param keys the document unique keys
   * @param fieldNames field names to highlight in the order
   * @param snippets map from field name to snippet array for the docs
   * @return encoded namedlist of summaries
   */
  protected NamedList<Object> encodeSnippets(String[] keys, String[] fieldNames, Map<String,String[]> snippets) {
    NamedList<Object> list = new SimpleOrderedMap<Object>();
    for (int i = 0; i < keys.length; i++) {
      NamedList<Object> summary = new SimpleOrderedMap<Object>();
      for (String field : fieldNames) {
        String snippet = snippets.get(field)[i];
        // box in an array to match the format of existing highlighters, 
        // even though its always one element.
        if (snippet == null) {
          summary.add(field, new String[0]);
        } else {
          summary.add(field, new String[] { snippet });
        }
      }
      list.add(keys[i], summary);
    }
    return list;
  }
  
  /** Converts solr's DocList to a lucene TopDocs */
  protected TopDocs toTopDocs(DocList docs) {
    ScoreDoc[] scoreDocs = new ScoreDoc[docs.size()];
    DocIterator iterator = docs.iterator();
    for (int i = 0; i < scoreDocs.length; i++) {
      if (!iterator.hasNext()) {
        throw new AssertionError();
      }
      scoreDocs[i] = new ScoreDoc(iterator.nextDoc(), Float.NaN);
    }
    if (iterator.hasNext()) {
      throw new AssertionError();
    }
    return new TopDocs(docs.matches(), scoreDocs, Float.NaN);
  }
  
  /** Retrieves the unique keys for the topdocs to key the results */
  protected String[] getUniqueKeys(SolrIndexSearcher searcher, TopDocs topDocs) throws IOException {
    IndexSchema schema = searcher.getSchema();
    SchemaField keyField = schema.getUniqueKeyField();
    if (keyField != null) {
      Set<String> selector = Collections.singleton(keyField.getName());
      String uniqueKeys[] = new String[topDocs.scoreDocs.length];
      for (int i = 0; i < topDocs.scoreDocs.length; i++) {
        int docid = topDocs.scoreDocs[i].doc;
        Document doc = searcher.doc(docid, selector);
        String id = schema.printableUniqueKey(doc);
        uniqueKeys[i] = id;
      }
      return uniqueKeys;
    } else {
      return new String[topDocs.scoreDocs.length];
    }
  }
}
