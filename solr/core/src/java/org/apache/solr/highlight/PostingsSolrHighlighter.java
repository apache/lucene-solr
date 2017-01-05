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

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.postingshighlight.DefaultPassageFormatter;
import org.apache.lucene.search.postingshighlight.Passage;
import org.apache.lucene.search.postingshighlight.PassageFormatter;
import org.apache.lucene.search.postingshighlight.PassageScorer;
import org.apache.lucene.search.postingshighlight.PostingsHighlighter;
import org.apache.lucene.search.postingshighlight.WholeBreakIterator;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
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
 *   &lt;requestHandler name="/select" class="solr.SearchHandler"&gt;
 *     &lt;lst name="defaults"&gt;
 *       &lt;str name="hl.method"&gt;postings&lt;/str&gt;
 *       &lt;int name="hl.snippets"&gt;1&lt;/int&gt;
 *       &lt;str name="hl.tag.pre"&gt;&amp;lt;em&amp;gt;&lt;/str&gt;
 *       &lt;str name="hl.tag.post"&gt;&amp;lt;/em&amp;gt;&lt;/str&gt;
 *       &lt;str name="hl.tag.ellipsis"&gt;... &lt;/str&gt;
 *       &lt;bool name="hl.defaultSummary"&gt;true&lt;/bool&gt;
 *       &lt;str name="hl.encoder"&gt;simple&lt;/str&gt;
 *       &lt;float name="hl.score.k1"&gt;1.2&lt;/float&gt;
 *       &lt;float name="hl.score.b"&gt;0.75&lt;/float&gt;
 *       &lt;float name="hl.score.pivot"&gt;87&lt;/float&gt;
 *       &lt;str name="hl.bs.language"&gt;&lt;/str&gt;
 *       &lt;str name="hl.bs.country"&gt;&lt;/str&gt;
 *       &lt;str name="hl.bs.variant"&gt;&lt;/str&gt;
 *       &lt;str name="hl.bs.type"&gt;SENTENCE&lt;/str&gt;
 *       &lt;int name="hl.maxAnalyzedChars"&gt;10000&lt;/int&gt;
 *       &lt;str name="hl.multiValuedSeparatorChar"&gt; &lt;/str&gt;
 *       &lt;bool name="hl.highlightMultiTerm"&gt;false&lt;/bool&gt;
 *     &lt;/lst&gt;
 *   &lt;/requestHandler&gt;
 * </pre>
 * <p>
 * Notes:
 *  <ul>
 *    <li>fields to highlight must be configured with storeOffsetsWithPositions="true"
 *    <li>hl.q (string) can specify the query
 *    <li>hl.fl (string) specifies the field list.
 *    <li>hl.snippets (int) specifies how many underlying passages form the resulting snippet.
 *    <li>hl.tag.pre (string) specifies text which appears before a highlighted term.
 *    <li>hl.tag.post (string) specifies text which appears after a highlighted term.
 *    <li>hl.tag.ellipsis (string) specifies text which joins non-adjacent passages.
 *    <li>hl.defaultSummary (bool) specifies if a field should have a default summary.
 *    <li>hl.encoder (string) can be 'html' (html escapes content) or 'simple' (no escaping).
 *    <li>hl.score.k1 (float) specifies bm25 scoring parameter 'k1'
 *    <li>hl.score.b (float) specifies bm25 scoring parameter 'b'
 *    <li>hl.score.pivot (float) specifies bm25 scoring parameter 'avgdl'
 *    <li>hl.bs.type (string) specifies how to divide text into passages: [SENTENCE, LINE, WORD, CHAR, WHOLE]
 *    <li>hl.bs.language (string) specifies language code for BreakIterator. default is empty string (root locale)
 *    <li>hl.bs.country (string) specifies country code for BreakIterator. default is empty string (root locale)
 *    <li>hl.bs.variant (string) specifies country code for BreakIterator. default is empty string (root locale)
 *    <li>hl.maxAnalyzedChars specifies how many characters at most will be processed in a document.
 *    <li>hl.multiValuedSeparatorChar specifies the logical separator between values for multi-valued fields.
 *    <li>hl.highlightMultiTerm enables highlighting for range/wildcard/fuzzy/prefix queries.
 *        NOTE: currently hl.maxAnalyzedChars cannot yet be specified per-field
 *  </ul>
 *  
 * @lucene.experimental 
 */
public class PostingsSolrHighlighter extends SolrHighlighter implements PluginInfoInitialized {
  
  @Override
  public void init(PluginInfo info) {}

  @Override
  public NamedList<Object> doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields) throws IOException {
    final SolrParams params = req.getParams(); 
    
    // if highlighting isnt enabled, then why call doHighlighting?
    if (!isHighlightingEnabled(params))
      return null;

    SolrIndexSearcher searcher = req.getSearcher();
    int[] docIDs = toDocIDs(docs);

    // fetch the unique keys
    String[] keys = getUniqueKeys(searcher, docIDs);

    // query-time parameters
    String[] fieldNames = getHighlightFields(query, req, defaultFields);

    int maxPassages[] = new int[fieldNames.length];
    for (int i = 0; i < fieldNames.length; i++) {
      maxPassages[i] = params.getFieldInt(fieldNames[i], HighlightParams.SNIPPETS, 1);
    }

    PostingsHighlighter highlighter = getHighlighter(req);
    Map<String,String[]> snippets = highlighter.highlightFields(fieldNames, query, searcher, docIDs, maxPassages);
    return encodeSnippets(keys, fieldNames, snippets);
  }

  /** Creates an instance of the Lucene PostingsHighlighter. Provided for subclass extension so that
   * a subclass can return a subclass of {@link PostingsSolrHighlighter.SolrExtendedPostingsHighlighter}. */
  protected PostingsHighlighter getHighlighter(SolrQueryRequest req) {
    return new SolrExtendedPostingsHighlighter(req);
  }

  /** 
   * Encodes the resulting snippets into a namedlist
   * @param keys the document unique keys
   * @param fieldNames field names to highlight in the order
   * @param snippets map from field name to snippet array for the docs
   * @return encoded namedlist of summaries
   */
  protected NamedList<Object> encodeSnippets(String[] keys, String[] fieldNames, Map<String,String[]> snippets) {
    NamedList<Object> list = new SimpleOrderedMap<>();
    for (int i = 0; i < keys.length; i++) {
      NamedList<Object> summary = new SimpleOrderedMap<>();
      for (String field : fieldNames) {
        String snippet = snippets.get(field)[i];
        // box in an array to match the format of existing highlighters, 
        // even though it's always one element.
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
  
  /** Converts solr's DocList to the int[] docIDs */
  protected int[] toDocIDs(DocList docs) {
    int[] docIDs = new int[docs.size()];
    DocIterator iterator = docs.iterator();
    for (int i = 0; i < docIDs.length; i++) {
      if (!iterator.hasNext()) {
        throw new AssertionError();
      }
      docIDs[i] = iterator.nextDoc();
    }
    if (iterator.hasNext()) {
      throw new AssertionError();
    }
    return docIDs;
  }
  
  /** Retrieves the unique keys for the topdocs to key the results */
  protected String[] getUniqueKeys(SolrIndexSearcher searcher, int[] docIDs) throws IOException {
    IndexSchema schema = searcher.getSchema();
    SchemaField keyField = schema.getUniqueKeyField();
    if (keyField != null) {
      Set<String> selector = Collections.singleton(keyField.getName());
      String uniqueKeys[] = new String[docIDs.length];
      for (int i = 0; i < docIDs.length; i++) {
        int docid = docIDs[i];
        Document doc = searcher.doc(docid, selector);
        String id = schema.printableUniqueKey(doc);
        uniqueKeys[i] = id;
      }
      return uniqueKeys;
    } else {
      return new String[docIDs.length];
    }
  }

  /** From {@link #getHighlighter(org.apache.solr.request.SolrQueryRequest)}. */
  public class SolrExtendedPostingsHighlighter extends PostingsHighlighter {
    protected final SolrParams params;
    protected final IndexSchema schema;

    public SolrExtendedPostingsHighlighter(SolrQueryRequest req) {
      super(req.getParams().getInt(HighlightParams.MAX_CHARS, PostingsHighlighter.DEFAULT_MAX_LENGTH));
      this.params = req.getParams();
      this.schema = req.getSchema();
    }

    @Override
    protected Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
      boolean defaultSummary = params.getFieldBool(fieldName, HighlightParams.DEFAULT_SUMMARY, true);
      if (defaultSummary) {
        return super.getEmptyHighlight(fieldName, bi, maxPassages);
      } else {
        //TODO reuse logic of DefaultSolrHighlighter.alternateField
        return new Passage[0];
      }
    }

    @Override
    protected PassageFormatter getFormatter(String fieldName) {
      String preTag = params.getFieldParam(fieldName, HighlightParams.TAG_PRE, "<em>");
      String postTag = params.getFieldParam(fieldName, HighlightParams.TAG_POST, "</em>");
      String ellipsis = params.getFieldParam(fieldName, HighlightParams.TAG_ELLIPSIS, "... ");
      String encoder = params.getFieldParam(fieldName, HighlightParams.ENCODER, "simple");
      return new DefaultPassageFormatter(preTag, postTag, ellipsis, "html".equals(encoder));
    }

    @Override
    protected PassageScorer getScorer(String fieldName) {
      float k1 = params.getFieldFloat(fieldName, HighlightParams.SCORE_K1, 1.2f);
      float b = params.getFieldFloat(fieldName, HighlightParams.SCORE_B, 0.75f);
      float pivot = params.getFieldFloat(fieldName, HighlightParams.SCORE_PIVOT, 87f);
      return new PassageScorer(k1, b, pivot);
    }

    @Override
    protected BreakIterator getBreakIterator(String field) {
      String language = params.getFieldParam(field, HighlightParams.BS_LANGUAGE);
      String country = params.getFieldParam(field, HighlightParams.BS_COUNTRY);
      String variant = params.getFieldParam(field, HighlightParams.BS_VARIANT);
      Locale locale = parseLocale(language, country, variant);
      String type = params.getFieldParam(field, HighlightParams.BS_TYPE);
      return parseBreakIterator(type, locale);
    }

    @Override
    protected char getMultiValuedSeparator(String field) {
      String sep = params.getFieldParam(field, HighlightParams.MULTI_VALUED_SEPARATOR, " ");
      if (sep.length() != 1) {
        throw new IllegalArgumentException(HighlightParams.MULTI_VALUED_SEPARATOR + " must be exactly one character.");
      }
      return sep.charAt(0);
    }

    @Override
    protected Analyzer getIndexAnalyzer(String field) {
      if (params.getFieldBool(field, HighlightParams.HIGHLIGHT_MULTI_TERM, false)) {
        return schema.getIndexAnalyzer();
      } else {
        return null;
      }
    }
  }

  /** parse a break iterator type for the specified locale */
  protected BreakIterator parseBreakIterator(String type, Locale locale) {
    if (type == null || "SENTENCE".equals(type)) {
      return BreakIterator.getSentenceInstance(locale);
    } else if ("LINE".equals(type)) {
      return BreakIterator.getLineInstance(locale);
    } else if ("WORD".equals(type)) {
      return BreakIterator.getWordInstance(locale);
    } else if ("CHARACTER".equals(type)) {
      return BreakIterator.getCharacterInstance(locale);
    } else if ("WHOLE".equals(type)) {
      return new WholeBreakIterator();
    } else {
      throw new IllegalArgumentException("Unknown " + HighlightParams.BS_TYPE + ": " + type);
    }
  }
  
  /** parse a locale from a language+country+variant spec */
  protected Locale parseLocale(String language, String country, String variant) {
    if (language == null && country == null && variant == null) {
      return Locale.ROOT;
    } else if (language != null && country == null && variant != null) {
      throw new IllegalArgumentException("To specify variant, country is required");
    } else if (language != null && country != null && variant != null) {
      return new Locale(language, country, variant);
    } else if (language != null && country != null) {
      return new Locale(language, country);
    } else { 
      return new Locale(language);
    }
  }
}
