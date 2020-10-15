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
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.DefaultPassageFormatter;
import org.apache.lucene.search.uhighlight.LengthGoalBreakIterator;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.PassageScorer;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.search.uhighlight.WholeBreakIterator;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
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
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.plugin.PluginInfoInitialized;

/**
 * Highlighter impl that uses {@link UnifiedHighlighter}
 * <p>
 * Example configuration with default values:
 * <pre class="prettyprint">
 * &lt;requestHandler name="/select" class="solr.SearchHandler"&gt;
 * &lt;lst name="defaults"&gt;
 * &lt;str name="hl.method"&gt;unified&lt;/str&gt;
 * &lt;int name="hl.snippets"&gt;1&lt;/int&gt;
 * &lt;str name="hl.tag.pre"&gt;&amp;lt;em&amp;gt;&lt;/str&gt;
 * &lt;str name="hl.tag.post"&gt;&amp;lt;/em&amp;gt;&lt;/str&gt;
 * &lt;str name="hl.simple.pre"&gt;&amp;lt;em&amp;gt;&lt;/str&gt;
 * &lt;str name="hl.simple.post"&gt;&amp;lt;/em&amp;gt;&lt;/str&gt;
 * &lt;str name="hl.tag.ellipsis"&gt;(internal/unspecified)&lt;/str&gt;
 * &lt;bool name="hl.defaultSummary"&gt;false&lt;/bool&gt;
 * &lt;str name="hl.encoder"&gt;simple&lt;/str&gt;
 * &lt;float name="hl.score.k1"&gt;1.2&lt;/float&gt;
 * &lt;float name="hl.score.b"&gt;0.75&lt;/float&gt;
 * &lt;float name="hl.score.pivot"&gt;87&lt;/float&gt;
 * &lt;str name="hl.bs.language"&gt;&lt;/str&gt;
 * &lt;str name="hl.bs.country"&gt;&lt;/str&gt;
 * &lt;str name="hl.bs.variant"&gt;&lt;/str&gt;
 * &lt;str name="hl.bs.type"&gt;SENTENCE&lt;/str&gt;
 * &lt;int name="hl.maxAnalyzedChars"&gt;51200&lt;/int&gt;
 * &lt;bool name="hl.highlightMultiTerm"&gt;true&lt;/bool&gt;
 * &lt;bool name="hl.usePhraseHighlighter"&gt;true&lt;/bool&gt;
 * &lt;int name="hl.cacheFieldValCharsThreshold"&gt;524288&lt;/int&gt;
 * &lt;str name="hl.offsetSource"&gt;&lt;/str&gt;
 * &lt;bool name="hl.weightMatches"&gt;true&lt;/bool&gt;
 * &lt;/lst&gt;
 * &lt;/requestHandler&gt;
 * </pre>
 * <p>
 * Notes:
 * <ul>
 * <li>hl.q (string) can specify the query
 * <li>hl.fl (string) specifies the field list.
 * <li>hl.snippets (int) specifies how many snippets to return.
 * <li>hl.tag.pre (string) specifies text which appears before a highlighted term.
 * <li>hl.tag.post (string) specifies text which appears after a highlighted term.
 * <li>hl.simple.pre (string) specifies text which appears before a highlighted term. (prefer hl.tag.pre)
 * <li>hl.simple.post (string) specifies text which appears before a highlighted term. (prefer hl.tag.post)
 * <li>hl.tag.ellipsis (string) specifies text which joins non-adjacent passages. The default is to retain each
 * value in a list without joining them.
 * <li>hl.defaultSummary (bool) specifies if a field should have a default summary of the leading text.
 * <li>hl.encoder (string) can be 'html' (html escapes content) or 'simple' (no escaping).
 * <li>hl.score.k1 (float) specifies bm25 scoring parameter 'k1'
 * <li>hl.score.b (float) specifies bm25 scoring parameter 'b'
 * <li>hl.score.pivot (float) specifies bm25 scoring parameter 'avgdl'
 * <li>hl.bs.type (string) specifies how to divide text into passages: [SENTENCE, LINE, WORD, CHAR, WHOLE]
 * <li>hl.bs.language (string) specifies language code for BreakIterator. default is empty string (root locale)
 * <li>hl.bs.country (string) specifies country code for BreakIterator. default is empty string (root locale)
 * <li>hl.bs.variant (string) specifies country code for BreakIterator. default is empty string (root locale)
 * <li>hl.maxAnalyzedChars (int) specifies how many characters at most will be processed in a document for any one field.
 * <li>hl.highlightMultiTerm (bool) enables highlighting for range/wildcard/fuzzy/prefix queries at some cost. default is true
 * <li>hl.usePhraseHighlighter (bool) enables phrase highlighting. default is true
 * <li>hl.cacheFieldValCharsThreshold (int) controls how many characters from a field are cached. default is 524288 (1MB in 2 byte chars)
 * <li>hl.offsetSource (string) specifies which offset source to use, prefers postings, but will use what's available if not specified
 * <li>hl.weightMatches (bool) enables Lucene Weight Matches mode</li>
 * </ul>
 *
 * @lucene.experimental
 */
public class UnifiedSolrHighlighter extends SolrHighlighter implements PluginInfoInitialized {

  protected static final String SNIPPET_SEPARATOR = "\u0000";
  private static final String[] ZERO_LEN_STR_ARRAY = new String[0];

  @Override
  public void init(PluginInfo info) {
  }

  @Override
  public NamedList<Object> doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields) throws IOException {
    final SolrParams params = req.getParams();

    // if highlighting isn't enabled, then why call doHighlighting?
    if (!isHighlightingEnabled(params))
      return null;

    int[] docIDs = toDocIDs(docs);

    // fetch the unique keys
    String[] keys = getUniqueKeys(req.getSearcher(), docIDs);

    // query-time parameters
    String[] fieldNames = getHighlightFields(query, req, defaultFields);

    int maxPassages[] = new int[fieldNames.length];
    for (int i = 0; i < fieldNames.length; i++) {
      maxPassages[i] = params.getFieldInt(fieldNames[i], HighlightParams.SNIPPETS, 1);
    }

    UnifiedHighlighter highlighter = getHighlighter(req);
    Map<String, String[]> snippets = highlighter.highlightFields(fieldNames, query, docIDs, maxPassages);
    return encodeSnippets(keys, fieldNames, snippets);
  }

  /**
   * Creates an instance of the Lucene {@link UnifiedHighlighter}. Provided for subclass extension so that
   * a subclass can return a subclass of {@link SolrExtendedUnifiedHighlighter}.
   */
  protected UnifiedHighlighter getHighlighter(SolrQueryRequest req) {
    return new SolrExtendedUnifiedHighlighter(req);
  }

  /**
   * Encodes the resulting snippets into a namedlist
   *
   * @param keys       the document unique keys
   * @param fieldNames field names to highlight in the order
   * @param snippets   map from field name to snippet array for the docs
   * @return encoded namedlist of summaries
   */
  protected NamedList<Object> encodeSnippets(String[] keys, String[] fieldNames, Map<String, String[]> snippets) {
    NamedList<Object> list = new SimpleOrderedMap<>();
    for (int i = 0; i < keys.length; i++) {
      NamedList<Object> summary = new SimpleOrderedMap<>();
      for (String field : fieldNames) {
        String snippet = snippets.get(field)[i];
        if (snippet == null) {
          //TODO reuse logic of DefaultSolrHighlighter.alternateField
          summary.add(field, ZERO_LEN_STR_ARRAY);
        } else {
          // we used a special snippet separator char and we can now split on it.
          summary.add(field, snippet.split(SNIPPET_SEPARATOR));
        }
      }
      list.add(keys[i], summary);
    }
    return list;
  }

  /**
   * Converts solr's DocList to the int[] docIDs
   */
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

  /**
   * Retrieves the unique keys for the topdocs to key the results
   */
  protected String[] getUniqueKeys(SolrIndexSearcher searcher, int[] docIDs) throws IOException {
    IndexSchema schema = searcher.getSchema();
    SchemaField keyField = schema.getUniqueKeyField();
    if (keyField != null) {
      SolrReturnFields returnFields = new SolrReturnFields(keyField.getName(), null);
      String[] uniqueKeys = new String[docIDs.length];
      for (int i = 0; i < docIDs.length; i++) {
        int docid = docIDs[i];
        SolrDocument solrDoc = searcher.getDocFetcher().solrDoc(docid, returnFields);
        uniqueKeys[i] = schema.printableUniqueKey(solrDoc);
      }
      return uniqueKeys;
    } else {
      return new String[docIDs.length];
    }
  }

  /**
   * From {@link #getHighlighter(org.apache.solr.request.SolrQueryRequest)}.
   */
  protected static class SolrExtendedUnifiedHighlighter extends UnifiedHighlighter {
    protected final static Predicate<String> NOT_REQUIRED_FIELD_MATCH_PREDICATE = s -> true;
    protected final SolrParams params;

    protected final IndexSchema schema;
    protected final RTimerTree loadFieldValuesTimer;

    public SolrExtendedUnifiedHighlighter(SolrQueryRequest req) {
      super(req.getSearcher(), req.getSchema().getIndexAnalyzer());
      this.params = req.getParams();
      this.schema = req.getSchema();
      this.setMaxLength(
          params.getInt(HighlightParams.MAX_CHARS, DEFAULT_MAX_CHARS));
      this.setCacheFieldValCharsThreshold(
          params.getInt(HighlightParams.CACHE_FIELD_VAL_CHARS_THRESHOLD, DEFAULT_CACHE_CHARS_THRESHOLD));

      final RTimerTree timerTree;
      if (req.getRequestTimer() != null) { //It may be null if not used in a search context.
        timerTree = req.getRequestTimer();
      } else {
        timerTree = new RTimerTree(); // since null checks are annoying
      }
      loadFieldValuesTimer = timerTree.sub("loadFieldValues"); // we assume a new timer, state of STARTED
      loadFieldValuesTimer.pause(); // state of PAUSED now with about zero time. Will fail if state isn't STARTED.
    }

    @Override
    protected OffsetSource getOffsetSource(String field) {
      String sourceStr = params.getFieldParam(field, HighlightParams.OFFSET_SOURCE);
      if (sourceStr != null) {
        return OffsetSource.valueOf(sourceStr.toUpperCase(Locale.ROOT));
      } else {
        return super.getOffsetSource(field);
      }
    }

    // optimization for Solr which keeps a FieldInfos on-hand
    @Override
    protected FieldInfo getFieldInfo(String field) {
      return ((SolrIndexSearcher)searcher).getFieldInfos().fieldInfo(field);
    }

    @Override
    public int getMaxNoHighlightPassages(String field) {
      boolean defaultSummary = params.getFieldBool(field, HighlightParams.DEFAULT_SUMMARY, false);
      if (defaultSummary) {
        return -1;// signifies return first hl.snippets passages worth of the content
      } else {
        return 0;// will return null
      }
    }

    @Override
    protected PassageFormatter getFormatter(String fieldName) {
      String preTag = params.getFieldParam(fieldName, HighlightParams.TAG_PRE,
          params.getFieldParam(fieldName, HighlightParams.SIMPLE_PRE, "<em>")
      );

      String postTag = params.getFieldParam(fieldName, HighlightParams.TAG_POST,
          params.getFieldParam(fieldName, HighlightParams.SIMPLE_POST, "</em>")
      );
      String ellipsis = params.getFieldParam(fieldName, HighlightParams.TAG_ELLIPSIS, SNIPPET_SEPARATOR);
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
      // Use a default fragsize the same as the regex Fragmenter (original Highlighter) since we're
      //  both likely shooting for sentence-like patterns.
      int fragsize = params.getFieldInt(field, HighlightParams.FRAGSIZE, LuceneRegexFragmenter.DEFAULT_FRAGMENT_SIZE);
      String type = params.getFieldParam(field, HighlightParams.BS_TYPE);
      if (fragsize == 0 || "WHOLE".equals(type)) { // 0 is special value; no fragmenting
        return new WholeBreakIterator();
      }

      BreakIterator baseBI;
      if ("SEPARATOR".equals(type)) {
        char customSep = parseBiSepChar(params.getFieldParam(field, HighlightParams.BS_SEP));
        baseBI = new CustomSeparatorBreakIterator(customSep);
      } else {
        String language = params.getFieldParam(field, HighlightParams.BS_LANGUAGE);
        String country = params.getFieldParam(field, HighlightParams.BS_COUNTRY);
        String variant = params.getFieldParam(field, HighlightParams.BS_VARIANT);
        Locale locale = parseLocale(language, country, variant);
        baseBI = parseBreakIterator(type, locale);
      }

      if (fragsize <= 1) { // no real minimum size
        return baseBI;
      }

      float fragalign = params.getFieldFloat(field, HighlightParams.FRAGALIGNRATIO, 0.5f);
      if (params.getFieldBool(field, HighlightParams.FRAGSIZEISMINIMUM, true)) {
        return LengthGoalBreakIterator.createMinLength(baseBI, fragsize, fragalign);
      }
      return LengthGoalBreakIterator.createClosestToLength(baseBI, fragsize, fragalign);
    }

    /**
     * parse custom separator char for {@link CustomSeparatorBreakIterator}
     */
    protected char parseBiSepChar(String sepChar) {
      if (sepChar == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, HighlightParams.BS_SEP + " not passed");
      }
      if (sepChar.length() != 1) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, HighlightParams.BS_SEP +
            " must be a single char but got: '" + sepChar + "'");
      }
      return sepChar.charAt(0);
    }

    /**
     * parse a break iterator type for the specified locale
     */
    protected BreakIterator parseBreakIterator(String type, Locale locale) {
      if (type == null || "SENTENCE".equals(type)) {
        return BreakIterator.getSentenceInstance(locale);
      } else if ("LINE".equals(type)) {
        return BreakIterator.getLineInstance(locale);
      } else if ("WORD".equals(type)) {
        return BreakIterator.getWordInstance(locale);
      } else if ("CHARACTER".equals(type)) {
        return BreakIterator.getCharacterInstance(locale);
      } else {
        throw new IllegalArgumentException("Unknown " + HighlightParams.BS_TYPE + ": " + type);
      }
    }

    /**
     * parse a locale from a language+country+variant spec
     */
    protected Locale parseLocale(String language, String country, String variant) {
      if (language == null && country == null && variant == null) {
        return Locale.ROOT;
      } else if (language == null) {
        throw new IllegalArgumentException("language is required if country or variant is specified");
      } else if (country == null && variant != null) {
        throw new IllegalArgumentException("To specify variant, country is required");
      } else if (country != null && variant != null) {
        return new Locale(language, country, variant);
      } else if (country != null) {
        return new Locale(language, country);
      } else {
        return new Locale(language);
      }
    }

    @Override
    protected List<CharSequence[]> loadFieldValues(String[] fields, DocIdSetIterator docIter, int
        cacheCharsThreshold) throws IOException {
      // Time loading field values.  It can be an expensive part of highlighting.
      loadFieldValuesTimer.resume();
      try {
        return super.loadFieldValues(fields, docIter, cacheCharsThreshold);
      } finally {
        loadFieldValuesTimer.pause(); // note: doesn't need to be "stopped"; pause is fine.
      }
    }

    @Override
    protected Set<HighlightFlag> getFlags(String field) {
      Set<HighlightFlag> flags = EnumSet.noneOf(HighlightFlag.class);
      if (params.getFieldBool(field, HighlightParams.HIGHLIGHT_MULTI_TERM, true)) {
        flags.add(HighlightFlag.MULTI_TERM_QUERY);
      }
      if (params.getFieldBool(field, HighlightParams.USE_PHRASE_HIGHLIGHTER, true)) {
        flags.add(HighlightFlag.PHRASES);
      }
      flags.add(HighlightFlag.PASSAGE_RELEVANCY_OVER_SPEED);

      if (params.getFieldBool(field, HighlightParams.WEIGHT_MATCHES, true)
          && flags.contains(HighlightFlag.PHRASES) && flags.contains(HighlightFlag.MULTI_TERM_QUERY)) {
        flags.add(HighlightFlag.WEIGHT_MATCHES);
      }
      return flags;
    }

    @Override
    protected Predicate<String> getFieldMatcher(String field) {
      // TODO define hl.queryFieldPattern as a more advanced alternative to hl.requireFieldMatch.

      // note that the UH at Lucene level default to effectively "true"
      if (params.getFieldBool(field, HighlightParams.FIELD_MATCH, false)) {
        return field::equals; // requireFieldMatch
      } else {
        return NOT_REQUIRED_FIELD_MATCH_PREDICATE;
      }
    }
  }

}