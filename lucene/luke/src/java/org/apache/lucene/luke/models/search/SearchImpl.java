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

package org.apache.lucene.luke.models.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.LukeModel;
import org.apache.lucene.luke.models.util.IndexUtils;
import org.apache.lucene.luke.util.LoggerFactory;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.ArrayUtil;

/** Default implementation of {@link Search} */
public final class SearchImpl extends LukeModel implements Search {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int DEFAULT_PAGE_SIZE = 10;

  private static final int DEFAULT_TOTAL_HITS_THRESHOLD = 1000;

  private final IndexSearcher searcher;

  private int pageSize = DEFAULT_PAGE_SIZE;

  private int currentPage = -1;

  private TotalHits totalHits;

  private ScoreDoc[] docs = new ScoreDoc[0];

  private boolean exactHitsCount;

  private Query query;

  private Sort sort;

  private Set<String> fieldsToLoad;

  /**
   * Constructs a SearchImpl that holds given {@link IndexReader}
   * @param reader - the index reader
   */
  public SearchImpl(IndexReader reader) {
    super(reader);
    this.searcher = new IndexSearcher(reader);
  }

  @Override
  public Collection<String> getSortableFieldNames() {
    return IndexUtils.getFieldNames(reader).stream()
        .map(f -> IndexUtils.getFieldInfo(reader, f))
        .filter(info -> !info.getDocValuesType().equals(DocValuesType.NONE))
        .map(info -> info.name)
        .collect(Collectors.toList());
  }

  @Override
  public Collection<String> getSearchableFieldNames() {
    return IndexUtils.getFieldNames(reader).stream()
        .map(f -> IndexUtils.getFieldInfo(reader, f))
        .filter(info -> !info.getIndexOptions().equals(IndexOptions.NONE))
        .map(info -> info.name)
        .collect(Collectors.toList());
  }

  @Override
  public Collection<String> getRangeSearchableFieldNames() {
    return IndexUtils.getFieldNames(reader).stream()
        .map(f -> IndexUtils.getFieldInfo(reader, f))
        .filter(info -> info.getPointDimensionCount() > 0)
        .map(info -> info.name)
        .collect(Collectors.toSet());
  }

  @Override
  public Query getCurrentQuery() {
    return this.query;
  }

  @Override
  public Query parseQuery(String expression, String defField, Analyzer analyzer,
                          QueryParserConfig config, boolean rewrite) {
    Objects.requireNonNull(expression);
    Objects.requireNonNull(defField);
    Objects.requireNonNull(analyzer);
    Objects.requireNonNull(config);

    Query query = config.isUseClassicParser() ?
        parseByClassicParser(expression, defField, analyzer, config) :
        parseByStandardParser(expression, defField, analyzer, config);

    if (rewrite) {
      try {
        query = query.rewrite(reader);
      } catch (IOException e) {
        throw new LukeException(String.format(Locale.ENGLISH, "Failed to rewrite query: %s", query.toString()), e);
      }
    }

    return query;
  }

  private Query parseByClassicParser(String expression, String defField, Analyzer analyzer,
                                     QueryParserConfig config) {
    QueryParser parser = new QueryParser(defField, analyzer);

    switch (config.getDefaultOperator()) {
      case OR:
        parser.setDefaultOperator(QueryParser.Operator.OR);
        break;
      case AND:
        parser.setDefaultOperator(QueryParser.Operator.AND);
        break;
    }

    parser.setSplitOnWhitespace(config.isSplitOnWhitespace());
    parser.setAutoGenerateMultiTermSynonymsPhraseQuery(config.isAutoGenerateMultiTermSynonymsPhraseQuery());
    parser.setAutoGeneratePhraseQueries(config.isAutoGeneratePhraseQueries());
    parser.setEnablePositionIncrements(config.isEnablePositionIncrements());
    parser.setAllowLeadingWildcard(config.isAllowLeadingWildcard());
    parser.setDateResolution(config.getDateResolution());
    parser.setFuzzyMinSim(config.getFuzzyMinSim());
    parser.setFuzzyPrefixLength(config.getFuzzyPrefixLength());
    parser.setLocale(config.getLocale());
    parser.setTimeZone(config.getTimeZone());
    parser.setPhraseSlop(config.getPhraseSlop());

    try {
      return parser.parse(expression);
    } catch (ParseException e) {
      throw new LukeException(String.format(Locale.ENGLISH, "Failed to parse query expression: %s", expression), e);
    }

  }

  private Query parseByStandardParser(String expression, String defField, Analyzer analyzer,
                                      QueryParserConfig config) {
    StandardQueryParser parser = new StandardQueryParser(analyzer);

    switch (config.getDefaultOperator()) {
      case OR:
        parser.setDefaultOperator(StandardQueryConfigHandler.Operator.OR);
        break;
      case AND:
        parser.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);
        break;
    }

    parser.setEnablePositionIncrements(config.isEnablePositionIncrements());
    parser.setAllowLeadingWildcard(config.isAllowLeadingWildcard());
    parser.setDateResolution(config.getDateResolution());
    parser.setFuzzyMinSim(config.getFuzzyMinSim());
    parser.setFuzzyPrefixLength(config.getFuzzyPrefixLength());
    parser.setLocale(config.getLocale());
    parser.setTimeZone(config.getTimeZone());
    parser.setPhraseSlop(config.getPhraseSlop());

    if (config.getTypeMap() != null) {
      Map<String, PointsConfig> pointsConfigMap = new HashMap<>();

      for (Map.Entry<String, Class<? extends Number>> entry : config.getTypeMap().entrySet()) {
        String field = entry.getKey();
        Class<? extends Number> type = entry.getValue();
        PointsConfig pc;
        if (type == Integer.class || type == Long.class) {
          pc = new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), type);
        } else if (type == Float.class || type == Double.class) {
          pc = new PointsConfig(NumberFormat.getNumberInstance(Locale.ROOT), type);
        } else {
          log.warn(String.format(Locale.ENGLISH, "Ignored invalid number type: %s.", type.getName()));
          continue;
        }
        pointsConfigMap.put(field, pc);
      }

      parser.setPointsConfigMap(pointsConfigMap);
    }

    try {
      return parser.parse(expression, defField);
    } catch (QueryNodeException e) {
      throw new LukeException(String.format(Locale.ENGLISH, "Failed to parse query expression: %s", expression), e);
    }

  }

  @Override
  public Query mltQuery(int docid, MLTConfig mltConfig, Analyzer analyzer) {
    MoreLikeThis mlt = new MoreLikeThis(reader);

    mlt.setAnalyzer(analyzer);
    mlt.setFieldNames(mltConfig.getFieldNames());
    mlt.setMinDocFreq(mltConfig.getMinDocFreq());
    mlt.setMaxDocFreq(mltConfig.getMaxDocFreq());
    mlt.setMinTermFreq(mltConfig.getMinTermFreq());

    try {
      return mlt.like(docid);
    } catch (IOException e) {
      throw new LukeException("Failed to create MLT query for doc: " + docid);
    }
  }

  @Override
  public SearchResults search(
      Query query, SimilarityConfig simConfig, Set<String> fieldsToLoad, int pageSize, boolean exactHitsCount) {
    return search(query, simConfig, null, fieldsToLoad, pageSize, exactHitsCount);
  }

  @Override
  public SearchResults search(
      Query query, SimilarityConfig simConfig, Sort sort, Set<String> fieldsToLoad, int pageSize, boolean exactHitsCount) {
    if (pageSize < 0) {
      throw new LukeException(new IllegalArgumentException("Negative integer is not acceptable for page size."));
    }

    // reset internal status to prepare for a new search session
    this.docs = new ScoreDoc[0];
    this.currentPage = 0;
    this.pageSize = pageSize;
    this.exactHitsCount = exactHitsCount;
    this.query = Objects.requireNonNull(query);
    this.sort = sort;
    this.fieldsToLoad = fieldsToLoad == null ? null : Collections.unmodifiableSet(fieldsToLoad);
    searcher.setSimilarity(createSimilarity(Objects.requireNonNull(simConfig)));

    try {
      return search();
    } catch (IOException e) {
      throw new LukeException("Search Failed.", e);
    }
  }

  private SearchResults search() throws IOException {
    // execute search
    ScoreDoc after = docs.length == 0 ? null : docs[docs.length - 1];

    TopDocs topDocs;
    if (sort != null) {
      topDocs = searcher.searchAfter(after, query, pageSize, sort);
    } else {
      int hitsThreshold = exactHitsCount ? Integer.MAX_VALUE : DEFAULT_TOTAL_HITS_THRESHOLD;
      TopScoreDocCollector collector = TopScoreDocCollector.create(pageSize, after, hitsThreshold);
      searcher.search(query, collector);
      topDocs = collector.topDocs();
    }

    // reset total hits for the current query
    this.totalHits = topDocs.totalHits;

    // cache search results for later use
    ScoreDoc[] newDocs = new ScoreDoc[docs.length + topDocs.scoreDocs.length];
    System.arraycopy(docs, 0, newDocs, 0, docs.length);
    System.arraycopy(topDocs.scoreDocs, 0, newDocs, docs.length, topDocs.scoreDocs.length);
    this.docs = newDocs;

    return SearchResults.of(topDocs.totalHits, topDocs.scoreDocs, currentPage * pageSize, searcher, fieldsToLoad);
  }

  @Override
  public Optional<SearchResults> nextPage() {
    if (currentPage < 0 || query == null) {
      throw new LukeException(new IllegalStateException("Search session not started."));
    }

    // proceed to next page
    currentPage += 1;

    if (totalHits.value == 0 ||
        (totalHits.relation == TotalHits.Relation.EQUAL_TO && currentPage * pageSize >= totalHits.value)) {
      log.warn("No more next search results are available.");
      return Optional.empty();
    }

    try {

      if (currentPage * pageSize < docs.length) {
        // if cached results exist, return that.
        int from = currentPage * pageSize;
        int to = Math.min(from + pageSize, docs.length);
        ScoreDoc[] part = ArrayUtil.copyOfSubArray(docs, from, to);
        return Optional.of(SearchResults.of(totalHits, part, from, searcher, fieldsToLoad));
      } else {
        return Optional.of(search());
      }

    } catch (IOException e) {
      throw new LukeException("Search Failed.", e);
    }
  }


  @Override
  public Optional<SearchResults> prevPage() {
    if (currentPage < 0 || query == null) {
      throw new LukeException(new IllegalStateException("Search session not started."));
    }

    // return to previous page
    currentPage -= 1;

    if (currentPage < 0) {
      log.warn("No more previous search results are available.");
      return Optional.empty();
    }

    try {
      // there should be cached results for this page
      int from = currentPage * pageSize;
      int to = Math.min(from + pageSize, docs.length);
      ScoreDoc[] part = ArrayUtil.copyOfSubArray(docs, from, to);
      return Optional.of(SearchResults.of(totalHits, part, from, searcher, fieldsToLoad));
    } catch (IOException e) {
      throw new LukeException("Search Failed.", e);
    }
  }

  private Similarity createSimilarity(SimilarityConfig config) {
    Similarity similarity;

    if (config.isUseClassicSimilarity()) {
      ClassicSimilarity tfidf = new ClassicSimilarity();
      tfidf.setDiscountOverlaps(config.isDiscountOverlaps());
      similarity = tfidf;
    } else {
      BM25Similarity bm25 = new BM25Similarity(config.getK1(), config.getB());
      bm25.setDiscountOverlaps(config.isDiscountOverlaps());
      similarity = bm25;
    }

    return similarity;
  }

  @Override
  public List<SortField> guessSortTypes(String name) {
    FieldInfo finfo = IndexUtils.getFieldInfo(reader, name);
    if (finfo == null) {
      throw new LukeException("No such field: " + name, new IllegalArgumentException());
    }

    DocValuesType dvType = finfo.getDocValuesType();

    switch (dvType) {
      case NONE:
        return Collections.emptyList();

      case NUMERIC:
        return Arrays.stream(new SortField[]{
            new SortField(name, SortField.Type.INT),
            new SortField(name, SortField.Type.LONG),
            new SortField(name, SortField.Type.FLOAT),
            new SortField(name, SortField.Type.DOUBLE)
        }).collect(Collectors.toList());

      case SORTED_NUMERIC:
        return Arrays.stream(new SortField[]{
            new SortedNumericSortField(name, SortField.Type.INT),
            new SortedNumericSortField(name, SortField.Type.LONG),
            new SortedNumericSortField(name, SortField.Type.FLOAT),
            new SortedNumericSortField(name, SortField.Type.DOUBLE)
        }).collect(Collectors.toList());

      case SORTED:
        return Arrays.stream(new SortField[] {
            new SortField(name, SortField.Type.STRING),
            new SortField(name, SortField.Type.STRING_VAL)
        }).collect(Collectors.toList());

      case SORTED_SET:
        return Collections.singletonList(new SortedSetSortField(name, false));

      default:
        return Collections.singletonList(new SortField(name, SortField.Type.DOC));
    }

  }

  @Override
  public Optional<SortField> getSortType(String name, String type, boolean reverse) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(type);
    List<SortField> candidates = guessSortTypes(name);
    if (candidates.isEmpty()) {
      log.warn(String.format(Locale.ENGLISH, "No available sort types for: %s", name));
      return Optional.empty();
    }

    // TODO should be refactored...
    for (SortField sf : candidates) {
      if (sf instanceof SortedSetSortField) {
        return Optional.of(new SortedSetSortField(sf.getField(), reverse));
      } else if (sf instanceof SortedNumericSortField) {
        SortField.Type sfType = ((SortedNumericSortField) sf).getNumericType();
        if (sfType.name().equals(type)) {
          return Optional.of(new SortedNumericSortField(sf.getField(), sfType, reverse));
        }
      } else {
        SortField.Type sfType = sf.getType();
        if (sfType.name().equals(type)) {
          return Optional.of(new SortField(sf.getField(), sfType, reverse));
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public Explanation explain(Query query, int docid) {
    try {
      return searcher.explain(query, docid);
    } catch (IOException e) {
      throw new LukeException(String.format(Locale.ENGLISH, "Failed to create explanation for doc: %d for query: \"%s\"", docid, query.toString()), e);
    }
  }
}
