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
package org.apache.lucene.search.matchhighlight;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;

/**
 * Utility class to compute a list of "match regions" for a given query, searcher and document(s)
 * using {@link Matches} API.
 */
public class MatchRegionRetriever {
  private final List<LeafReaderContext> leaves;
  private final Weight weight;
  private final TreeSet<String> affectedFields;
  private final Map<String, OffsetsRetrievalStrategy> offsetStrategies;
  private final Set<String> preloadFields;

  /**
   * A callback for accepting a single document (and its associated leaf reader, leaf document ID)
   * and its match offset ranges, as indicated by the {@link Matches} interface retrieved for the
   * query.
   */
  @FunctionalInterface
  public interface MatchOffsetsConsumer {
    void accept(
        int docId, LeafReader leafReader, int leafDocId, Map<String, List<OffsetRange>> hits)
        throws IOException;
  }

  /**
   * An abstraction that provides document values for a given field. Default implementation in
   * {@link DocumentFieldValueProvider} just reaches to a preloaded {@link Document}. It is possible
   * to write a more efficient implementation on top of a reusable character buffer (that reuses the
   * buffer while retrieving hit regions for documents).
   */
  @FunctionalInterface
  public interface FieldValueProvider {
    List<CharSequence> getValues(String field);
  }

  /**
   * A constructor with the default offset strategy supplier.
   *
   * @param analyzer An analyzer that may be used to reprocess (retokenize) document fields in the
   *     absence of position offsets in the index. Note that the analyzer must return tokens
   *     (positions and offsets) identical to the ones stored in the index.
   */
  public MatchRegionRetriever(IndexSearcher searcher, Query query, Analyzer analyzer)
      throws IOException {
    this(searcher, query, computeOffsetRetrievalStrategies(searcher.getIndexReader(), analyzer));
  }

  /**
   * @param searcher Index searcher to be used for retrieving matches.
   * @param query The query for which matches should be retrieved. The query should be rewritten
   *     against the provided searcher.
   * @param fieldOffsetStrategySupplier A custom supplier of per-field {@link
   *     OffsetsRetrievalStrategy} instances.
   */
  public MatchRegionRetriever(
      IndexSearcher searcher,
      Query query,
      OffsetsRetrievalStrategySupplier fieldOffsetStrategySupplier)
      throws IOException {
    leaves = searcher.getIndexReader().leaves();
    assert checkOrderConsistency(leaves);

    // We need full scoring mode so that we can receive matches from all sub-clauses
    // (no optimizations in Boolean queries take place).
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 0);

    // Compute the subset of fields affected by this query so that we don't load or scan
    // fields that are irrelevant.
    affectedFields = new TreeSet<>();
    query.visit(
        new QueryVisitor() {
          @Override
          public boolean acceptField(String field) {
            affectedFields.add(field);
            return false;
          }
        });

    // Compute value offset retrieval strategy for all affected fields.
    offsetStrategies = new HashMap<>();
    for (String field : affectedFields) {
      offsetStrategies.put(field, fieldOffsetStrategySupplier.apply(field));
    }

    // Ask offset strategies if they'll need field values.
    preloadFields = new HashSet<>();
    offsetStrategies.forEach(
        (field, strategy) -> {
          if (strategy.requiresDocument()) {
            preloadFields.add(field);
          }
        });

    // Only preload those field values that can be affected by the query and are required
    // by strategies.
    preloadFields.retainAll(affectedFields);
  }

  public void highlightDocuments(TopDocs topDocs, MatchOffsetsConsumer consumer)
      throws IOException {
    highlightDocuments(
        Arrays.stream(topDocs.scoreDocs).mapToInt(scoreDoc -> scoreDoc.doc).sorted().iterator(),
        consumer);
  }

  /**
   * Low-level, high-efficiency method for highlighting large numbers of documents at once in a
   * streaming fashion.
   *
   * @param docIds A stream of <em>sorted</em> document identifiers for which hit ranges should be
   *     returned.
   * @param consumer A streaming consumer for document-hits pairs.
   */
  public void highlightDocuments(PrimitiveIterator.OfInt docIds, MatchOffsetsConsumer consumer)
      throws IOException {
    if (leaves.isEmpty()) {
      return;
    }

    Iterator<LeafReaderContext> ctx = leaves.iterator();
    LeafReaderContext currentContext = ctx.next();
    int previousDocId = -1;
    Map<String, List<OffsetRange>> highlights = new TreeMap<>();
    while (docIds.hasNext()) {
      int docId = docIds.nextInt();

      if (docId < previousDocId) {
        throw new RuntimeException("Input document IDs must be sorted (increasing).");
      }
      previousDocId = docId;

      while (docId >= currentContext.docBase + currentContext.reader().maxDoc()) {
        currentContext = ctx.next();
      }

      int contextRelativeDocId = docId - currentContext.docBase;

      // Only preload fields we may potentially need.
      FieldValueProvider documentSupplier;
      if (preloadFields.isEmpty()) {
        documentSupplier = null;
      } else {
        Document doc = currentContext.reader().document(contextRelativeDocId, preloadFields);
        documentSupplier = new DocumentFieldValueProvider(doc);
      }

      highlights.clear();
      highlightDocument(
          currentContext, contextRelativeDocId, documentSupplier, (field) -> true, highlights);
      consumer.accept(docId, currentContext.reader(), contextRelativeDocId, highlights);
    }
  }

  /**
   * Low-level method for retrieving hit ranges for a single document. This method can be used with
   * custom document {@link FieldValueProvider}.
   */
  public void highlightDocument(
      LeafReaderContext leafReaderContext,
      int contextDocId,
      FieldValueProvider doc,
      Predicate<String> acceptField,
      Map<String, List<OffsetRange>> outputHighlights)
      throws IOException {
    Matches matches = weight.matches(leafReaderContext, contextDocId);
    if (matches == null) {
      return;
    }

    for (String field : affectedFields) {
      if (acceptField.test(field)) {
        MatchesIterator matchesIterator = matches.getMatches(field);
        if (matchesIterator == null) {
          // No matches on this field, even though the field was part of the query. This may be
          // possible
          // with complex queries that source non-text fields (have no "hit regions" in any textual
          // representation). Skip.
        } else {
          OffsetsRetrievalStrategy offsetStrategy = offsetStrategies.get(field);
          if (offsetStrategy == null) {
            throw new IOException(
                "Non-empty matches but no offset retrieval strategy for field: " + field);
          }
          List<OffsetRange> ranges = offsetStrategy.get(matchesIterator, doc);
          if (!ranges.isEmpty()) {
            outputHighlights.put(field, ranges);
          }
        }
      }
    }
  }

  private boolean checkOrderConsistency(List<LeafReaderContext> leaves) {
    for (int i = 1; i < leaves.size(); i++) {
      LeafReaderContext prev = leaves.get(i - 1);
      LeafReaderContext next = leaves.get(i);
      assert prev.docBase <= next.docBase;
      assert prev.docBase + prev.reader().maxDoc() == next.docBase;
    }
    return true;
  }

  /**
   * Compute default strategies for retrieving offsets from {@link MatchesIterator} instances for a
   * set of given fields.
   */
  public static OffsetsRetrievalStrategySupplier computeOffsetRetrievalStrategies(
      IndexReader reader, Analyzer analyzer) {
    FieldInfos fieldInfos = FieldInfos.getMergedFieldInfos(reader);
    return (field) -> {
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      if (fieldInfo == null) {
        return (mi, doc) -> {
          throw new IOException("FieldInfo is null for field: " + field);
        };
      }

      switch (fieldInfo.getIndexOptions()) {
        case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
          return new OffsetsFromMatchIterator(field);

        case DOCS_AND_FREQS_AND_POSITIONS:
          return new OffsetsFromPositions(field, analyzer);

        case DOCS_AND_FREQS:
        case DOCS:
          // By default retrieve offsets from individual tokens
          // retrieved by the analyzer (possibly narrowed down to
          // only those terms that the query hinted at when passed
          // a QueryVisitor.
          //
          // Alternative strategies are also possible and may make sense
          // depending on the use case (OffsetsFromValues, for example).
          return new OffsetsFromTokens(field, analyzer);

        default:
          return (matchesIterator, doc) -> {
            throw new IOException(
                "Field is indexed without positions and/or offsets: "
                    + field
                    + ", "
                    + fieldInfo.getIndexOptions());
          };
      }
    };
  }

  /** Implements {@link FieldValueProvider} wrapping a preloaded {@link Document}. */
  private static final class DocumentFieldValueProvider implements FieldValueProvider {
    private final Document doc;

    public DocumentFieldValueProvider(Document doc) {
      this.doc = doc;
    }

    @Override
    public List<CharSequence> getValues(String field) {
      return Arrays.asList(doc.getValues(field));
    }
  }
}
