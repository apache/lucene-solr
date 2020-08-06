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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
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
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
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

/**
 * Utility class to compute a list of "hit regions" for a given query, searcher and
 * document(s) using {@link Matches} API.
 */
public class MatchRegionRetriever {
  private final List<LeafReaderContext> leaves;
  private final Weight weight;
  private final TreeSet<String> affectedFields;
  private final Map<String, OffsetsFromMatchesStrategy> offsetStrategies;
  private final Set<String> preloadFields;

  public MatchRegionRetriever(IndexSearcher searcher, Query query, Analyzer analyzer)
      throws IOException {
    leaves = searcher.getIndexReader().leaves();
    assert checkOrderConsistency(leaves);

    weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0);

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
    offsetStrategies =
        computeOffsetStrategies(affectedFields, searcher.getIndexReader(), analyzer);

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

  public void highlightDocuments(PrimitiveIterator.OfInt docIds, HitRegionConsumer consumer)
      throws IOException {
    if (leaves.isEmpty() || affectedFields.isEmpty()) {
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

      highlightDocument(
          currentContext, contextRelativeDocId, documentSupplier, highlights, (field) -> true);

      consumer.accept(currentContext.reader(), contextRelativeDocId, highlights);
      highlights.clear();
    }
  }

  public void highlightDocument(
      LeafReaderContext currentContext,
      int contextDocId,
      FieldValueProvider doc,
      Map<String, List<OffsetRange>> highlights,
      Predicate<String> acceptField)
      throws IOException {
    Matches matches = weight.matches(currentContext, contextDocId);
    if (matches == null) {
      return;
    }

    for (String field : affectedFields) {
      if (acceptField.test(field)) {
        MatchesIterator matchesIterator = matches.getMatches(field);
        if (matchesIterator == null) {
          // No matches on this field, even though the field was part of the query. This may be possible
          // with complex queries that source non-text fields (have no "hit regions" in any textual
          // representation).
        } else {
          OffsetsFromMatchesStrategy offsetStrategy = offsetStrategies.get(field);
          if (offsetStrategy == null) {
            throw new IOException(
                "Non-empty matches but no offset retrieval strategy for field: " + field);
          }
          List<OffsetRange> ranges = offsetStrategy.get(matchesIterator, doc);
          if (!ranges.isEmpty()) {
            highlights.put(field, ranges);
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

  private static Map<String, OffsetsFromMatchesStrategy> computeOffsetStrategies(
      Set<String> affectedFields, IndexReader reader, Analyzer analyzer) {
    Map<String, OffsetsFromMatchesStrategy> offsetStrategies = new HashMap<>();
    FieldInfos fieldInfos = FieldInfos.getMergedFieldInfos(reader);
    for (String field : affectedFields) {
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);

      OffsetsFromMatchesStrategy offsetStrategy;
      if (fieldInfo != null && fieldInfo.getIndexOptions() != null) {
        switch (fieldInfo.getIndexOptions()) {
          case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
            offsetStrategy = new OffsetsFromMatchIterator(field);
            break;

          case DOCS_AND_FREQS_AND_POSITIONS:
            offsetStrategy = new OffsetsFromPositions(field, analyzer);
            break;


          case DOCS_AND_FREQS:
            // offsetStrategy = new OffsetsFromTokens(field, analyzer);
            offsetStrategy = new OffsetsFromValues(field, analyzer);
            break;

          default:
            offsetStrategy =
                (matchesIterator, doc) -> {
                  throw new IOException(
                      "Field is indexed without positions and/or offsets: "
                          + field
                          + ", "
                          + fieldInfo.getIndexOptions());
                };
        }
        offsetStrategies.put(field, offsetStrategy);
      }
    }
    return offsetStrategies;
  }

  public interface HitRegionConsumer {
    void accept(LeafReader leafReader, int leafDocId, Map<String, List<OffsetRange>> hits)
        throws IOException;
  }

  /**
   * An abstraction that provides document values for a given field. Default implementation
   * in {@link DocumentFieldValueProvider} just reaches to a preloaded {@link Document}. It is
   * possible to write a more efficient implementation on top of a reusable character buffer
   * (that reuses the buffer while retrieving hit regions for documents).
   */
  public interface FieldValueProvider {
    List<CharSequence> getValues(String field);
  }

  public static final class DocumentFieldValueProvider implements FieldValueProvider {
    private final Document doc;

    public DocumentFieldValueProvider(Document doc) {
      this.doc = doc;
    }

    @Override
    public List<CharSequence> getValues(String field) {
      return Arrays.asList(doc.getValues(field));
    }
  }

  /**
   * Determines how match offset regions are computed from {@link MatchesIterator}. Several
   * possibilities exist, ranging from retrieving offsets directly from a match instance
   * to re-evaluating the document's field and recomputing offsets from there.
   */
  private interface OffsetsFromMatchesStrategy {
    List<OffsetRange> get(MatchesIterator matchesIterator, FieldValueProvider doc)
        throws IOException;

    default boolean requiresDocument() {
      return false;
    }
  }

  /**
   * This strategy retrieves offsets directly from {@link MatchesIterator}.
   */
  private static class OffsetsFromMatchIterator implements OffsetsFromMatchesStrategy {
    private final String field;

    OffsetsFromMatchIterator(String field) {
      this.field = field;
    }

    @Override
    public List<OffsetRange> get(MatchesIterator matchesIterator, FieldValueProvider doc)
        throws IOException {
      ArrayList<OffsetRange> ranges = new ArrayList<>();
      while (matchesIterator.next()) {
        int from = matchesIterator.startOffset();
        int to = matchesIterator.endOffset();
        if (from < 0 || to < 0) {
          throw new IOException("Matches API returned negative offsets for field: " + field);
        }
        ranges.add(new OffsetRange(from, to));
      }
      return ranges;
    }
  }

  /**
   * This strategy works for fields where we know the match occurred but there are
   * no known positions or offsets.
   *
   * We re-analyze field values and return offset ranges for entire values
   * (not individual tokens). Re-analysis is required because analyzer may return
   * an unknown offset gap.
   */
  private static class OffsetsFromValues implements OffsetsFromMatchesStrategy {
    private final String field;
    private final Analyzer analyzer;

    public OffsetsFromValues(String field, Analyzer analyzer) {
      this.field = field;
      this.analyzer = analyzer;
    }

    @Override
    public List<OffsetRange> get(MatchesIterator matchesIterator, FieldValueProvider doc) throws IOException {
      List<CharSequence> values = doc.getValues(field);

      ArrayList<OffsetRange> ranges = new ArrayList<>();
      int valueOffset = 0;
      for (CharSequence charSequence : values) {
        final String value = charSequence.toString();

        TokenStream ts = analyzer.tokenStream(field, value);
        OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
        ts.reset();
        int startOffset = valueOffset;
        while (ts.incrementToken()) {
          // Go through all tokens to increment offset attribute properly.
        }
        ts.end();
        valueOffset += offsetAttr.endOffset();
        ranges.add(new OffsetRange(startOffset, valueOffset));
        valueOffset += analyzer.getOffsetGap(field);
        ts.close();
      }
      return ranges;
    }

    @Override
    public boolean requiresDocument() {
      return true;
    }
  }

  /**
   * This strategy works for fields where we know the match occurred but there are
   * no known positions or offsets.
   *
   * We re-analyze field values and return offset ranges for all returned tokens.
   */
  private static class OffsetsFromTokens implements OffsetsFromMatchesStrategy {
    private final String field;
    private final Analyzer analyzer;

    public OffsetsFromTokens(String field, Analyzer analyzer) {
      this.field = field;
      this.analyzer = analyzer;
    }

    @Override
    public List<OffsetRange> get(MatchesIterator matchesIterator, FieldValueProvider doc) throws IOException {
      List<CharSequence> values = doc.getValues(field);

      ArrayList<OffsetRange> ranges = new ArrayList<>();
      int valueOffset = 0;
      for (int valueIndex = 0, max = values.size(); valueIndex < max; valueIndex++) {
        final String value = values.get(valueIndex).toString();

        TokenStream ts = analyzer.tokenStream(field, value);
        OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
          int startOffset = valueOffset + offsetAttr.startOffset();
          int endOffset = valueOffset + offsetAttr.endOffset();
          ranges.add(new OffsetRange(startOffset, endOffset));
        }
        ts.end();
        valueOffset += offsetAttr.endOffset() + analyzer.getOffsetGap(field);
        ts.close();
      }
      return ranges;
    }

    @Override
    public boolean requiresDocument() {
      return true;
    }
  }

  /**
   * This strategy applies to fields with stored positions but no offsets. We re-analyze
   * the field's value to find out offsets of match positions.
   *
   * Note that this may fail if index data (positions stored in the index) is out of sync
   * with the field values or the analyzer. This strategy assumes it'll never happen.
   */
  private static class OffsetsFromPositions implements OffsetsFromMatchesStrategy {
    private final String field;
    private final Analyzer analyzer;

    OffsetsFromPositions(String field, Analyzer analyzer) {
      this.field = field;
      this.analyzer = analyzer;
    }

    @Override
    public List<OffsetRange> get(MatchesIterator matchesIterator, FieldValueProvider doc)
        throws IOException {
      ArrayList<OffsetRange> ranges = new ArrayList<>();
      while (matchesIterator.next()) {
        int from = matchesIterator.startPosition();
        int to = matchesIterator.endPosition();
        if (from < 0 || to < 0) {
          throw new IOException("Matches API returned negative positions for field: " + field);
        }
        ranges.add(new OffsetRange(from, to));
      }

      // Convert from positions to offsets.
      ranges = convertPositionsToOffsets(ranges, analyzer, field, doc.getValues(field));

      return ranges;
    }

    @Override
    public boolean requiresDocument() {
      return true;
    }

    private static ArrayList<OffsetRange> convertPositionsToOffsets(
        ArrayList<OffsetRange> ranges,
        Analyzer analyzer,
        String fieldName,
        List<CharSequence> values)
        throws IOException {

      if (ranges.isEmpty()) {
        return ranges;
      }

      class LeftRight {
        int left = Integer.MAX_VALUE;
        int right = Integer.MIN_VALUE;
      }

      Map<Integer, LeftRight> requiredPositionSpans = new HashMap<>();
      int minPosition = Integer.MAX_VALUE;
      int maxPosition = Integer.MIN_VALUE;
      for (OffsetRange range : ranges) {
        requiredPositionSpans.computeIfAbsent(range.from, (key) -> new LeftRight());
        requiredPositionSpans.computeIfAbsent(range.to, (key) -> new LeftRight());
        minPosition = Math.min(minPosition, range.from);
        maxPosition = Math.max(maxPosition, range.to);
      }

      int position = -1;
      int valueOffset = 0;
      for (int valueIndex = 0, max = values.size(); valueIndex < max; valueIndex++) {
        final String value = values.get(valueIndex).toString();
        final boolean lastValue = valueIndex + 1 == max;

        TokenStream ts = analyzer.tokenStream(fieldName, value);
        OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
        PositionIncrementAttribute posAttr = ts.getAttribute(PositionIncrementAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
          position += posAttr.getPositionIncrement();

          if (position >= minPosition) {
            LeftRight leftRight = requiredPositionSpans.get(position);
            if (leftRight != null) {
              int startOffset = valueOffset + offsetAttr.startOffset();
              int endOffset = valueOffset + offsetAttr.endOffset();

              leftRight.left = Math.min(leftRight.left, startOffset);
              leftRight.right = Math.max(leftRight.right, endOffset);
            }

            // Only short-circuit if we're on the last value (which should be the common
            // case since most fields would only have a single value anyway). We need
            // to make sure of this because otherwise offsetAttr would have incorrect value.
            if (position > maxPosition && lastValue) {
              break;
            }
          }
        }
        ts.end();
        valueOffset += offsetAttr.endOffset() + analyzer.getOffsetGap(fieldName);
        ts.close();
      }

      ArrayList<OffsetRange> converted = new ArrayList<>();
      for (OffsetRange range : ranges) {
        LeftRight left = requiredPositionSpans.get(range.from);
        LeftRight right = requiredPositionSpans.get(range.to);
        if (left == null
            || right == null
            || left.left == Integer.MAX_VALUE
            || right.right == Integer.MIN_VALUE) {
          throw new RuntimeException("Position not properly initialized for range: " + range);
        }
        converted.add(new OffsetRange(left.left, right.right));
      }

      return converted;
    }
  }
}
