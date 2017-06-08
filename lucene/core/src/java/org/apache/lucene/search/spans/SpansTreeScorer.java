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
package org.apache.lucene.search.spans;

import java.io.IOException;
import java.util.Objects;
import java.util.HashMap;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.TwoPhaseIterator;

/**
 * A Scorer for (nested) spans.
 * This associates the spans with a {@link SpansDocScorer} and uses its score values.
 * <p>
 * For {@link SpansTreeQuery}. Public for extension.
 *
 * @lucene.experimental
 */
public class SpansTreeScorer extends Scorer {

  protected final Spans spans;
  protected final double topLevelSlopFactor;
  protected final double nonMatchWeight;
  protected final HashMap<SpanQuery,SpansDocScorer<?>> spansDocScorerByQuery;
  protected final SpansDocScorer<?> spansDocScorer;

  protected int lastScoredDoc = -1;

  public SpansTreeScorer(Weight weight, Spans spans, double topLevelSlopFactor, double nonMatchWeight) {
    super(weight);
    this.spans = Objects.requireNonNull(spans);
    this.topLevelSlopFactor = topLevelSlopFactor;
    this.nonMatchWeight = nonMatchWeight;
    this.spansDocScorerByQuery = new HashMap<>();
    this.spansDocScorer = createSpansDocScorer(spans);
  }

  @Override
  public int docID() {
    return spans.docID();
  }

  @Override
  public DocIdSetIterator iterator() {
    return spans;
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return spans.asTwoPhaseIterator();
  }

  /**
   * Provide the SpansDocScorer that will be used by {@link #score} and {@link #freq}.
   * <br>
   * Override this to provide support for span queries for which the spans are not supported here.
   * <table rules="all" frame="box" cellpadding="3" summary="SpansDocScorer for Spans">
   * <tr><td>For {@link Spans}:</td>
   *     <td>normally from {@link SpanQuery}:</td>
   *     <td>return:</td>
   * </tr>
   * <tr><td>{@link TermSpans}</td>
   *     <td>{@link SpanTermQuery}</td>
   *     <td>{@link TermSpansDocScorer}</td>
   * </tr>
   * <tr><td>{@link DisjunctionNearSpans}</td>
   *     <td>{@link SpanOrQuery#SpanOrQuery(int,SpanQuery...)}</td>
   *     <td>{@link DisjunctionNearSpansDocScorer}</td>
   * </tr>
   * <tr><td>{@link DisjunctionSpans}</td>
   *     <td>{@link SpanOrQuery#SpanOrQuery(SpanQuery...)}</td>
   *     <td>{@link DisjunctionSpansDocScorer}</td>
   * </tr>
   * <tr><td>{@link SynonymSpans}</td>
   *     <td>{@link SpanSynonymQuery#SpanSynonymQuery(Term...)}</td>
   *     <td>{@link SynonymSpansDocScorer}</td>
   * </tr>
   * <tr><td>{@link ConjunctionNearSpans}</td>
   *     <td>{@link SpanNearQuery}</td>
   *     <td>{@link ConjunctionNearSpansDocScorer}</td>
   * </tr>
   * <tr><td>{@link FilterSpans}</td>
   *     <td>{@link SpanNotQuery}, {@link SpanFirstQuery}</td>
   *     <td>recursively use {@link FilterSpans#in}</td>
   * </tr>
   * <tr><td>{@link ContainSpans}</td>
   *     <td>{@link SpanContainingQuery}, {@link SpanWithinQuery}</td>
   *     <td>recursively use {@link ContainSpans#sourceSpans}</td>
   * </tr>
   * </table>
   * For a term that is present more than once via TermSpans a single SpansDocScorer will be provided.
   * For a synonym set of terms that is present more than once via SynonymSpans a single SpansDocScorer will be provided.
   */
  public SpansDocScorer<?> createSpansDocScorer(Spans spans) {
    SpansDocScorer<?> spansDocScorer = null;
    if (spans instanceof TermSpans) {
      TermSpans termSpans = (TermSpans) spans;
      SpanQuery stq = new SpanTermQuery(termSpans.term);
      spansDocScorer = spansDocScorerByQuery.get(stq);
      if (spansDocScorer == null) {
        spansDocScorer = new TermSpansDocScorer(termSpans, nonMatchWeight);
        spansDocScorerByQuery.put(stq, spansDocScorer);
      }
    }
    else if (spans instanceof DisjunctionNearSpans) {
      spansDocScorer = new DisjunctionNearSpansDocScorer(this, (DisjunctionNearSpans) spans);
    }
    else if (spans instanceof SynonymSpans) {
      SynonymSpans synSpans = (SynonymSpans) spans;
      SpanQuery ssq = synSpans.getSpanQuery();
      spansDocScorer = spansDocScorerByQuery.get(ssq);
      if (spansDocScorer == null) {
        spansDocScorer = new SynonymSpansDocScorer(synSpans, nonMatchWeight);
        spansDocScorerByQuery.put(ssq, spansDocScorer);
      }
    }
    else if (spans instanceof DisjunctionSpans) {
      spansDocScorer = new DisjunctionSpansDocScorer<>(this, (DisjunctionSpans) spans);
    }
    else if (spans instanceof ConjunctionNearSpans) {
      spansDocScorer =  new ConjunctionNearSpansDocScorer(this, (ConjunctionNearSpans) spans);
    }
    else if (spans instanceof FilterSpans) {
      spansDocScorer = createSpansDocScorer(((FilterSpans) spans).in);
    }
    else if (spans instanceof ContainSpans) {
      spansDocScorer = createSpansDocScorer(((ContainSpans) spans).sourceSpans);
    }
    if (spansDocScorer == null) {
      throw new IllegalArgumentException("Not implemented for Spans class: "
                                          + spans.getClass().getName());
    }
    spans.spansDocScorer = spansDocScorer;
    return spansDocScorer;
  }

  /**
   * Record the span matches in the current document.
   * <p>
   * This will be called at most once per document.
   */
  protected void recordMatchesCurrentDoc() throws IOException {
    int startPos = spans.nextStartPosition();
    assert startPos != Spans.NO_MORE_POSITIONS;
    spansDocScorer.beginDoc(spans.docID());
    do {
      spansDocScorer.recordMatch(topLevelSlopFactor, spans.startPosition());
      startPos = spans.nextStartPosition();
    } while (startPos != Spans.NO_MORE_POSITIONS);
  }

  /**
   * Ensure recordMatchesCurrentDoc is called, if not already called for the current doc.
   */
  public void ensureMatchesRecorded() throws IOException {
    int currentDoc = docID();
    if (lastScoredDoc != currentDoc) {
      recordMatchesCurrentDoc();
      lastScoredDoc = currentDoc;
    }
  }

  /** Score the current document.
   * See {@link #createSpansDocScorer} and {@link SpansDocScorer#docScore}.
   */
  @Override
  public final float score() throws IOException {
    ensureMatchesRecorded();
    return (float) spansDocScorer.docScore();
  }

  /** Return the total matching frequency of the current document.
   * See {@link #createSpansDocScorer} and {@link SpansDocScorer#docMatchFreq}.
   */
  @Override
  public final int freq() throws IOException {
    ensureMatchesRecorded();
    return spansDocScorer.docMatchFreq();
  }

  public String toString() {
    return "SpansTreeScorer(" + spansDocScorer + ")";
  }
}
