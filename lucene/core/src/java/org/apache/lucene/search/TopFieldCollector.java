package org.apache.lucene.search;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.util.PriorityQueue;

/**
 * A {@link Collector} that sorts by {@link SortField} using
 * {@link FieldComparator}s.
 * <p/>
 * See the {@link #create(org.apache.lucene.search.Sort, int, boolean, boolean, boolean, boolean)} method
 * for instantiating a TopFieldCollector.
 * 
 * @lucene.experimental
 */
public abstract class TopFieldCollector extends TopDocsCollector<Entry> {
  
  // TODO: one optimization we could do is to pre-fill
  // the queue with sentinel value that guaranteed to
  // always compare lower than a real hit; this would
  // save having to check queueFull on each insert

  /*
   * Implements a TopFieldCollector over one SortField criteria, without
   * tracking document scores and maxScore.
   */
  private static class OneComparatorNonScoringCollector extends 
      TopFieldCollector {

    FieldComparator<?> comparator;
    final int reverseMul;
    final FieldValueHitQueue<Entry> queue;
    
    public OneComparatorNonScoringCollector(FieldValueHitQueue<Entry> queue,
        int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
      this.queue = queue;
      comparator = queue.getComparators()[0];
      reverseMul = queue.getReverseMul()[0];
    }
    
    final void updateBottom(int doc) {
      // bottom.score is already set to Float.NaN in add().
      bottom.doc = docBase + doc;
      bottom = pq.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        if ((reverseMul * comparator.compareBottom(doc)) <= 0) {
          // since docs are visited in doc Id order, if compare is 0, it means
          // this document is larger than anything else in the queue, and
          // therefore not competitive.
          return;
        }
        
        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc);
        comparator.setBottom(bottom.slot);
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, Float.NaN);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }
    }
    
    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.docBase = context.docBase;
      queue.setComparator(0, comparator.setNextReader(context));
      comparator = queue.firstComparator;
    }
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {
      comparator.setScorer(scorer);
    }
    
  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, without
   * tracking document scores and maxScore, and assumes out of orderness in doc
   * Ids collection.
   */
  private static class OutOfOrderOneComparatorNonScoringCollector extends
      OneComparatorNonScoringCollector {

    public OutOfOrderOneComparatorNonScoringCollector(FieldValueHitQueue<Entry> queue,
        int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        final int cmp = reverseMul * comparator.compareBottom(doc);
        if (cmp < 0 || (cmp == 0 && doc + docBase > bottom.doc)) {
          return;
        }
        
        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc);
        comparator.setBottom(bottom.slot);
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, Float.NaN);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, while tracking
   * document scores but no maxScore.
   */
  private static class OneComparatorScoringNoMaxScoreCollector extends
      OneComparatorNonScoringCollector {

    Scorer scorer;

    public OneComparatorScoringNoMaxScoreCollector(FieldValueHitQueue<Entry> queue,
        int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
    }
    
    final void updateBottom(int doc, float score) {
      bottom.doc = docBase + doc;
      bottom.score = score;
      bottom = pq.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        if ((reverseMul * comparator.compareBottom(doc)) <= 0) {
          // since docs are visited in doc Id order, if compare is 0, it means
          // this document is largest than anything else in the queue, and
          // therefore not competitive.
          return;
        }
        
        // Compute the score only if the hit is competitive.
        final float score = scorer.score();

        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc, score);
        comparator.setBottom(bottom.slot);
      } else {
        // Compute the score only if the hit is competitive.
        final float score = scorer.score();

        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, score);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }
    }
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      comparator.setScorer(scorer);
    }
    
  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, while tracking
   * document scores but no maxScore, and assumes out of orderness in doc Ids
   * collection.
   */
  private static class OutOfOrderOneComparatorScoringNoMaxScoreCollector extends
      OneComparatorScoringNoMaxScoreCollector {

    public OutOfOrderOneComparatorScoringNoMaxScoreCollector(
        FieldValueHitQueue<Entry> queue, int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        final int cmp = reverseMul * comparator.compareBottom(doc);
        if (cmp < 0 || (cmp == 0 && doc + docBase > bottom.doc)) {
          return;
        }
        
        // Compute the score only if the hit is competitive.
        final float score = scorer.score();

        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc, score);
        comparator.setBottom(bottom.slot);
      } else {
        // Compute the score only if the hit is competitive.
        final float score = scorer.score();

        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, score);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, with tracking
   * document scores and maxScore.
   */
  private static class OneComparatorScoringMaxScoreCollector extends
      OneComparatorNonScoringCollector {

    Scorer scorer;
    
    public OneComparatorScoringMaxScoreCollector(FieldValueHitQueue<Entry> queue,
        int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
      // Must set maxScore to NEG_INF, or otherwise Math.max always returns NaN.
      maxScore = Float.NEGATIVE_INFINITY;
    }
    
    final void updateBottom(int doc, float score) {
      bottom.doc = docBase + doc;
      bottom.score = score;
      bottom =  pq.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      final float score = scorer.score();
      if (score > maxScore) {
        maxScore = score;
      }
      ++totalHits;
      if (queueFull) {
        if ((reverseMul * comparator.compareBottom(doc)) <= 0) {
          // since docs are visited in doc Id order, if compare is 0, it means
          // this document is largest than anything else in the queue, and
          // therefore not competitive.
          return;
        }
        
        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc, score);
        comparator.setBottom(bottom.slot);
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, score);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }

    }
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      super.setScorer(scorer);
    }
  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, with tracking
   * document scores and maxScore, and assumes out of orderness in doc Ids
   * collection.
   */
  private static class OutOfOrderOneComparatorScoringMaxScoreCollector extends
      OneComparatorScoringMaxScoreCollector {

    public OutOfOrderOneComparatorScoringMaxScoreCollector(FieldValueHitQueue<Entry> queue,
        int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      final float score = scorer.score();
      if (score > maxScore) {
        maxScore = score;
      }
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        final int cmp = reverseMul * comparator.compareBottom(doc);
        if (cmp < 0 || (cmp == 0 && doc + docBase > bottom.doc)) {
          return;
        }
        
        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc, score);
        comparator.setBottom(bottom.slot);
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, score);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector over multiple SortField criteria, without
   * tracking document scores and maxScore.
   */
  private static class MultiComparatorNonScoringCollector extends TopFieldCollector {
    
    final FieldComparator<?>[] comparators;
    final int[] reverseMul;
    final FieldValueHitQueue<Entry> queue;
    public MultiComparatorNonScoringCollector(FieldValueHitQueue<Entry> queue,
        int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
      this.queue = queue;
      comparators = queue.getComparators();
      reverseMul = queue.getReverseMul();
    }
    
    final void updateBottom(int doc) {
      // bottom.score is already set to Float.NaN in add().
      bottom.doc = docBase + doc;
      bottom = pq.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // Here c=0. If we're at the last comparator, this doc is not
            // competitive, since docs are visited in doc Id order, which means
            // this doc cannot compete with any other document in the queue.
            return;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        updateBottom(doc);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }
        add(slot, doc, Float.NaN);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      docBase = context.docBase;
      for (int i = 0; i < comparators.length; i++) {
        queue.setComparator(i, comparators[i].setNextReader(context));
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      // set the scorer on all comparators
      for (int i = 0; i < comparators.length; i++) {
        comparators[i].setScorer(scorer);
      }
    }
  }
  
  /*
   * Implements a TopFieldCollector over multiple SortField criteria, without
   * tracking document scores and maxScore, and assumes out of orderness in doc
   * Ids collection.
   */
  private static class OutOfOrderMultiComparatorNonScoringCollector extends
      MultiComparatorNonScoringCollector {
    
    public OutOfOrderMultiComparatorNonScoringCollector(FieldValueHitQueue<Entry> queue,
        int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // This is the equals case.
            if (doc + docBase > bottom.doc) {
              // Definitely not competitive
              return;
            }
            break;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        updateBottom(doc);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }
        add(slot, doc, Float.NaN);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector over multiple SortField criteria, with
   * tracking document scores and maxScore.
   */
  private static class MultiComparatorScoringMaxScoreCollector extends MultiComparatorNonScoringCollector {
    
    Scorer scorer;
    
    public MultiComparatorScoringMaxScoreCollector(FieldValueHitQueue<Entry> queue,
        int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
      // Must set maxScore to NEG_INF, or otherwise Math.max always returns NaN.
      maxScore = Float.NEGATIVE_INFINITY;
    }
    
    final void updateBottom(int doc, float score) {
      bottom.doc = docBase + doc;
      bottom.score = score;
      bottom =  pq.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      final float score = scorer.score();
      if (score > maxScore) {
        maxScore = score;
      }
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // Here c=0. If we're at the last comparator, this doc is not
            // competitive, since docs are visited in doc Id order, which means
            // this doc cannot compete with any other document in the queue.
            return;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        updateBottom(doc, score);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }
        add(slot, doc, score);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      super.setScorer(scorer);
    }
  }

  /*
   * Implements a TopFieldCollector over multiple SortField criteria, with
   * tracking document scores and maxScore, and assumes out of orderness in doc
   * Ids collection.
   */
  private final static class OutOfOrderMultiComparatorScoringMaxScoreCollector
      extends MultiComparatorScoringMaxScoreCollector {
    
    public OutOfOrderMultiComparatorScoringMaxScoreCollector(FieldValueHitQueue<Entry> queue,
        int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      final float score = scorer.score();
      if (score > maxScore) {
        maxScore = score;
      }
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // This is the equals case.
            if (doc + docBase > bottom.doc) {
              // Definitely not competitive
              return;
            }
            break;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        updateBottom(doc, score);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }
        add(slot, doc, score);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector over multiple SortField criteria, with
   * tracking document scores and maxScore.
   */
  private static class MultiComparatorScoringNoMaxScoreCollector extends MultiComparatorNonScoringCollector {
    
    Scorer scorer;
    
    public MultiComparatorScoringNoMaxScoreCollector(FieldValueHitQueue<Entry> queue,
        int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
    }
    
    final void updateBottom(int doc, float score) {
      bottom.doc = docBase + doc;
      bottom.score = score;
      bottom = pq.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // Here c=0. If we're at the last comparator, this doc is not
            // competitive, since docs are visited in doc Id order, which means
            // this doc cannot compete with any other document in the queue.
            return;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        // Compute score only if it is competitive.
        final float score = scorer.score();
        updateBottom(doc, score);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }

        // Compute score only if it is competitive.
        final float score = scorer.score();
        add(slot, doc, score);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      super.setScorer(scorer);
    }
  }

  /*
   * Implements a TopFieldCollector over multiple SortField criteria, with
   * tracking document scores and maxScore, and assumes out of orderness in doc
   * Ids collection.
   */
  private final static class OutOfOrderMultiComparatorScoringNoMaxScoreCollector
      extends MultiComparatorScoringNoMaxScoreCollector {
    
    public OutOfOrderMultiComparatorScoringNoMaxScoreCollector(
        FieldValueHitQueue<Entry> queue, int numHits, boolean fillFields) {
      super(queue, numHits, fillFields);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // This is the equals case.
            if (doc + docBase > bottom.doc) {
              // Definitely not competitive
              return;
            }
            break;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        // Compute score only if it is competitive.
        final float score = scorer.score();
        updateBottom(doc, score);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }

        // Compute score only if it is competitive.
        final float score = scorer.score();
        add(slot, doc, score);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      super.setScorer(scorer);
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector when after != null.
   */
  private final static class PagingFieldCollector extends TopFieldCollector {

    Scorer scorer;
    int collectedHits;
    final FieldComparator<?>[] comparators;
    final int[] reverseMul;
    final FieldValueHitQueue<Entry> queue;
    final boolean trackDocScores;
    final boolean trackMaxScore;
    final FieldDoc after;
    int afterDoc;
    
    public PagingFieldCollector(
                                FieldValueHitQueue<Entry> queue, FieldDoc after, int numHits, boolean fillFields,
                                boolean trackDocScores, boolean trackMaxScore) {
      super(queue, numHits, fillFields);
      this.queue = queue;
      this.trackDocScores = trackDocScores;
      this.trackMaxScore = trackMaxScore;
      this.after = after;
      comparators = queue.getComparators();
      reverseMul = queue.getReverseMul();

      // Must set maxScore to NEG_INF, or otherwise Math.max always returns NaN.
      maxScore = Float.NEGATIVE_INFINITY;
    }
    
    void updateBottom(int doc, float score) {
      bottom.doc = docBase + doc;
      bottom.score = score;
      bottom = pq.updateTop();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void collect(int doc) throws IOException {
      totalHits++;

      //System.out.println("  collect doc=" + doc);

      // Check if this hit was already collected on a
      // previous page:
      boolean sameValues = true;
      for(int compIDX=0;compIDX<comparators.length;compIDX++) {
        final FieldComparator comp = comparators[compIDX];

        final int cmp = reverseMul[compIDX] * comp.compareDocToValue(doc, after.fields[compIDX]);
        if (cmp < 0) {
          // Already collected on a previous page
          //System.out.println("    skip: before");
          return;
        } else if (cmp > 0) {
          // Not yet collected
          sameValues = false;
          //System.out.println("    keep: after");
          break;
        }
      }

      // Tie-break by docID:
      if (sameValues && doc <= afterDoc) {
        // Already collected on a previous page
        //System.out.println("    skip: tie-break");
        return;
      }

      collectedHits++;

      float score = Float.NaN;
      if (trackMaxScore) {
        score = scorer.score();
        if (score > maxScore) {
          maxScore = score;
        }
      }

      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // This is the equals case.
            if (doc + docBase > bottom.doc) {
              // Definitely not competitive
              return;
            }
            break;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        // Compute score only if it is competitive.
        if (trackDocScores && !trackMaxScore) {
          score = scorer.score();
        }
        updateBottom(doc, score);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = collectedHits - 1;
        //System.out.println("    slot=" + slot);
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }

        // Compute score only if it is competitive.
        if (trackDocScores && !trackMaxScore) {
          score = scorer.score();
        }
        bottom = pq.add(new Entry(slot, docBase + doc, score));
        queueFull = collectedHits == numHits;
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) {
      this.scorer = scorer;
      for (int i = 0; i < comparators.length; i++) {
        comparators[i].setScorer(scorer);
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
      docBase = context.docBase;
      afterDoc = after.doc - docBase;
      for (int i = 0; i < comparators.length; i++) {
        queue.setComparator(i, comparators[i].setNextReader(context));
      }
    }
  }

  private static final ScoreDoc[] EMPTY_SCOREDOCS = new ScoreDoc[0];
  
  private final boolean fillFields;

  /*
   * Stores the maximum score value encountered, needed for normalizing. If
   * document scores are not tracked, this value is initialized to NaN.
   */
  float maxScore = Float.NaN;

  final int numHits;
  FieldValueHitQueue.Entry bottom = null;
  boolean queueFull;
  int docBase;
  
  // Declaring the constructor private prevents extending this class by anyone
  // else. Note that the class cannot be final since it's extended by the
  // internal versions. If someone will define a constructor with any other
  // visibility, then anyone will be able to extend the class, which is not what
  // we want.
  private TopFieldCollector(PriorityQueue<Entry> pq, int numHits, boolean fillFields) {
    super(pq);
    this.numHits = numHits;
    this.fillFields = fillFields;
  }

  /**
   * Creates a new {@link TopFieldCollector} from the given
   * arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>.
   * 
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param fillFields
   *          specifies whether the actual field values should be returned on
   *          the results (FieldDoc).
   * @param trackDocScores
   *          specifies whether document scores should be tracked and set on the
   *          results. Note that if set to false, then the results' scores will
   *          be set to Float.NaN. Setting this to true affects performance, as
   *          it incurs the score computation on each competitive result.
   *          Therefore if document scores are not required by the application,
   *          it is recommended to set it to false.
   * @param trackMaxScore
   *          specifies whether the query's maxScore should be tracked and set
   *          on the resulting {@link TopDocs}. Note that if set to false,
   *          {@link TopDocs#getMaxScore()} returns Float.NaN. Setting this to
   *          true affects performance as it incurs the score computation on
   *          each result. Also, setting this true automatically sets
   *          <code>trackDocScores</code> to true as well.
   * @param docsScoredInOrder
   *          specifies whether documents are scored in doc Id order or not by
   *          the given {@link Scorer} in {@link #setScorer(Scorer)}.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   * @throws IOException if there is a low-level I/O error
   */
  public static TopFieldCollector create(Sort sort, int numHits,
      boolean fillFields, boolean trackDocScores, boolean trackMaxScore,
      boolean docsScoredInOrder)
      throws IOException {
    return create(sort, numHits, null, fillFields, trackDocScores, trackMaxScore, docsScoredInOrder);
  }

  /**
   * Creates a new {@link TopFieldCollector} from the given
   * arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>.
   * 
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param after
   *          only hits after this FieldDoc will be collected
   * @param fillFields
   *          specifies whether the actual field values should be returned on
   *          the results (FieldDoc).
   * @param trackDocScores
   *          specifies whether document scores should be tracked and set on the
   *          results. Note that if set to false, then the results' scores will
   *          be set to Float.NaN. Setting this to true affects performance, as
   *          it incurs the score computation on each competitive result.
   *          Therefore if document scores are not required by the application,
   *          it is recommended to set it to false.
   * @param trackMaxScore
   *          specifies whether the query's maxScore should be tracked and set
   *          on the resulting {@link TopDocs}. Note that if set to false,
   *          {@link TopDocs#getMaxScore()} returns Float.NaN. Setting this to
   *          true affects performance as it incurs the score computation on
   *          each result. Also, setting this true automatically sets
   *          <code>trackDocScores</code> to true as well.
   * @param docsScoredInOrder
   *          specifies whether documents are scored in doc Id order or not by
   *          the given {@link Scorer} in {@link #setScorer(Scorer)}.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   * @throws IOException if there is a low-level I/O error
   */
  public static TopFieldCollector create(Sort sort, int numHits, FieldDoc after,
      boolean fillFields, boolean trackDocScores, boolean trackMaxScore,
      boolean docsScoredInOrder)
      throws IOException {

    if (sort.fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }
    
    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    FieldValueHitQueue<Entry> queue = FieldValueHitQueue.create(sort.fields, numHits);

    if (after == null) {
      if (queue.getComparators().length == 1) {
        if (docsScoredInOrder) {
          if (trackMaxScore) {
            return new OneComparatorScoringMaxScoreCollector(queue, numHits, fillFields);
          } else if (trackDocScores) {
            return new OneComparatorScoringNoMaxScoreCollector(queue, numHits, fillFields);
          } else {
            return new OneComparatorNonScoringCollector(queue, numHits, fillFields);
          }
        } else {
          if (trackMaxScore) {
            return new OutOfOrderOneComparatorScoringMaxScoreCollector(queue, numHits, fillFields);
          } else if (trackDocScores) {
            return new OutOfOrderOneComparatorScoringNoMaxScoreCollector(queue, numHits, fillFields);
          } else {
            return new OutOfOrderOneComparatorNonScoringCollector(queue, numHits, fillFields);
          }
        }
      }

      // multiple comparators.
      if (docsScoredInOrder) {
        if (trackMaxScore) {
          return new MultiComparatorScoringMaxScoreCollector(queue, numHits, fillFields);
        } else if (trackDocScores) {
          return new MultiComparatorScoringNoMaxScoreCollector(queue, numHits, fillFields);
        } else {
          return new MultiComparatorNonScoringCollector(queue, numHits, fillFields);
        }
      } else {
        if (trackMaxScore) {
          return new OutOfOrderMultiComparatorScoringMaxScoreCollector(queue, numHits, fillFields);
        } else if (trackDocScores) {
          return new OutOfOrderMultiComparatorScoringNoMaxScoreCollector(queue, numHits, fillFields);
        } else {
          return new OutOfOrderMultiComparatorNonScoringCollector(queue, numHits, fillFields);
        }
      }
    } else {
      if (after.fields == null) {
        throw new IllegalArgumentException("after.fields wasn't set; you must pass fillFields=true for the previous search");
      }

      if (after.fields.length != sort.getSort().length) {
        throw new IllegalArgumentException("after.fields has " + after.fields.length + " values but sort has " + sort.getSort().length);
      }

      return new PagingFieldCollector(queue, after, numHits, fillFields, trackDocScores, trackMaxScore);
    }
  }
  
  final void add(int slot, int doc, float score) {
    bottom = pq.add(new Entry(slot, docBase + doc, score));
    queueFull = totalHits == numHits;
  }

  /*
   * Only the following callback methods need to be overridden since
   * topDocs(int, int) calls them to return the results.
   */

  @Override
  protected void populateResults(ScoreDoc[] results, int howMany) {
    if (fillFields) {
      // avoid casting if unnecessary.
      FieldValueHitQueue<Entry> queue = (FieldValueHitQueue<Entry>) pq;
      for (int i = howMany - 1; i >= 0; i--) {
        results[i] = queue.fillFields(queue.pop());
      }
    } else {
      for (int i = howMany - 1; i >= 0; i--) {
        Entry entry = pq.pop();
        results[i] = new FieldDoc(entry.doc, entry.score);
      }
    }
  }
  
  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      results = EMPTY_SCOREDOCS;
      // Set maxScore to NaN, in case this is a maxScore tracking collector.
      maxScore = Float.NaN;
    }

    // If this is a maxScoring tracking collector and there were no results, 
    return new TopFieldDocs(totalHits, results, ((FieldValueHitQueue<Entry>) pq).getFields(), maxScore);
  }
  
  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }
}
