package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.IndexReader;

/* Description from Doug Cutting (excerpted from
 * LUCENE-1483):
 *
 * BooleanScorer uses a ~16k array to score windows of
 * docs. So it scores docs 0-16k first, then docs 16-32k,
 * etc. For each window it iterates through all query terms
 * and accumulates a score in table[doc%16k]. It also stores
 * in the table a bitmask representing which terms
 * contributed to the score. Non-zero scores are chained in
 * a linked list. At the end of scoring each window it then
 * iterates through the linked list and, if the bitmask
 * matches the boolean constraints, collects a hit. For
 * boolean queries with lots of frequent terms this can be
 * much faster, since it does not need to update a priority
 * queue for each posting, instead performing constant-time
 * operations per posting. The only downside is that it
 * results in hits being delivered out-of-order within the
 * window, which means it cannot be nested within other
 * scorers. But it works well as a top-level scorer.
 *
 * The new BooleanScorer2 implementation instead works by
 * merging priority queues of postings, albeit with some
 * clever tricks. For example, a pure conjunction (all terms
 * required) does not require a priority queue. Instead it
 * sorts the posting streams at the start, then repeatedly
 * skips the first to to the last. If the first ever equals
 * the last, then there's a hit. When some terms are
 * required and some terms are optional, the conjunction can
 * be evaluated first, then the optional terms can all skip
 * to the match and be added to the score. Thus the
 * conjunction can reduce the number of priority queue
 * updates for the optional terms. */

final class BooleanScorer extends Scorer {
  
  private static final class BooleanScorerCollector extends Collector {
    private BucketTable bucketTable;
    private int mask;
    private Scorer scorer;
    
    public BooleanScorerCollector(int mask, BucketTable bucketTable) {
      this.mask = mask;
      this.bucketTable = bucketTable;
    }
    public final void collect(final int doc) throws IOException {
      final BucketTable table = bucketTable;
      final int i = doc & BucketTable.MASK;
      Bucket bucket = table.buckets[i];
      if (bucket == null)
        table.buckets[i] = bucket = new Bucket();
      
      if (bucket.doc != doc) {                    // invalid bucket
        bucket.doc = doc;                         // set doc
        bucket.score = scorer.score();            // initialize score
        bucket.bits = mask;                       // initialize mask
        bucket.coord = 1;                         // initialize coord

        bucket.next = table.first;                // push onto valid list
        table.first = bucket;
      } else {                                    // valid bucket
        bucket.score += scorer.score();           // increment score
        bucket.bits |= mask;                      // add bits in mask
        bucket.coord++;                           // increment coord
      }
    }
    
    public void setNextReader(IndexReader reader, int docBase) {
      // not needed by this implementation
    }
    
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }
    
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }
  
  // An internal class which is used in score(Collector, int) for setting the
  // current score. This is required since Collector exposes a setScorer method
  // and implementations that need the score will call scorer.score().
  // Therefore the only methods that are implemented are score() and doc().
  private static final class BucketScorer extends Scorer {

    float score;
    int doc = NO_MORE_DOCS;
    
    public BucketScorer() { super(null); }
    
    public int advance(int target) throws IOException { return NO_MORE_DOCS; }

    /** @deprecated use {@link #docID()} instead. */
    public int doc() { return doc; }

    public int docID() { return doc; }

    public Explanation explain(int doc) throws IOException { return null; }
    
    /** @deprecated use {@link #nextDoc()} instead. */
    public boolean next() throws IOException { return false; }

    public int nextDoc() throws IOException { return NO_MORE_DOCS; }
    
    public float score() throws IOException { return score; }
    
    /** @deprecated use {@link #advance(int)} instead. */
    public boolean skipTo(int target) throws IOException { return false; }
    
  }

  static final class Bucket {
    int doc = -1;            // tells if bucket is valid
    float score;             // incremental score
    int bits;                // used for bool constraints
    int coord;               // count of terms in score
    Bucket next;             // next valid bucket
  }
  
  /** A simple hash table of document scores within a range. */
  static final class BucketTable {
    public static final int SIZE = 1 << 11;
    public static final int MASK = SIZE - 1;

    final Bucket[] buckets = new Bucket[SIZE];
    Bucket first = null;                          // head of valid list
  
    public BucketTable() {}

    public Collector newCollector(int mask) {
      return new BooleanScorerCollector(mask, this);
    }

    public final int size() { return SIZE; }
  }

  static final class SubScorer {
    public Scorer scorer;
    public boolean required = false;
    public boolean prohibited = false;
    public Collector collector;
    public SubScorer next;

    public SubScorer(Scorer scorer, boolean required, boolean prohibited,
        Collector collector, SubScorer next)
      throws IOException {
      this.scorer = scorer;
      this.required = required;
      this.prohibited = prohibited;
      this.collector = collector;
      this.next = next;
    }
  }
  
  private SubScorer scorers = null;
  private BucketTable bucketTable = new BucketTable();
  private int maxCoord = 1;
  private final float[] coordFactors;
  private int requiredMask = 0;
  private int prohibitedMask = 0;
  private int nextMask = 1;
  private final int minNrShouldMatch;
  private int end;
  private Bucket current;
  private int doc = -1;

  BooleanScorer(Similarity similarity, int minNrShouldMatch,
      List optionalScorers, List prohibitedScorers) throws IOException {
    super(similarity);
    this.minNrShouldMatch = minNrShouldMatch;

    if (optionalScorers != null && optionalScorers.size() > 0) {
      for (Iterator si = optionalScorers.iterator(); si.hasNext();) {
        Scorer scorer = (Scorer) si.next();
        maxCoord++;
        if (scorer.nextDoc() != NO_MORE_DOCS) {
          scorers = new SubScorer(scorer, false, false, bucketTable.newCollector(0), scorers);
        }
      }
    }
    
    if (prohibitedScorers != null && prohibitedScorers.size() > 0) {
      for (Iterator si = prohibitedScorers.iterator(); si.hasNext();) {
        Scorer scorer = (Scorer) si.next();
        int mask = nextMask;
        nextMask = nextMask << 1;
        prohibitedMask |= mask;                     // update prohibited mask
        if (scorer.nextDoc() != NO_MORE_DOCS) {
          scorers = new SubScorer(scorer, false, true, bucketTable.newCollector(mask), scorers);
        }
      }
    }

    coordFactors = new float[maxCoord];
    Similarity sim = getSimilarity();
    for (int i = 0; i < maxCoord; i++) {
      coordFactors[i] = sim.coord(i, maxCoord - 1); 
    }
  }

  // firstDocID is ignored since nextDoc() initializes 'current'
  protected boolean score(Collector collector, int max, int firstDocID) throws IOException {
    boolean more;
    Bucket tmp;
    BucketScorer bs = new BucketScorer();
    // The internal loop will set the score and doc before calling collect.
    collector.setScorer(bs);
    do {
      bucketTable.first = null;
      
      while (current != null) {         // more queued 

        // check prohibited & required
        if ((current.bits & prohibitedMask) == 0 && 
            (current.bits & requiredMask) == requiredMask) {
          
          if (current.doc >= max){
            tmp = current;
            current = current.next;
            tmp.next = bucketTable.first;
            bucketTable.first = tmp;
            continue;
          }
          
          if (current.coord >= minNrShouldMatch) {
            bs.score = current.score * coordFactors[current.coord];
            bs.doc = current.doc;
            collector.collect(current.doc);
          }
        }
        
        current = current.next;         // pop the queue
      }
      
      if (bucketTable.first != null){
        current = bucketTable.first;
        bucketTable.first = current.next;
        return true;
      }

      // refill the queue
      more = false;
      end += BucketTable.SIZE;
      for (SubScorer sub = scorers; sub != null; sub = sub.next) {
        int subScorerDocID = sub.scorer.docID();
        if (subScorerDocID != NO_MORE_DOCS) {
          more |= sub.scorer.score(sub.collector, end, subScorerDocID);
        }
      }
      current = bucketTable.first;
      
    } while (current != null || more);

    return false;
  }

  /** @deprecated use {@link #score(Collector, int, int)} instead. */
  protected boolean score(HitCollector hc, int max) throws IOException {
    return score(new HitCollectorWrapper(hc), max, docID());
  }
  
  public int advance(int target) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** @deprecated use {@link #docID()} instead. */
  public int doc() { return current.doc; }
  
  public int docID() {
    return doc;
  }

  public Explanation explain(int doc) {
    throw new UnsupportedOperationException();
  }

  /** @deprecated use {@link #nextDoc()} instead. */
  public boolean next() throws IOException {
    return nextDoc() != NO_MORE_DOCS;
  }
  
  public int nextDoc() throws IOException {
    boolean more;
    do {
      while (bucketTable.first != null) {         // more queued
        current = bucketTable.first;
        bucketTable.first = current.next;         // pop the queue

        // check prohibited & required, and minNrShouldMatch
        if ((current.bits & prohibitedMask) == 0 &&
            (current.bits & requiredMask) == requiredMask &&
            current.coord >= minNrShouldMatch) {
          return doc = current.doc;
        }
      }

      // refill the queue
      more = false;
      end += BucketTable.SIZE;
      for (SubScorer sub = scorers; sub != null; sub = sub.next) {
        Scorer scorer = sub.scorer;
        sub.collector.setScorer(scorer);
        int doc = scorer.docID();
        while (doc < end) {
          sub.collector.collect(doc);
          doc = scorer.nextDoc();
        }
        more |= (doc != NO_MORE_DOCS);
      }
    } while (bucketTable.first != null || more);

    return doc = NO_MORE_DOCS;
  }

  public float score() {
    return current.score * coordFactors[current.coord];
  }

  public void score(Collector collector) throws IOException {
    score(collector, Integer.MAX_VALUE, nextDoc());
  }

  /** @deprecated use {@link #score(Collector)} instead. */
  public void score(HitCollector hc) throws IOException {
    score(new HitCollectorWrapper(hc));
  }
  
  /** @deprecated use {@link #advance(int)} instead. */
  public boolean skipTo(int target) {
    throw new UnsupportedOperationException();
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("boolean(");
    for (SubScorer sub = scorers; sub != null; sub = sub.next) {
      buffer.append(sub.scorer.toString());
      buffer.append(" ");
    }
    buffer.append(")");
    return buffer.toString();
  }

}
