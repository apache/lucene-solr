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
package org.apache.lucene.misc.search;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.misc.search.DiversifiedTopDocsCollector.ScoreDocKey;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.PriorityQueue;

/**
 * A {@link TopDocsCollector} that controls diversity in results by ensuring no more than
 * maxHitsPerKey results from a common source are collected in the final results.
 *
 * <p>An example application might be a product search in a marketplace where no more than 3 results
 * per retailer are permitted in search results.
 *
 * <p>To compare behaviour with other forms of collector, a useful analogy might be the problem of
 * making a compilation album of 1967's top hit records:
 *
 * <ol>
 *   <li>A vanilla query's results might look like a "Best of the Beatles" album - high quality but
 *       not much diversity
 *   <li>A GroupingSearch would produce the equivalent of "The 10 top-selling artists of 1967 - some
 *       killer and quite a lot of filler"
 *   <li>A "diversified" query would be the top 20 hit records of that year - with a max of 3
 *       Beatles hits in order to maintain diversity
 * </ol>
 *
 * This collector improves on the "GroupingSearch" type queries by
 *
 * <ul>
 *   <li>Working in one pass over the data
 *   <li>Not requiring the client to guess how many groups are required
 *   <li>Removing low-scoring "filler" which sits at the end of each group's hits
 * </ul>
 *
 * This is an abstract class and subclasses have to provide a source of keys for documents which is
 * then used to help identify duplicate sources.
 *
 * @lucene.experimental
 */
public abstract class DiversifiedTopDocsCollector extends TopDocsCollector<ScoreDocKey> {
  ScoreDocKey spare;
  private ScoreDocKeyQueue globalQueue;
  private int numHits;
  private Map<Long, ScoreDocKeyQueue> perKeyQueues;
  protected int maxNumPerKey;
  private Stack<ScoreDocKeyQueue> sparePerKeyQueues = new Stack<>();

  public DiversifiedTopDocsCollector(int numHits, int maxHitsPerKey) {
    super(new ScoreDocKeyQueue(numHits));
    // Need to access pq.lessThan() which is protected so have to cast here...
    this.globalQueue = (ScoreDocKeyQueue) pq;
    perKeyQueues = new HashMap<Long, ScoreDocKeyQueue>();
    this.numHits = numHits;
    this.maxNumPerKey = maxHitsPerKey;
  }

  /** Get a source of values used for grouping keys */
  protected abstract NumericDocValues getKeys(LeafReaderContext context);

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE;
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      return EMPTY_TOPDOCS;
    }

    return new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), results);
  }

  protected ScoreDocKey insert(ScoreDocKey addition, int docBase, NumericDocValues keys)
      throws IOException {
    if ((globalQueue.size() >= numHits) && (globalQueue.lessThan(addition, globalQueue.top()))) {
      // Queue is full and proposed addition is not a globally
      // competitive score
      return addition;
    }
    // The addition stands a chance of being entered - check the
    // key-specific restrictions.
    // We delay fetching the key until we are certain the score is globally
    // competitive. We need to adjust the ScoreDoc's global doc value to be
    // a leaf reader value when looking up keys
    int leafDocID = addition.doc - docBase;
    long value;
    if (keys.advanceExact(leafDocID)) {
      value = keys.longValue();
    } else {
      value = 0;
    }
    addition.key = value;

    // For this to work the choice of key class needs to implement
    // hashcode and equals.
    ScoreDocKeyQueue thisKeyQ = perKeyQueues.get(addition.key);

    if (thisKeyQ == null) {
      if (sparePerKeyQueues.size() == 0) {
        thisKeyQ = new ScoreDocKeyQueue(maxNumPerKey);
      } else {
        thisKeyQ = sparePerKeyQueues.pop();
      }
      perKeyQueues.put(addition.key, thisKeyQ);
    }
    ScoreDocKey perKeyOverflow = thisKeyQ.insertWithOverflow(addition);
    if (perKeyOverflow == addition) {
      // This key group has reached capacity and our proposed addition
      // was not competitive in the group - do not insert into the
      // main PQ or the key will be overly-populated in final results.
      return addition;
    }
    if (perKeyOverflow == null) {
      // This proposed addition is also locally competitive within the
      // key group - make a global entry and return
      ScoreDocKey globalOverflow = globalQueue.insertWithOverflow(addition);
      perKeyGroupRemove(globalOverflow);
      return globalOverflow;
    }
    // For the given key, we have reached max capacity but the new addition
    // is better than a prior entry that still exists in the global results
    // - request the weaker-scoring entry to be removed from the global
    // queue.
    globalQueue.remove(perKeyOverflow);
    // Add the locally-competitive addition into the globally queue
    globalQueue.add(addition);
    return perKeyOverflow;
  }

  private void perKeyGroupRemove(ScoreDocKey globalOverflow) {
    if (globalOverflow == null) {
      return;
    }
    ScoreDocKeyQueue q = perKeyQueues.get(globalOverflow.key);
    ScoreDocKey perKeyLowest = q.pop();
    // The least globally-competitive item should also always be the least
    // key-local item
    assert (globalOverflow == perKeyLowest);
    if (q.size() == 0) {
      perKeyQueues.remove(globalOverflow.key);
      sparePerKeyQueues.push(q);
    }
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    final int base = context.docBase;
    final NumericDocValues keySource = getKeys(context);

    return new LeafCollector() {
      Scorable scorer;

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        this.scorer = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        float score = scorer.score();

        // This collector cannot handle NaN
        assert !Float.isNaN(score);

        totalHits++;

        doc += base;

        if (spare == null) {
          spare = new ScoreDocKey(doc, score);
        } else {
          spare.doc = doc;
          spare.score = score;
        }
        spare = insert(spare, base, keySource);
      }
    };
  }

  static class ScoreDocKeyQueue extends PriorityQueue<ScoreDocKey> {

    ScoreDocKeyQueue(int size) {
      super(size);
    }

    @Override
    protected final boolean lessThan(ScoreDocKey hitA, ScoreDocKey hitB) {
      if (hitA.score == hitB.score) {
        return hitA.doc > hitB.doc;
      } else {
        return hitA.score < hitB.score;
      }
    }
  }

  //
  /** An extension to ScoreDoc that includes a key used for grouping purposes */
  public static class ScoreDocKey extends ScoreDoc {
    Long key;

    protected ScoreDocKey(int doc, float score) {
      super(doc, score);
    }

    public Long getKey() {
      return key;
    }

    @Override
    public String toString() {
      return "key:" + key + " doc=" + doc + " s=" + score;
    }
  }
}
