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

package org.apache.lucene.search;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.lucene.search.TopFieldCollector.EMPTY_SCOREDOCS;

/**
 * CollectorManager which allows early termination across multiple slices
 * when the index sort key and the query sort key are the same
 */
public class EarlyTerminatingFieldCollectorManager implements CollectorManager<TopFieldCollector, TopFieldDocs> {
  private final Sort sort;
  private final int numHits;
  private final int totalHitsThreshold;
  private final AtomicInteger globalTotalHits;
  private final ReentrantLock lock;
  private int numCollectors;

  private final ConcurrentLinkedQueue<TopFieldCollector.EarlyTerminatingFieldCollector> mergeableCollectors;
  private FieldValueHitQueue globalHitQueue;
  private FieldValueHitQueue.Entry bottom;
  // We do not make this Atomic since it will be sought under a lock
  private int queueSlotCounter;
  private final AtomicBoolean mergeStarted;
  public final AtomicBoolean mergeCompleted;

  public EarlyTerminatingFieldCollectorManager(Sort sort, int numHits, int totalHitsThreshold) {
    this.sort = sort;
    this.numHits = numHits;
    this.totalHitsThreshold = totalHitsThreshold;
    this.globalTotalHits = new AtomicInteger();
    this.lock = new ReentrantLock();
    this.mergeStarted = new AtomicBoolean();
    this.mergeCompleted = new AtomicBoolean();
    this.mergeableCollectors = new ConcurrentLinkedQueue();
    this.globalHitQueue = null;
  }

  @Override
  public TopFieldCollector.EarlyTerminatingFieldCollector newCollector() {
    ++numCollectors;

    return new TopFieldCollector.EarlyTerminatingFieldCollector(sort, FieldValueHitQueue.create(sort.fields, numHits), numHits,
        totalHitsThreshold, this, globalTotalHits);
  }

  @Override
  public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) {

    if (globalHitQueue == null) {
      final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
      int i = 0;
      for (TopFieldCollector collector : collectors) {
        topDocs[i++] = collector.topDocs();
      }
      return TopDocs.merge(sort, 0, numHits, topDocs);
    }

    ScoreDoc[] results = populateResults(globalHitQueue.size());

    return newTopDocs(results);
  }

  public int compareAndUpdateBottom(int docBase, int doc, Object value) {

    try {
      lock.lock();

      // If not enough hits are accumulated, add this hit to the global hit queue
      if (globalHitQueue.size() < numHits) {
        FieldValueHitQueue.Entry newEntry = new FieldValueHitQueue.Entry(queueSlotCounter++, (doc + docBase), value);
        bottom = (FieldValueHitQueue.Entry) globalHitQueue.add(newEntry);
        return 1;
      }

      FieldComparator[] comparators = globalHitQueue.getComparators();
      int[] reverseMul = globalHitQueue.getReverseMul();
      Object bottomValues = bottom.values;
      Object[] valuesArray;
      Object[] bottomValuesArray;

      if (comparators.length > 1) {
        assert value instanceof Object[];
        valuesArray = (Object[]) value;

        assert bottomValues instanceof Object[];
        bottomValuesArray = (Object[]) bottomValues;
      } else {
        valuesArray = new Object[1];
        valuesArray[0] = value;

        bottomValuesArray = new Object[1];
        bottomValuesArray[0] = bottomValues;
      }

      int cmp;
      int i = 0;
      for (FieldComparator comparator : comparators) {
        cmp = reverseMul[i] * comparator.compareValues(bottomValuesArray[i], valuesArray[i]);
        ++i;

        if (cmp != 0) {
          if (cmp > 0) {
            updateBottom(docBase, doc, value);
          }

          return cmp;
        }
      }

      // For equal values, we choose the lower docID
      if ((doc + docBase) < bottom.doc) {
        updateBottom(docBase, doc, value);

        // Return a value greater than 0 to signify replacement
        return 1;
      }

      return 0;
    } finally {
      lock.unlock();
    }
  }

  private final void updateBottom(int docBase, int doc, Object values) {
    bottom.doc = docBase + doc;
    bottom.values = values;
    bottom = (FieldValueHitQueue.Entry) globalHitQueue.updateTop();

    assert bottom != null;
  }

  FieldValueHitQueue.Entry addCollectorToGlobalQueue(TopFieldCollector.EarlyTerminatingFieldCollector fieldCollector, int docBase) {
    FieldValueHitQueue queue = fieldCollector.queue;

    try {
      lock.lock();
      if (globalHitQueue == null) {
        this.globalHitQueue = FieldValueHitQueue.createValuesComparingQueue(sort.fields, numHits);
      }

      FieldValueHitQueue.Entry entry = (FieldValueHitQueue.Entry) queue.pop();
      while (entry != null) {
        if (queueSlotCounter > numHits) {
          throw new IllegalStateException("Global number exceeds number of hits. Current hit number " + queueSlotCounter + " numHits " + numHits);
        }


        if (globalHitQueue.size() > numHits) {
          throw new IllegalStateException("WTF?");
        }

        // If hit count was already achieved, return this entry
        if (globalHitQueue.size() == numHits) {
          return entry;
        }

        FieldValueHitQueue.Entry newEntry = new FieldValueHitQueue.Entry(queueSlotCounter++, (entry.doc + docBase), entry.values);

        bottom = (FieldValueHitQueue.Entry) globalHitQueue.add(newEntry);

        entry = (FieldValueHitQueue.Entry) queue.pop();
      }
    } finally {
      lock.unlock();
    }

    return null;
  }

  private ScoreDoc[] populateResults(int howMany) {
    ScoreDoc[] results = new ScoreDoc[howMany];
    // avoid casting if unnecessary.
    for (int i = howMany - 1; i >= 0; i--) {
      results[i] = globalHitQueue.fillFields((FieldValueHitQueue.Entry) globalHitQueue.pop());
    }

    return results;
  }

  protected TopFieldDocs newTopDocs(ScoreDoc[] results) {
    if (results == null) {
      results = EMPTY_SCOREDOCS;
    }

    //TODO: atris -- Is the relation correct, since we are early terminating?
    return new TopFieldDocs(new TotalHits(results.length, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), results, globalHitQueue.getFields());
  }
}
