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


import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorDocValues;
import org.apache.lucene.util.PriorityQueue;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * A per-document array of references to other documents.  nocommit Should this class be moved to
 * o.a.l.index ? It currently exposes at least one method that is not really part of its intended
 * public api in order to make it accessible to KnnGraphWriter.
 */
public class GraphSearch {

  // private static final boolean VERBOSE = Boolean.parseBoolean(System.getProperty("GraphSearch.verbose", "false"));
  public static boolean VERBOSE;
  private final Set<Integer> visited = new HashSet<>();
  private final IndexSearcher searcher;
  private final String vectorField;
  private final String neighborField;
  private final int topK;

  private float[] scratch;
  private ScoreDocQueue queue;
  private TreeSet<ScoreDoc> frontier;

  /**
   * @param topK how many results to return when searching, and how many nearest neighbors (fanout)
   * to connect while indexing
   */
  public GraphSearch(int topK) {
    this(null, null, null, topK);
  }

  public static GraphSearch fromDimension(int dimension) {
    // TODO: experiment to find out how we can best set these heuristics
    // Malkov, Ponomarenko, Logvinov, Krylov found 3*dim optimal for dim <= 20
    // Their statements about how many iters to run while indexing amount to running a monte carlo experiment
    // return new GraphSearch((int) (180 * (Math.log(1 + dimension / 20.0))));
    // return new GraphSearch(3 * dimension);
    return new GraphSearch(60);
  }

  private GraphSearch(IndexSearcher searcher, String vectorField, String neighborField, int topK) {
    this.searcher = searcher;
    this.vectorField = vectorField;
    this.neighborField = neighborField;
    this.topK = topK;           // PriorityQueue could expose this, but does not
    frontier = new TreeSet<>(GraphSearch::compareScoreDoc);
  }

  /**
   * Find the topK nearest neighbors to target.
   * @param topK how many results to return when searching, and how many nearest neighbors (fanout)
   * to connect while indexing
   * @param numProbe how many probes of the graph to perform when searching (and finding neighbors
   * while indexing).
   * @return a TopDocs listing the topK (approximate) nearest neighbors to target in order of
   * increasing distance and docid.
   */
  public static TopDocs search(IndexSearcher searcher, String knnGraphField, int topK, int numProbe, float[] target)
      throws IOException {
    return new GraphSearch(searcher, knnGraphField, knnGraphField + "$nbr", topK).search(target, numProbe);
  }

  private TopDocs search(float[] target, int numProbe) throws IOException {
    // TODO: implement a Query and let IndexSearcher/Collector handle this
    ScoreDocQueue segmentQueues[] = new ScoreDocQueue[searcher.getIndexReader().leaves().size()];
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      LeafReader reader = context.reader();
      if (VERBOSE) {
        System.out.printf("[GraphSearch] segment #%d [%d docs]\n", context.ord, reader.maxDoc());
      }
      frontier.clear();
      queue = new ScoreDocQueue(topK, () -> new ScoreDoc(-1, Float.MAX_VALUE), false);
      doSearch(() -> VectorDocValues.get(reader, vectorField),
          () -> DocValues.getSortedNumeric(reader, neighborField),
          target, reader.maxDoc(), numProbe);
      segmentQueues[context.ord] = queue;
    }
    return constructResults(segmentQueues, searcher.getIndexReader().leaves());
  }

  public interface SupplierThrowsIoe<T> {
    T get() throws IOException;
  }

  /**
   * Find the (approximate) nearest neighbor documents to the given target vector. Used when
   * indexing - not intended as a public method. 
   * @param vectorsFactory Creates VectorDocValues of the documents to search
   * @param neighborsFactory Creates SortedNumericDocValues representing the graph to search
   * @param target the target vector
   * @param maxDoc one more than the maximum document to search. This is used to generate seed entry points in the graph
   * @return an Iterable of the approximately nearest docs, ordered by increasing distance from the target
   * @throws IOException when there is an underlying exception reading the index
   */
  public Iterable<ScoreDoc> search(SupplierThrowsIoe<VectorDocValues> vectorsFactory,
                                   SupplierThrowsIoe<SortedNumericDocValues> neighborsFactory,
                                   float[] target, int maxDoc)
    throws IOException {
    assert maxDoc > 0;
    queue = new ScoreDocQueue(topK, () -> new ScoreDoc(-1, Float.MAX_VALUE), false);
    //System.out.printf("graph search maxDoc=%d\n", maxDoc);
    // start from a set of limit (= log10(N)) documents, biased towards the lower ones
    int numProbes = (int) Math.round(2 * (Math.log(maxDoc) + 1));
    doSearch(vectorsFactory, neighborsFactory, target, maxDoc, numProbes);
    return queue;
  }

  private void doSearch(SupplierThrowsIoe<VectorDocValues> vectorsFactory, SupplierThrowsIoe<SortedNumericDocValues> neighborsFactory,
                        float[] target, int maxDoc, int numProbes) throws IOException {
    scratch = new float[target.length]; // TODO: move to constructor and require dimension to be provided there
    visited.clear();
    VectorDocValues vectors = vectorsFactory.get();
    int entryDocId = maxDoc % numProbes;  // pseudorandom rotation among document probe cycles as the index increases in size
    for (int i = 0; i < numProbes; i++, entryDocId += getEntryIncrement(numProbes, maxDoc)) {
      if (VERBOSE) {
        System.out.printf("[GraphSearch] entryDocId #%d = %d\n", i, entryDocId);
      }
      entryDocId %= maxDoc;
      int docId = vectors.advance(entryDocId);
      if (docId == NO_MORE_DOCS || docId >= maxDoc) {
        if (i == 0) {
          // edge case - we advanced past the send of the segment on our first attempt; just try again from the beginning
          docId = vectors.advance(0);
          assert docId != NO_MORE_DOCS && docId < maxDoc;
        } else {
          return;
        }
      }
      if (visited.contains(docId)) {
        continue;
      }
      ScoreDoc front = queue.top();
      // if docid is competitive, front will be set to <docId, d(docId, target)>
      enqueue(docId, target, vectors, front);
      if (front.doc != docId) {
        // FIXME - on following segments, score is not competitive here - we need to give it a chance
        continue;
      }
      if (VERBOSE) {
        System.out.printf("[GraphSearch] i=%d doc=%d\n", i, docId);
      }
      while (true) {
        VectorDocValues childVectors = vectorsFactory.get();
        SortedNumericDocValues neighbors = neighborsFactory.get();
        // front may have docid = -1???
        ScoreDoc top = gather(childVectors, neighbors, target, front, maxDoc);
        front = frontier.pollLast();
        if (front == null || front.score > top.score) {
          // No frontier doc is competitive
          break;
        }
      }
    }
  }

  private ScoreDoc gather(VectorDocValues vectors, SortedNumericDocValues neighbors, float[] target, ScoreDoc front, int maxDoc) throws IOException {
    assert front.doc >= 0;
    assert front.doc < maxDoc : "docid=" + front.doc + ", maxDoc=" + maxDoc;
    ScoreDoc bottom = queue.top();
    //System.out.printf("  get neighbors of %d\n", front.doc);
    if (neighbors.advanceExact(front.doc) == false) {
      // when merging this seems to happen? why isn't it taken care of above?
      return bottom;
    }
    int n = neighbors.docValueCount();
    assert n > 0;
    for (int i = 0; i < n; i++) {
      int docId = (int) neighbors.nextValue();
      assert docId >= 0;
      assert docId < maxDoc : "docid=" + docId + ", maxDoc=" + maxDoc;
      if (visited.contains(docId) == false) {
        visited.add(docId);
        boolean hasVector = vectors.advanceExact(docId);
        assert hasVector : "doc " + (docId) + " has no vector";
        vectors.vector(scratch);
        float distance = distance(scratch, target, bottom.score);
        if (VERBOSE) {
          System.out.printf("  traverse doc=%d dist=%f\n", docId, distance);
        }
        // Add competitive neighbors to the output queue FIXME this test does not capture that we must compare scores here
        if (updateQueue(bottom, docId, distance)) {
          // and to the frontier for further expansion, creating a new ScoreDoc since
          // we modify the docs in the result queue
          frontier.add(new ScoreDoc(docId, distance));
          bottom = queue.top();
        }
      }
    }
    return bottom;
  }

  private void enqueue(int doc, float[] target, VectorDocValues vectors, ScoreDoc top) throws IOException {
    boolean hasVector = vectors.advanceExact(doc);
    assert hasVector : "doc " + doc + " has no vector";
    visited.add(doc);
    vectors.vector(scratch);
    float score = distance(target, scratch, top.score);
    updateQueue(top, doc, score);
  }

  private boolean updateQueue(ScoreDoc top, int doc, float distance) throws IOException {
    //System.out.printf("  distance to %d = %f\n", doc, distance);
    if (distance < top.score || (distance == top.score && doc < top.doc)) {
      // If this neighbor is competitive, add it to the topK queue
      top.score = distance;
      // record global docid since we are merging into a global queue
      top.doc = doc;
      queue.updateTop();
      return true;
      // System.out.println(" queue " + scoreDoc.doc + " " + distance + " new min score=" + top.score);
    } else {
      return false;
    }
  }

  private TopDocs constructResults(ScoreDocQueue[] queues, List<LeafReaderContext> contexts) {
    TopDocs[] topDocs = new TopDocs[queues.length];
    for (int i = 0; i < topDocs.length; i++) {
      topDocs[i] = constructResults(queues[i], contexts.get(i).docBase);
    }
    return TopDocs.merge(topK, topDocs);
  }

  private TopDocs constructResults(ScoreDocQueue q, int docBase) {
    int found = 0;
    for (ScoreDoc scoreDoc : q) {
      if (scoreDoc.doc >= 0) {
        ++found;
      }
    }
    ScoreDoc[] results = new ScoreDoc[found];
    for (int i = found -1 ; i >= 0;) {
      ScoreDoc scoreDoc = q.pop();
      // skip sentinels
      if (scoreDoc.doc != -1) {
        scoreDoc.doc += docBase;
        scoreDoc.score = -scoreDoc.score; // TopDocs.merge will sort in ascending score order
        results[i--] = scoreDoc;
      }
    }
    // the search is for the K nearest neighbors, so we never have more than K to return. The number
    // found may be less than K though.
    return new TopDocs(new TotalHits(found, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), results);
  }

  private static float distance(float[] a, float[] b, float minScore) {
    assert a.length == b.length;
    float total = 0;
    for (int i = 0; i < a.length; i++) {
      float d = a[i] - b[i];
      total += d * d;
      if (total > minScore) {
        // return early since every dimension of the score is positive; it can only increase
        // TODO: optimize by skipping this test until the queue is full of non-sentinels
        return Float.MAX_VALUE;
      }
    }
    return total;
  }

  private static int getEntryIncrement(int m, int maxDoc) {
    return maxDoc / (m + 1);
  }

  /**
   * Prefers docs with lower (positive) scores and lower docids
   */
  private static class ScoreDocQueue extends PriorityQueue<ScoreDoc> {
    private final boolean ascending;

    /**
     * Creates a new queue with the given size and rank order
     * @param capacity the number of elements the queue accommodates
     * @param ascending if true, the least element is that with the least score. Conversely if false,
     *                 the least element has the greatest score. In both cases, when scores are equal,
     *                 a document with a higher docId is less than a document with a lower docId.
     */
    ScoreDocQueue(int capacity, Supplier<ScoreDoc> sentinel, boolean ascending) {
      super(capacity, sentinel);
      this.ascending = ascending;
    }

    @Override
    protected boolean lessThan(ScoreDoc a, ScoreDoc b) {
      if (a.score > b.score) {
        return !ascending;
      } else if (a.score < b.score) {
        return ascending;
      } else {
        return a.doc > b.doc;
      }
    }
  }

  private static int compareScoreDoc(ScoreDoc a, ScoreDoc b) {
    if (a.score < b.score) {
      return 1;
    } else if (a.score > b.score) {
      return -1;
    } else {
      return b.doc - a.doc;
    }
  }
}

