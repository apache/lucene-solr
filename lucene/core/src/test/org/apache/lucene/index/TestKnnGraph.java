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
package org.apache.lucene.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.hnsw.HnswGraphBuilder.randSeed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene90.Lucene90VectorReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.index.VectorValues.SearchStrategy;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.junit.After;
import org.junit.Before;

/** Tests indexing of a knn-graph */
public class TestKnnGraph extends LuceneTestCase {

  private static final String KNN_GRAPH_FIELD = "vector";

  private static int maxConn = HnswGraphBuilder.DEFAULT_MAX_CONN;

  private SearchStrategy searchStrategy;

  @Before
  public void setup() {
    randSeed = random().nextLong();
    if (random().nextBoolean()) {
      maxConn = HnswGraphBuilder.DEFAULT_MAX_CONN;
      HnswGraphBuilder.DEFAULT_MAX_CONN = random().nextInt(256) + 2;
    }
    int strategy = random().nextInt(SearchStrategy.values().length - 1) + 1;
    searchStrategy = SearchStrategy.values()[strategy];
  }

  @After
  public void cleanup() {
    HnswGraphBuilder.DEFAULT_MAX_CONN = maxConn;
  }

  /** Basic test of creating documents in a graph */
  public void testBasic() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw =
            new IndexWriter(dir, newIndexWriterConfig(null).setCodec(Codec.forName("Lucene90")))) {
      int numDoc = atLeast(10);
      int dimension = atLeast(3);
      float[][] values = new float[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        if (random().nextBoolean()) {
          values[i] = new float[dimension];
          for (int j = 0; j < dimension; j++) {
            values[i][j] = random().nextFloat();
          }
        }
        add(iw, i, values[i]);
      }
      assertConsistentGraph(iw, values);
    }
  }

  public void testSingleDocument() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw =
            new IndexWriter(dir, newIndexWriterConfig(null).setCodec(Codec.forName("Lucene90")))) {
      float[][] values = new float[][] {new float[] {0, 1, 2}};
      add(iw, 0, values[0]);
      assertConsistentGraph(iw, values);
      iw.commit();
      assertConsistentGraph(iw, values);
    }
  }

  /** Verify that the graph properties are preserved when merging */
  public void testMerge() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw =
            new IndexWriter(dir, newIndexWriterConfig(null).setCodec(Codec.forName("Lucene90")))) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      float[][] values = randomVectors(numDoc, dimension);
      for (int i = 0; i < numDoc; i++) {
        if (random().nextBoolean()) {
          values[i] = new float[dimension];
          for (int j = 0; j < dimension; j++) {
            values[i][j] = random().nextFloat();
          }
          VectorUtil.l2normalize(values[i]);
        }
        add(iw, i, values[i]);
        if (random().nextInt(10) == 3) {
          iw.commit();
        }
      }
      if (random().nextBoolean()) {
        iw.forceMerge(1);
      }
      assertConsistentGraph(iw, values);
    }
  }

  /**
   * Verify that we get the *same* graph by indexing one segment as we do by indexing two segments
   * and merging.
   */
  public void testMergeProducesSameGraph() throws Exception {
    long seed = random().nextLong();
    int numDoc = atLeast(100);
    int dimension = atLeast(10);
    float[][] values = randomVectors(numDoc, dimension);
    int mergePoint = random().nextInt(numDoc);
    int[][] mergedGraph = getIndexedGraph(values, mergePoint, seed);
    int[][] singleSegmentGraph = getIndexedGraph(values, -1, seed);
    assertGraphEquals(singleSegmentGraph, mergedGraph);
  }

  private void assertGraphEquals(int[][] expected, int[][] actual) {
    assertEquals("graph sizes differ", expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals("difference at ord=" + i, expected[i], actual[i]);
    }
  }

  private int[][] getIndexedGraph(float[][] values, int mergePoint, long seed) throws IOException {
    HnswGraphBuilder.randSeed = seed;
    int[][] graph;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setMergePolicy(new LogDocMergePolicy()); // for predictable segment ordering when merging
      iwc.setCodec(Codec.forName("Lucene90")); // don't use SimpleTextCodec
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < values.length; i++) {
          add(iw, i, values[i]);
          if (i == mergePoint) {
            // flush proactively to create a segment
            iw.flush();
          }
        }
        iw.forceMerge(1);
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        Lucene90VectorReader vectorReader =
            ((Lucene90VectorReader) ((CodecReader) getOnlyLeafReader(reader)).getVectorReader());
        graph = copyGraph(vectorReader.getGraphValues(KNN_GRAPH_FIELD));
      }
    }
    return graph;
  }

  private float[][] randomVectors(int numDoc, int dimension) {
    float[][] values = new float[numDoc][];
    for (int i = 0; i < numDoc; i++) {
      if (random().nextBoolean()) {
        values[i] = new float[dimension];
        for (int j = 0; j < dimension; j++) {
          values[i][j] = random().nextFloat();
        }
        VectorUtil.l2normalize(values[i]);
      }
    }
    return values;
  }

  int[][] copyGraph(KnnGraphValues values) throws IOException {
    int size = values.size();
    int[][] graph = new int[size][];
    int[] scratch = new int[HnswGraphBuilder.DEFAULT_MAX_CONN];
    for (int node = 0; node < size; node++) {
      int n, count = 0;
      values.seek(node);
      while ((n = values.nextNeighbor()) != NO_MORE_DOCS) {
        scratch[count++] = n;
        // graph[node][i++] = n;
      }
      graph[node] = ArrayUtil.copyOfSubArray(scratch, 0, count);
    }
    return graph;
  }

  /** Verify that searching does something reasonable */
  public void testSearch() throws Exception {
    // We can't use dot product here since the vectors are laid out on a grid, not a sphere.
    searchStrategy = SearchStrategy.EUCLIDEAN_HNSW;
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig())) {
      // Add a document for every cartesian point in an NxN square so we can
      // easily know which are the nearest neighbors to every point. Insert by iterating
      // using a prime number that is not a divisor of N*N so that we will hit each point once,
      // and chosen so that points will be inserted in a deterministic
      // but somewhat distributed pattern
      int n = 5, stepSize = 17;
      float[][] values = new float[n * n][];
      int index = 0;
      for (int i = 0; i < values.length; i++) {
        // System.out.printf("%d: (%d, %d)\n", i, index % n, index / n);
        int x = index % n, y = index / n;
        values[i] = new float[] {x, y};
        index = (index + stepSize) % (n * n);
        add(iw, i, values[i]);
        if (i == 13) {
          // create 2 segments
          iw.commit();
        }
      }
      boolean forceMerge = random().nextBoolean();
      // System.out.println("");
      if (forceMerge) {
        iw.forceMerge(1);
      }
      assertConsistentGraph(iw, values);
      try (DirectoryReader dr = DirectoryReader.open(iw)) {
        // results are ordered by score (descending) and docid (ascending);
        // This is the insertion order:
        // column major, origin at upper left
        //  0 15  5 20 10
        //  3 18  8 23 13
        //  6 21 11  1 16
        //  9 24 14  4 19
        // 12  2 17  7 22

        /* For this small graph the "search" is exhaustive, so this mostly tests the APIs, the
         * orientation of the various priority queues, the scoring function, but not so much the
         * approximate KNN search algorithm
         */
        assertGraphSearch(new int[] {0, 15, 3, 18, 5}, new float[] {0f, 0.1f}, dr);
        // Tiebreaking by docid must be done after VectorValues.search.
        // assertGraphSearch(new int[]{11, 1, 8, 14, 21}, new float[]{2, 2}, dr);
        assertGraphSearch(new int[] {15, 18, 0, 3, 5}, new float[] {0.3f, 0.8f}, dr);
      }
    }
  }

  private void assertGraphSearch(int[] expected, float[] vector, IndexReader reader)
      throws IOException {
    TopDocs results = doKnnSearch(reader, vector, 5);
    for (ScoreDoc doc : results.scoreDocs) {
      // map docId to insertion id
      doc.doc = Integer.parseInt(reader.document(doc.doc).get("id"));
    }
    assertResults(expected, results);
  }

  private static TopDocs doKnnSearch(IndexReader reader, float[] vector, int k) throws IOException {
    TopDocs[] results = new TopDocs[reader.leaves().size()];
    for (LeafReaderContext ctx : reader.leaves()) {
      results[ctx.ord] = ctx.reader().getVectorValues(KNN_GRAPH_FIELD).search(vector, k, 10);
      if (ctx.docBase > 0) {
        for (ScoreDoc doc : results[ctx.ord].scoreDocs) {
          doc.doc += ctx.docBase;
        }
      }
    }
    return TopDocs.merge(k, results);
  }

  private void assertResults(int[] expected, TopDocs results) {
    assertEquals(results.toString(), expected.length, results.scoreDocs.length);
    for (int i = expected.length - 1; i >= 0; i--) {
      assertEquals(Arrays.toString(results.scoreDocs), expected[i], results.scoreDocs[i].doc);
    }
  }

  // For each leaf, verify that its graph nodes are 1-1 with vectors, that the vectors are the
  // expected values,
  // and that the graph is fully connected and symmetric.
  // NOTE: when we impose max-fanout on the graph it wil no longer be symmetric, but should still
  // be fully connected. Is there any other invariant we can test? Well, we can check that max
  // fanout
  // is respected. We can test *desirable* properties of the graph like small-world (the graph
  // diameter
  // should be tightly bounded).
  private void assertConsistentGraph(IndexWriter iw, float[][] values) throws IOException {
    int totalGraphDocs = 0;
    try (DirectoryReader dr = DirectoryReader.open(iw)) {
      for (LeafReaderContext ctx : dr.leaves()) {
        LeafReader reader = ctx.reader();
        VectorValues vectorValues = reader.getVectorValues(KNN_GRAPH_FIELD);
        Lucene90VectorReader vectorReader =
            ((Lucene90VectorReader) ((CodecReader) reader).getVectorReader());
        if (vectorReader == null) {
          continue;
        }
        KnnGraphValues graphValues = vectorReader.getGraphValues(KNN_GRAPH_FIELD);
        assertEquals((vectorValues == null), (graphValues == null));
        if (vectorValues == null) {
          continue;
        }
        int[][] graph = new int[reader.maxDoc()][];
        boolean foundOrphan = false;
        int graphSize = 0;
        for (int i = 0; i < reader.maxDoc(); i++) {
          int nextDocWithVectors = vectorValues.advance(i);
          // System.out.println("advanced to " + nextDocWithVectors);
          while (i < nextDocWithVectors && i < reader.maxDoc()) {
            int id = Integer.parseInt(reader.document(i).get("id"));
            assertNull("document " + id + " has no vector, but was expected to", values[id]);
            ++i;
          }
          if (nextDocWithVectors == NO_MORE_DOCS) {
            break;
          }
          int id = Integer.parseInt(reader.document(i).get("id"));
          graphValues.seek(graphSize);
          // documents with KnnGraphValues have the expected vectors
          float[] scratch = vectorValues.vectorValue();
          assertArrayEquals(
              "vector did not match for doc " + i + ", id=" + id + ": " + Arrays.toString(scratch),
              values[id],
              scratch,
              0f);
          // We collect neighbors for analysis below
          List<Integer> friends = new ArrayList<>();
          int arc;
          while ((arc = graphValues.nextNeighbor()) != NO_MORE_DOCS) {
            friends.add(arc);
          }
          if (friends.size() == 0) {
            // System.out.printf("knngraph @%d is singleton (advance returns %d)\n", i,
            // nextWithNeighbors);
            foundOrphan = true;
          } else {
            // NOTE: these friends are dense ordinals, not docIds.
            int[] friendCopy = new int[friends.size()];
            for (int j = 0; j < friends.size(); j++) {
              friendCopy[j] = friends.get(j);
            }
            graph[graphSize] = friendCopy;
            // System.out.printf("knngraph @%d => %s\n", i, Arrays.toString(graph[i]));
          }
          graphSize++;
        }
        assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());
        if (foundOrphan) {
          assertEquals("graph is not fully connected", 1, graphSize);
        } else {
          assertTrue(
              "Graph has " + graphSize + " nodes, but one of them has no neighbors", graphSize > 1);
        }
        if (HnswGraphBuilder.DEFAULT_MAX_CONN > graphSize) {
          // assert that the graph in each leaf is connected
          assertConnected(graph);
        } else {
          // assert that max-connections was respected
          assertMaxConn(graph, HnswGraphBuilder.DEFAULT_MAX_CONN);
        }
        totalGraphDocs += graphSize;
      }
    }
    int expectedCount = 0;
    for (float[] friends : values) {
      if (friends != null) {
        ++expectedCount;
      }
    }
    assertEquals(expectedCount, totalGraphDocs);
  }

  private void assertMaxConn(int[][] graph, int maxConn) {
    for (int[] ints : graph) {
      if (ints != null) {
        assert (ints.length <= maxConn);
        for (int k : ints) {
          assertNotNull(graph[k]);
        }
      }
    }
  }

  private void assertConnected(int[][] graph) {
    // every node in the graph is reachable from every other node
    Set<Integer> visited = new HashSet<>();
    List<Integer> queue = new LinkedList<>();
    int count = 0;
    for (int[] entry : graph) {
      if (entry != null) {
        if (queue.isEmpty()) {
          queue.add(entry[0]); // start from any node
          // System.out.println("start at " + entry[0]);
        }
        ++count;
      }
    }
    while (queue.isEmpty() == false) {
      int i = queue.remove(0);
      assertNotNull("expected neighbors of " + i, graph[i]);
      visited.add(i);
      for (int j : graph[i]) {
        if (visited.contains(j) == false) {
          // System.out.println("  ... " + j);
          queue.add(j);
        }
      }
    }
    for (int i = 0; i < count; i++) {
      assertTrue("Attempted to walk entire graph but never visited " + i, visited.contains(i));
    }
    // we visited each node exactly once
    assertEquals(
        "Attempted to walk entire graph but only visited " + visited.size(), count, visited.size());
  }

  private void add(IndexWriter iw, int id, float[] vector) throws IOException {
    add(iw, id, vector, searchStrategy);
  }

  private void add(IndexWriter iw, int id, float[] vector, SearchStrategy searchStrategy)
      throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new VectorField(KNN_GRAPH_FIELD, vector, searchStrategy));
    }
    String idString = Integer.toString(id);
    doc.add(new StringField("id", idString, Field.Store.YES));
    doc.add(new SortedDocValuesField("id", new BytesRef(idString)));
    // XSSystem.out.println("add " + idString + " " + Arrays.toString(vector));
    iw.updateDocument(new Term("id", idString), doc);
  }
}
