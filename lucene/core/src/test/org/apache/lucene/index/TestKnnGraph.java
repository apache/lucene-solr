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


import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.KnnGraphQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Before;

import static org.apache.lucene.util.hnsw.HNSWGraphWriter.RAND_SEED;

/** Tests indexing of a knn-graph */
public class TestKnnGraph extends LuceneTestCase {

  private static final String KNN_GRAPH_FIELD = "vector";

  @Before
  public void setup() {
    RAND_SEED = random().nextLong();
  }

  /**
   * Basic test of creating documents in a graph
   */
  public void testBasic() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null).setCodec(Codec.forName("Lucene90")))) {
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
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null).setCodec(Codec.forName("Lucene90")))) {
      float[][] values = new float[][]{new float[]{0, 1, 2}};
      add(iw, 0, values[0]);
      assertConsistentGraph(iw, values);
      iw.commit();
      assertConsistentGraph(iw, values);
    }
  }

  public void testSingleDocRecall() throws  Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null).setCodec(Codec.forName("Lucene90")))) {
      float[][] values = new float[][]{new float[]{0, 1, 2}};
      add(iw, 0, values[0]);
      assertConsistentGraph(iw, values);

      iw.commit();
      assertConsistentGraph(iw, values);

      assertRecall(dir, 0, values[0]);
    }
  }

  /**
   * Verify that the graph properties are preserved when merging
   */
  public void testMerge() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null).setCodec(Codec.forName("Lucene90")))) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      float[][] values = new float[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        if (random().nextBoolean()) {
          values[i] = new float[dimension];
          for (int j = 0; j < dimension; j++) {
            values[i][j] = random().nextFloat();
          }
        }
        add(iw, i, values[i]);
        if (random().nextInt(10) == 3) {
          //System.out.println("commit @" + i);
          iw.commit();
        }
      }
      if (random().nextBoolean()) {
        iw.forceMerge(1);
      }
      assertConsistentGraph(iw, values);
    }
  }

  // TODO: testSorted
  // TODO: testDeletions

  /**
   * Verify that searching does something reasonable
   */
  public void testSearch() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null).setCodec(Codec.forName("Lucene90")))) {
      // Add a document for every cartesian point  in an NxN square so we can
      // easily know which are the nearest neighbors to every point. Insert by iterating
      // using a prime number that is not a divisor of N*N so that we will hit each point once,
      // and chosen so that points will be inserted in a deterministic
      // but somewhat distributed pattern
      int n = 5, stepSize = 17;
      float[][] values = new float[n * n][];
      int index = 0;
      for (int i = 0; i < values.length; i++) {
        // System.out.printf("%d: (%d, %d)\n", i, index % n, index / n);
        values[i] = new float[]{index % n, index / n};
        index = (index + stepSize) % (n * n);
        add(iw, i, values[i]);
        if (i == 13) {
          // create 2 segments
          iw.commit();
        }
      }
      //System.out.println("");
      if (random().nextBoolean()) {
        iw.forceMerge(1);
      }
      assertConsistentGraph(iw, values);
      try (DirectoryReader dr = DirectoryReader.open(iw)) {
        IndexSearcher searcher = new IndexSearcher(dr);
        // results are ordered by distance (descending) and docid (ascending);
        // This is the docid ordering:
        // column major, origin at upper left
        //  0 15  5 20 10
        //  3 18  8 23 13
        //  6 21 11  1 16
        //  9 24 14  4 19
        // 12  2 17  7 22

        // For this small graph it seems we can always get exact results with 2 probes
        assertGraphSearch(new int[]{0, 15, 3, 18, 5}, new float[]{0f, 0.1f}, searcher);
        assertGraphSearch(new int[]{11, 1, 8, 14, 21}, new float[]{2, 2}, searcher);
        assertGraphSearch(new int[]{15, 18, 0, 3, 5},new float[]{0.3f, 0.8f}, searcher);
      }
    }
  }

  private void assertGraphSearch(int[] expected, float[] vector, IndexSearcher searcher) throws IOException {
    TopDocs results = searcher.search(new KnnGraphQuery(KNN_GRAPH_FIELD, vector), 5);
    assertResults(expected, results);
  }

  private void assertResults(int[] expected, TopDocs topDocs) {
    String resultString = Arrays.asList(topDocs.scoreDocs).toString();
    assertEquals(resultString, expected.length, topDocs.scoreDocs.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(resultString, expected[i], topDocs.scoreDocs[i].doc);
    }
  }

  private void assertConsistentGraph(IndexWriter iw, float[][] values) throws IOException {
    int totalGraphDocs = 0;
    try (DirectoryReader dr = DirectoryReader.open(iw)) {
      for (LeafReaderContext ctx: dr.leaves()) {
        LeafReader reader = ctx.reader();
        VectorValues vectorValues = reader.getVectorValues(KNN_GRAPH_FIELD);
        KnnGraphValues neighbors = reader.getKnnGraphValues(KNN_GRAPH_FIELD);
        assertTrue((vectorValues == null) == (neighbors == null));
        if (vectorValues == null) {
          continue;
        }
        int[][] graph = new int[reader.maxDoc()][];
        boolean singleNodeGraph = false;
        int graphSize = 0;
        for (int i = 0; i < reader.maxDoc(); i++) {
          int id = Integer.parseInt(reader.document(i).get("id"));
          if (values[id] == null) {
            // documents without KnnGraphValues have no vectors or neighbors
            assertNotEquals("document " + id + " was not expected to have values", i, vectorValues.advance(i));
            assertNotEquals(i, neighbors.advance(i));
          } else {
            ++graphSize;
            // documents with KnnGraphValues have the expected vectors
            int doc = vectorValues.advance(i);
            assertEquals("doc " + i + " with id=" + id + " has no vector value", i, doc);
            float[] scratch = vectorValues.vectorValue();
            assertArrayEquals("vector did not match for doc " + i + ", id=" + id + ": " + Arrays.toString(scratch),
                values[id], scratch, 0f);
            // We collect neighbors for analysis below
            int nextWithNeighbors = neighbors.advance(i);
            if (i == nextWithNeighbors) {
              // FIXME: check every level, not only level 0
              IntsRef friends = neighbors.getFriends(0);
              if (friends.length == 0) {
                //System.out.printf("knngraph @%d is singleton (advance returns %d)\n", i, nextWithNeighbors);
                singleNodeGraph = true;
              } else {
                graph[i] = new int[friends.length];
                System.arraycopy(friends.ints, friends.offset, graph[i], 0, friends.length);
                //System.out.printf("knngraph @%d => %s\n", i, Arrays.toString(graph[i]));
              }
            } else {
              // graph must have a single node
              //System.out.printf("knngraph @%d is singleton (advance returns %d)\n", i, nextWithNeighbors);
              singleNodeGraph = true;
            }
          }
        }
        if (singleNodeGraph) {
          assertEquals("graph is not fully connected", 1, graphSize);
        } else {
          assertTrue("Graph has " + graphSize + " nodes, but one of them has no neighbors", graphSize > 1);
        }
        // assert that the graph in each leaf is connected and undirected (ie links are reciprocated)
        assertReciprocal(graph);
        assertConnected(graph);
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

  private void assertReciprocal(int[][] graph) {
    // The graph is undirected: if a -> b then b -> a.
    for (int i = 0; i < graph.length; i++) {
      if (graph[i] != null) {
        for (int j = 0; j < graph[i].length; j++) {
          int k = graph[i][j];
          assertTrue("" + i + "->" + k + " is not reciprocated", Arrays.binarySearch(graph[k], i) >= 0);
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
          //System.out.println("start at " + entry[0]);
        }
        ++count;
      }
    }
    while(queue.isEmpty() == false) {
      int i = queue.remove(0);
      assertNotNull("expected neighbors of " + i, graph[i]);
      visited.add(i);
      for (int j : graph[i]) {
        if (visited.contains(j) == false) {
          //System.out.println("  ... " + j);
          queue.add(j);
        }
      }
    }
    // we visited each node exactly once
    assertEquals("Attempted to walk entire graph but only visited " + visited.size(), count, visited.size());
  }


  private void add(IndexWriter iw, int id, float[] vector) throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new VectorField(KNN_GRAPH_FIELD, vector, VectorValues.DistanceFunction.EUCLIDEAN));
    }
    doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
    //System.out.println("add " + id + " " + Arrays.toString(vector));
    iw.addDocument(doc);
  }

  private void assertRecall(Directory dir, int expectDocId, float[] value) throws IOException {
    try (IndexReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      KnnGraphQuery query = new KnnGraphQuery(KNN_GRAPH_FIELD, value);
      TopDocs result = searcher.search(query, 1);

      int recallCnt = 0;
      for (LeafReaderContext ctx : reader.leaves()) {
        VectorValues vector = ctx.reader().getVectorValues(KNN_GRAPH_FIELD);
        if (vector.seek(result.scoreDocs[0].doc)) {
          ++recallCnt;
          assertEquals(expectDocId, vector.docID());
          assertEquals(0, Arrays.compare(value, vector.vectorValue()));
        }
      }
      assertEquals(1, recallCnt);
    }
  }

}
