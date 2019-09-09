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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnGraphField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.GraphSearch;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Tests indexing of a knn-graph by KnnGraphWriter */
public class TestKnnGraph extends LuceneTestCase {

  private static final String KNN_GRAPH_FIELD = "vector";
  private static final String KNN_GRAPH_NBR_FIELD = "vector$nbr";

  /**
   * Basic test of creating documents in a graph
   */
  public void testBasic() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null))) {
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
      assertConsistentGraph(iw, dimension, values);
    }
  }

  /**
   * Verify that the graph properties are preserved when merging
   */
  public void testMerge() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null))) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      float[][] values = new float[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        if (random().nextBoolean()) {
          values[i] = new float[dimension];
          for (int j = 0; j < dimension; j++) {
            // FIXME why do all the distances look identical?
            values[i][j] = random().nextFloat();
          }
        }
        add(iw, i, values[i]);
        if (random().nextInt(10) == 3) {
          //System.out.println("commit");
          iw.commit();
        }
      }
      if (random().nextBoolean()) {
        iw.forceMerge(1);
      }
      assertConsistentGraph(iw, dimension, values);
    }
  }

  // TODO: testSorted
  // TODO: testDeletions

  /**
   * Verify that searching does something reasonable
   */
  public void testSearch() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null))) {
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
      // TODO: enable this randomness
      if (random().nextBoolean()) {
        iw.forceMerge(1);
      }
      assertConsistentGraph(iw, 2, values);
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
        assertResults(new int[]{11, 1, 8, 14, 21},
            GraphSearch.search(searcher, KNN_GRAPH_FIELD, 5, 2, new float[]{2, 2}));
        assertResults(new int[]{0, 3, 15, 18, 5},
            GraphSearch.search(searcher, KNN_GRAPH_FIELD, 5, 2, new float[]{0, 0}));
        assertResults(new int[]{15, 18, 0, 3, 5},
            GraphSearch.search(searcher, KNN_GRAPH_FIELD, 5, 2, new float[]{0.3f, 0.8f}));
      }
    }
  }

  private void assertResults(int[] expected, TopDocs topDocs) {
    assertEquals(expected.length, topDocs.scoreDocs.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], topDocs.scoreDocs[i].doc);
    }
  }

  private void assertConsistentGraph(IndexWriter iw, int dimension, float[][] values) throws IOException {
    float[] scratch = new float[dimension];
    try (DirectoryReader dr = DirectoryReader.open(iw)) {
      for (LeafReaderContext ctx: dr.leaves()) {
        LeafReader reader = ctx.reader();
        VectorDocValues vectorDocValues = VectorDocValues.get(reader, KNN_GRAPH_FIELD);
        SortedNumericDocValues neighbors = DocValues.getSortedNumeric(reader, KNN_GRAPH_NBR_FIELD);
        int[][] graph = new int[reader.maxDoc()][];
        boolean singleNodeGraph = false;
        int graphSize = 0;
        for (int i = 0; i < reader.maxDoc(); i++) {
          int id = Integer.parseInt(reader.document(i).get("id"));
          if (values[id] == null) {
            // documents without KnnGraphValues have no vectors or neighbors
            assertFalse("document " + id + " was not expected to have values", vectorDocValues.advanceExact(i));
            assertFalse(neighbors.advanceExact(i));
          } else {
            ++graphSize;
            // documents with KnnGraphValues have the expected vectors
            assertTrue("doc " + i + " has no vector value", vectorDocValues.advanceExact(i));
            vectorDocValues.vector(scratch);
            assertArrayEquals(values[id], scratch, 0f);
            // We collect neighbors for analysis below
            if (neighbors.advanceExact(i)) {
              graph[i] = new int[neighbors.docValueCount()];
              for (int j = 0; j < graph[i].length; j++) {
                graph[i][j] = (int) neighbors.nextValue();
                //System.out.println("" + i + " -> " + graph[i][j]);
              }
            } else {
              // graph must have a single node
              singleNodeGraph = true;
            }
          }
        }
        assertTrue(singleNodeGraph || graphSize != 1);
        if (graphSize > 0) {
          assertEquals(dimension, vectorDocValues.dimension());
        }
        // assert that the graph in each leaf is connected and undirected (ie links are reciprocated)
        assertReciprocal(graph);
        assertConnected(graph);
      }
    }
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
    assertEquals(count, visited.size());
  }


  private void add(IndexWriter iw, int id, float[] vector) throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new KnnGraphField(KNN_GRAPH_FIELD, vector));
    }
    doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
    //System.out.println("add " + id + " " + vector);
    iw.addDocument(doc);
  }

}
