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

package org.apache.lucene.util.hnsw;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene90.Lucene90VectorReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;

/** Tests HNSW KNN graphs */
public class TestHnsw extends LuceneTestCase {

  // test writing out and reading in a graph gives the same graph
  public void testReadWrite() throws IOException {
    int dim = random().nextInt(100) + 1;
    int nDoc = random().nextInt(100) + 1;
    RandomVectorValues vectors = new RandomVectorValues(nDoc, dim, random());
    RandomVectorValues v2 = vectors.copy(), v3 = vectors.copy();
    long seed = random().nextLong();
    HnswGraphBuilder.randSeed = seed;
    HnswGraphBuilder builder = new HnswGraphBuilder(vectors);
    HnswGraph hnsw = builder.build(vectors.randomAccess());
    // Recreate the graph while indexing with the same random seed and write it out
    HnswGraphBuilder.randSeed = seed;
    try (Directory dir = newDirectory()) {
      int nVec = 0, indexedDoc = 0;
      // Don't merge randomly, create a single segment because we rely on the docid ordering for
      // this test
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(Codec.forName("Lucene90"));
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        while (v2.nextDoc() != NO_MORE_DOCS) {
          while (indexedDoc < v2.docID()) {
            // increment docId in the index by adding empty documents
            iw.addDocument(new Document());
            indexedDoc++;
          }
          Document doc = new Document();
          doc.add(new VectorField("field", v2.vectorValue(), v2.searchStrategy));
          doc.add(new StoredField("id", v2.docID()));
          iw.addDocument(doc);
          nVec++;
          indexedDoc++;
        }
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          VectorValues values = ctx.reader().getVectorValues("field");
          assertEquals(vectors.searchStrategy, values.searchStrategy());
          assertEquals(dim, values.dimension());
          assertEquals(nVec, values.size());
          assertEquals(indexedDoc, ctx.reader().maxDoc());
          assertEquals(indexedDoc, ctx.reader().numDocs());
          assertVectorsEqual(v3, values);
          KnnGraphValues graphValues =
              ((Lucene90VectorReader) ((CodecReader) ctx.reader()).getVectorReader())
                  .getGraphValues("field");
          assertGraphEqual(hnsw, graphValues, nVec);
        }
      }
    }
  }

  // Make sure we actually approximately find the closest k elements. Mostly this is about
  // ensuring that we have all the distance functions, comparators, priority queues and so on
  // oriented in the right directions
  public void testAknnDiverse() throws IOException {
    int nDoc = 100;
    RandomAccessVectorValuesProducer vectors = new CircularVectorValues(nDoc);
    HnswGraphBuilder builder = new HnswGraphBuilder(vectors, 16, 100, random().nextInt());
    HnswGraph hnsw = builder.build(vectors.randomAccess());
    // run some searches
    NeighborQueue nn =
        HnswGraph.search(new float[] {1, 0}, 10, 5, vectors.randomAccess(), hnsw, random());
    int sum = 0;
    for (int node : nn.nodes()) {
      sum += node;
    }
    // We expect to get approximately 100% recall; the lowest docIds are closest to zero; sum(0,9) =
    // 45
    assertTrue("sum(result docs)=" + sum, sum < 75);
    for (int i = 0; i < nDoc; i++) {
      NeighborArray neighbors = hnsw.getNeighbors(i);
      int[] nodes = neighbors.node;
      for (int j = 0; j < neighbors.size(); j++) {
        // all neighbors should be valid node ids.
        assertTrue(nodes[j] < nDoc);
      }
    }
  }

  public void testBoundsCheckerMax() {
    BoundsChecker max = BoundsChecker.create(false);
    float f = random().nextFloat() - 0.5f;
    // any float > -MAX_VALUE is in bounds
    assertFalse(max.check(f));
    // f is now the bound (minus some delta)
    max.update(f);
    assertFalse(max.check(f)); // f is not out of bounds
    assertFalse(max.check(f + 1)); // anything greater than f is in bounds
    assertTrue(max.check(f - 1e-5f)); // delta is zero initially
  }

  public void testBoundsCheckerMin() {
    BoundsChecker min = BoundsChecker.create(true);
    float f = random().nextFloat() - 0.5f;
    // any float < MAX_VALUE is in bounds
    assertFalse(min.check(f));
    // f is now the bound (minus some delta)
    min.update(f);
    assertFalse(min.check(f)); // f is not out of bounds
    assertFalse(min.check(f - 1)); // anything less than f is in bounds
    assertTrue(min.check(f + 1e-5f)); // delta is zero initially
  }

  public void testHnswGraphBuilderInvalid() {
    expectThrows(NullPointerException.class, () -> new HnswGraphBuilder(null, 0, 0, 0));
    expectThrows(
        IllegalArgumentException.class,
        () -> new HnswGraphBuilder(new RandomVectorValues(1, 1, random()), 0, 10, 0));
    expectThrows(
        IllegalArgumentException.class,
        () -> new HnswGraphBuilder(new RandomVectorValues(1, 1, random()), 10, 0, 0));
  }

  public void testDiversity() throws IOException {
    // Some carefully checked test cases with simple 2d vectors on the unit circle:
    MockVectorValues vectors =
        new MockVectorValues(
            VectorValues.SearchStrategy.DOT_PRODUCT_HNSW,
            new float[][] {
              unitVector2d(0.5),
              unitVector2d(0.75),
              unitVector2d(0.2),
              unitVector2d(0.9),
              unitVector2d(0.8),
              unitVector2d(0.77),
            });
    // First add nodes until everybody gets a full neighbor list
    HnswGraphBuilder builder = new HnswGraphBuilder(vectors, 2, 10, random().nextInt());
    // node 0 is added by the builder constructor
    // builder.addGraphNode(vectors.vectorValue(0));
    builder.addGraphNode(vectors.vectorValue(1));
    builder.addGraphNode(vectors.vectorValue(2));
    // now every node has tried to attach every other node as a neighbor, but
    // some were excluded based on diversity check.
    assertNeighbors(builder.hnsw, 0, 1, 2);
    assertNeighbors(builder.hnsw, 1, 0);
    assertNeighbors(builder.hnsw, 2, 0);

    builder.addGraphNode(vectors.vectorValue(3));
    assertNeighbors(builder.hnsw, 0, 1, 2);
    // we added 3 here
    assertNeighbors(builder.hnsw, 1, 0, 3);
    assertNeighbors(builder.hnsw, 2, 0);
    assertNeighbors(builder.hnsw, 3, 1);

    // supplant an existing neighbor
    builder.addGraphNode(vectors.vectorValue(4));
    // 4 is the same distance from 0 that 2 is; we leave the existing node in place
    assertNeighbors(builder.hnsw, 0, 1, 2);
    // 4 is closer to 1 than either existing neighbor (0, 3). 3 fails diversity check with 4, so
    // replace it
    assertNeighbors(builder.hnsw, 1, 0, 4);
    assertNeighbors(builder.hnsw, 2, 0);
    // 1 survives the diversity check
    assertNeighbors(builder.hnsw, 3, 1, 4);
    assertNeighbors(builder.hnsw, 4, 1, 3);

    builder.addGraphNode(vectors.vectorValue(5));
    assertNeighbors(builder.hnsw, 0, 1, 2);
    assertNeighbors(builder.hnsw, 1, 0, 5);
    assertNeighbors(builder.hnsw, 2, 0);
    // even though 5 is closer, 3 is not a neighbor of 5, so no update to *its* neighbors occurs
    assertNeighbors(builder.hnsw, 3, 1, 4);
    assertNeighbors(builder.hnsw, 4, 3, 5);
    assertNeighbors(builder.hnsw, 5, 1, 4);
  }

  private void assertNeighbors(HnswGraph graph, int node, int... expected) {
    Arrays.sort(expected);
    NeighborArray nn = graph.getNeighbors(node);
    int[] actual = ArrayUtil.copyOfSubArray(nn.node, 0, nn.size());
    Arrays.sort(actual);
    assertArrayEquals(
        "expected: " + Arrays.toString(expected) + " actual: " + Arrays.toString(actual),
        expected,
        actual);
  }

  public void testRandom() throws IOException {
    int size = atLeast(100);
    int dim = atLeast(10);
    int topK = 5;
    RandomVectorValues vectors = new RandomVectorValues(size, dim, random());
    HnswGraphBuilder builder = new HnswGraphBuilder(vectors, 10, 30, random().nextLong());
    HnswGraph hnsw = builder.build(vectors);
    int totalMatches = 0;
    for (int i = 0; i < 100; i++) {
      float[] query = randomVector(random(), dim);
      NeighborQueue actual = HnswGraph.search(query, topK, 100, vectors, hnsw, random());
      NeighborQueue expected = new NeighborQueue(topK, vectors.searchStrategy.reversed);
      for (int j = 0; j < size; j++) {
        float[] v = vectors.vectorValue(j);
        if (v != null) {
          expected.insertWithOverflow(
              j, vectors.searchStrategy.compare(query, vectors.vectorValue(j)));
        }
      }
      assertEquals(topK, actual.size());
      totalMatches += computeOverlap(actual.nodes(), expected.nodes());
    }
    double overlap = totalMatches / (double) (100 * topK);
    System.out.println("overlap=" + overlap + " totalMatches=" + totalMatches);
    assertTrue("overlap=" + overlap, overlap > 0.9);
  }

  private int computeOverlap(int[] a, int[] b) {
    Arrays.sort(a);
    Arrays.sort(b);
    int overlap = 0;
    for (int i = 0, j = 0; i < a.length && j < b.length; ) {
      if (a[i] == b[j]) {
        ++overlap;
        ++i;
        ++j;
      } else if (a[i] > b[j]) {
        ++j;
      } else {
        ++i;
      }
    }
    return overlap;
  }

  /** Returns vectors evenly distributed around the upper unit semicircle. */
  static class CircularVectorValues extends VectorValues
      implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {
    private final int size;
    private final float[] value;

    int doc = -1;

    CircularVectorValues(int size) {
      this.size = size;
      value = new float[2];
    }

    public CircularVectorValues copy() {
      return new CircularVectorValues(size);
    }

    @Override
    public SearchStrategy searchStrategy() {
      return SearchStrategy.DOT_PRODUCT_HNSW;
    }

    @Override
    public int dimension() {
      return 2;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public float[] vectorValue() {
      return vectorValue(doc);
    }

    @Override
    public RandomAccessVectorValues randomAccess() {
      return new CircularVectorValues(size);
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) {
      if (target >= 0 && target < size) {
        doc = target;
      } else {
        doc = NO_MORE_DOCS;
      }
      return doc;
    }

    @Override
    public long cost() {
      return size;
    }

    @Override
    public float[] vectorValue(int ord) {
      return unitVector2d(ord / (double) size, value);
    }

    @Override
    public BytesRef binaryValue(int ord) {
      return null;
    }

    @Override
    public TopDocs search(float[] target, int k, int fanout) {
      return null;
    }
  }

  private static float[] unitVector2d(double piRadians) {
    return unitVector2d(piRadians, new float[2]);
  }

  private static float[] unitVector2d(double piRadians, float[] value) {
    value[0] = (float) Math.cos(Math.PI * piRadians);
    value[1] = (float) Math.sin(Math.PI * piRadians);
    return value;
  }

  private void assertGraphEqual(KnnGraphValues g, KnnGraphValues h, int size) throws IOException {
    for (int node = 0; node < size; node++) {
      g.seek(node);
      h.seek(node);
      assertEquals("arcs differ for node " + node, getNeighborNodes(g), getNeighborNodes(h));
    }
  }

  private Set<Integer> getNeighborNodes(KnnGraphValues g) throws IOException {
    Set<Integer> neighbors = new HashSet<>();
    for (int n = g.nextNeighbor(); n != NO_MORE_DOCS; n = g.nextNeighbor()) {
      neighbors.add(n);
    }
    return neighbors;
  }

  private void assertVectorsEqual(VectorValues u, VectorValues v) throws IOException {
    int uDoc, vDoc;
    while (true) {
      uDoc = u.nextDoc();
      vDoc = v.nextDoc();
      assertEquals(uDoc, vDoc);
      if (uDoc == NO_MORE_DOCS) {
        break;
      }
      assertArrayEquals(
          "vectors do not match for doc=" + uDoc, u.vectorValue(), v.vectorValue(), 1e-4f);
    }
  }

  /** Produces random vectors and caches them for random-access. */
  static class RandomVectorValues extends MockVectorValues {

    RandomVectorValues(int size, int dimension, Random random) {
      super(
          SearchStrategy.values()[random.nextInt(SearchStrategy.values().length - 1) + 1],
          createRandomVectors(size, dimension, random));
    }

    RandomVectorValues(RandomVectorValues other) {
      super(other.searchStrategy, other.values);
    }

    @Override
    public RandomVectorValues copy() {
      return new RandomVectorValues(this);
    }

    private static float[][] createRandomVectors(int size, int dimension, Random random) {
      float[][] vectors = new float[size][];
      for (int offset = 0; offset < size; offset += random.nextInt(3) + 1) {
        vectors[offset] = randomVector(random, dimension);
      }
      return vectors;
    }
  }

  private static float[] randomVector(Random random, int dim) {
    float[] vec = new float[dim];
    for (int i = 0; i < dim; i++) {
      vec[i] = random.nextFloat();
    }
    VectorUtil.l2normalize(vec);
    return vec;
  }
}
