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

import java.io.IOException;
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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

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
    HnswGraph hnsw = HnswGraphBuilder.build((RandomAccessVectorValuesProducer) vectors);
    // Recreate the graph while indexing with the same random seed and write it out
    HnswGraphBuilder.randSeed = seed;
    try (Directory dir = newDirectory()) {
      int nVec = 0, indexedDoc = 0;
      // Don't merge randomly, create a single segment because we rely on the docid ordering for this test
      IndexWriterConfig iwc = new IndexWriterConfig()
        .setCodec(Codec.forName("Lucene90"));
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
          KnnGraphValues graphValues = ((Lucene90VectorReader) ((CodecReader) ctx.reader()).getVectorReader()).getGraphValues("field");
          assertGraphEqual(hnsw.getGraphValues(), graphValues, nVec);
        }
      }
    }
  }

  // Make sure we actually approximately find the closest k elements. Mostly this is about
  // ensuring that we have all the distance functions, comparators, priority queues and so on
  // oriented in the right directions
  public void testAknn() throws IOException {
    int nDoc = 100;
    RandomAccessVectorValuesProducer vectors = new CircularVectorValues(nDoc);
    HnswGraph hnsw = HnswGraphBuilder.build(vectors);
    // run some searches
    Neighbors nn = HnswGraph.search(new float[]{1, 0}, 10, 5, vectors.randomAccess(), hnsw.getGraphValues(), random());
    int sum = 0;
    Neighbors.NeighborIterator it = nn.iterator();
    for (int node = it.next(); node != NO_MORE_DOCS; node = it.next()) {
      sum += node;
    }
    // We expect to get approximately 100% recall; the lowest docIds are closest to zero; sum(0,9) = 45
    assertTrue("sum(result docs)=" + sum, sum < 75);
  }

  public void testMaxConnections() {
    // verify that maxConnections is observed, and that the retained arcs point to the best-scoring neighbors
    HnswGraph graph = new HnswGraph(1, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW);
    graph.connectNodes(0, 1, 0);
    assertArrayEquals(new int[]{1}, graph.getNeighborNodes(0));
    assertArrayEquals(new int[]{0}, graph.getNeighborNodes(1));
    graph.connectNodes(0, 2, 0.4f);
    assertArrayEquals(new int[]{2}, graph.getNeighborNodes(0));
    assertArrayEquals(new int[]{0}, graph.getNeighborNodes(1));
    assertArrayEquals(new int[]{0}, graph.getNeighborNodes(2));
    graph.connectNodes(2, 3, 0);
    assertArrayEquals(new int[]{2}, graph.getNeighborNodes(0));
    assertArrayEquals(new int[]{0}, graph.getNeighborNodes(1));
    assertArrayEquals(new int[]{0}, graph.getNeighborNodes(2));
    assertArrayEquals(new int[]{2}, graph.getNeighborNodes(3));

    graph = new HnswGraph(1, VectorValues.SearchStrategy.EUCLIDEAN_HNSW);
    graph.connectNodes(0, 1, 1);
    assertArrayEquals(new int[]{1}, graph.getNeighborNodes(0));
    assertArrayEquals(new int[]{0}, graph.getNeighborNodes(1));
    graph.connectNodes(0, 2, 2);
    assertArrayEquals(new int[]{1}, graph.getNeighborNodes(0));
    assertArrayEquals(new int[]{0}, graph.getNeighborNodes(1));
    assertArrayEquals(new int[]{0}, graph.getNeighborNodes(2));
    graph.connectNodes(2, 3, 1);
    assertArrayEquals(new int[]{1}, graph.getNeighborNodes(0));
    assertArrayEquals(new int[]{0}, graph.getNeighborNodes(1));
    assertArrayEquals(new int[]{3}, graph.getNeighborNodes(2));
    assertArrayEquals(new int[]{2}, graph.getNeighborNodes(3));
  }

  /** Returns vectors evenly distributed around the unit circle.
   */
  class CircularVectorValues extends VectorValues implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {
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
      value[0] = (float) Math.cos(Math.PI * ord / (double) size);
      value[1] = (float) Math.sin(Math.PI * ord / (double) size);
      return value;
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

  private void assertGraphEqual(KnnGraphValues g, KnnGraphValues h, int size) throws IOException {
    for (int node = 0; node < size; node ++) {
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
      assertArrayEquals("vectors do not match for doc=" + uDoc, u.vectorValue(), v.vectorValue(), 1e-4f);
    }
  }

  public void testNeighbors() {
    // make sure we have the sign correct
    Neighbors nn = Neighbors.create(2, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW);
    assertTrue(nn.insertWithOverflow(2, 0.5f));
    assertTrue(nn.insertWithOverflow(1, 0.2f));
    assertTrue(nn.insertWithOverflow(3, 1f));
    assertEquals(0.5f, nn.topScore(), 0);
    nn.pop();
    assertEquals(1f, nn.topScore(), 0);
    nn.pop();

    Neighbors fn = Neighbors.create(2, VectorValues.SearchStrategy.EUCLIDEAN_HNSW);
    assertTrue(fn.insertWithOverflow(2, 2));
    assertTrue(fn.insertWithOverflow(1, 1));
    assertFalse(fn.insertWithOverflow(3, 3));
    assertEquals(2f, fn.topScore(), 0);
    fn.pop();
    assertEquals(1f, fn.topScore(), 0);
  }

  private static float[] randomVector(Random random, int dim) {
    float[] vec = new float[dim];
    for (int i = 0; i < dim; i++) {
      vec[i] = random.nextFloat();
    }
    VectorUtil.l2normalize(vec);
    return vec;
  }

  /**
   * Produces random vectors and caches them for random-access.
   */
  class RandomVectorValues extends VectorValues implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {

    private final int dimension;
    private final float[][] denseValues;
    private final float[][] values;
    private final float[] scratch;
    private final SearchStrategy searchStrategy;

    final int numVectors;
    final int maxDoc;

    private int pos = -1;

    RandomVectorValues(int size, int dimension, Random random) {
      this.dimension = dimension;
      values = new float[size][];
      denseValues = new float[size][];
      scratch = new float[dimension];
      int sz = 0;
      int md = -1;
      for (int offset = 0; offset < size; offset += random.nextInt(3) + 1) {
        values[offset] = randomVector(random, dimension);
        denseValues[sz++] = values[offset];
        md = offset;
      }
      numVectors = sz;
      maxDoc = md;
      // get a random SearchStrategy other than NONE (0)
      searchStrategy = SearchStrategy.values()[random.nextInt(SearchStrategy.values().length - 1) + 1];
    }

    private RandomVectorValues(int dimension, SearchStrategy searchStrategy, float[][] denseValues, float[][] values, int size) {
      this.dimension = dimension;
      this.searchStrategy = searchStrategy;
      this.values = values;
      this.denseValues = denseValues;
      scratch = new float[dimension];
      numVectors = size;
      maxDoc = values.length - 1;
    }

    public RandomVectorValues copy() {
      return new RandomVectorValues(dimension, searchStrategy, denseValues, values, numVectors);
    }

    @Override
    public int size() {
      return numVectors;
    }

    @Override
    public SearchStrategy searchStrategy() {
      return searchStrategy;
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public float[] vectorValue() {
      if(random().nextBoolean()) {
        return values[pos];
      } else {
        // Sometimes use the same scratch array repeatedly, mimicing what the codec will do.
        // This should help us catch cases of aliasing where the same VectorValues source is used twice in a
        // single computation.
        System.arraycopy(values[pos], 0, scratch, 0, dimension);
        return scratch;
      }
    }

    @Override
    public RandomAccessVectorValues randomAccess() {
      return copy();
    }

    @Override
    public float[] vectorValue(int targetOrd) {
      return denseValues[targetOrd];
    }

    @Override
    public BytesRef binaryValue(int targetOrd) {
      return null;
    }

    @Override
    public TopDocs search(float[] target, int k, int fanout) {
      return null;
    }

    private boolean seek(int target) {
      if (target >= 0 && target < values.length && values[target] != null) {
        pos = target;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public int docID() {
      return pos;
    }

    @Override
    public int nextDoc() {
      return advance(pos + 1);
    }

    public int advance(int target) {
      while (++pos < values.length) {
        if (seek(pos)) {
          return pos;
        }
      }
      return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return size();
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
    expectThrows(IllegalArgumentException.class, () -> new HnswGraphBuilder(new RandomVectorValues(1, 1, random()), 0, 10, 0));
    expectThrows(IllegalArgumentException.class, () -> new HnswGraphBuilder(new RandomVectorValues(1, 1, random()), 10, 0, 0));
  }

}
