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
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.Random;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Tests HNSW KNN graphs */
public class HnswTest extends LuceneTestCase {

    // test writing out and reading in a graph gives the same graph
    public void testReadWrite() throws IOException {
        int dim = random().nextInt(100) + 1;
        int nDoc = random().nextInt(100) + 1;
        RandomVectorValues vectors = new RandomVectorValues(nDoc, dim, random());
        RandomVectorValues v2 = vectors.copy(), v3 = vectors.copy();
        long seed = random().nextLong();
        HnswGraphBuilder.randSeed = seed;
        HnswGraph hnsw = HnswGraphBuilder.build(vectors);
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
                    doc.add(new VectorField("field", v2.vectorValue(), v2.scoreFunction));
                    doc.add(new StoredField("id", v2.docID()));
                    iw.addDocument(doc);
                    nVec++;
                    indexedDoc++;
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    VectorValues values = ctx.reader().getVectorValues("field");
                    assertEquals(vectors.scoreFunction, values.scoreFunction());
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
        VectorValues vectors = new CircularVectorValues(nDoc);
        HnswGraph hnsw = HnswGraphBuilder.build(vectors);
        // run some searches
        Neighbors nn = HnswGraph.search(new float[]{1, 0}, 10, 5, vectors.randomAccess(), hnsw.getGraphValues(), random());
        int sum = 0;
        for (Neighbor n : nn) {
            sum += n.node;
        }
        // We expect to get approximately 100% recall; the lowest docIds are closest to zero; sum(0,9) = 45
        assertTrue("sum(result docs)=" + sum, sum < 75);
    }

    /** Returns vectors evenly distributed around the unit circle.
     */
    class CircularVectorValues extends VectorValues implements VectorValues.RandomAccess {
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
        public ScoreFunction scoreFunction() {
            return ScoreFunction.DOT_PRODUCT;
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
        public RandomAccess randomAccess() {
            return this;
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
        public TopDocs search(float[] target, int k, int fanout) throws IOException {
            return null;
        }

    }

    private void assertGraphEqual(KnnGraphValues g, KnnGraphValues h, int size) throws IOException {
        for (int node = 0; node < size; node ++) {
            g.seek(node);
            h.seek(node);
            for (int arc = g.nextArc(); arc != NO_MORE_DOCS; arc = g.nextArc()) {
                assertEquals("arcs differ for node " + node, arc, h.nextArc());
            }
        }
    }

    private void assertVectorsEqual(VectorValues u, VectorValues v) throws IOException {
        int uDoc, vDoc = -1;
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
        Neighbors nn = Neighbors.create(2, false);
        Neighbor a = new Neighbor(1, 10);
        Neighbor b = new Neighbor(2, 20);
        Neighbor c = new Neighbor(3, 30);
        assertNull(nn.insertWithOverflow(b));
        assertNull(nn.insertWithOverflow(a));
        assertSame(a, nn.insertWithOverflow(c));
        assertEquals(20, (int) nn.top().score);
        assertEquals(20, (int) nn.pop().score);
        assertEquals(30, (int) nn.top().score);
        assertEquals(30, (int) nn.pop().score);

        Neighbors fn = Neighbors.create(2, true);
        assertNull(fn.insertWithOverflow(b));
        assertNull(fn.insertWithOverflow(a));
        assertSame(c, fn.insertWithOverflow(c));
        assertEquals(20, (int) fn.top().score);
        assertEquals(20, (int) fn.pop().score);
        assertEquals(10, (int) fn.top().score);
        assertEquals(10, (int) fn.pop().score);
    }

    @SuppressWarnings("SelfComparison")
    public void testNeighbor() {
        Neighbor a = new Neighbor(1, 10);
        Neighbor b = new Neighbor(2, 20);
        Neighbor c = new Neighbor(3, 20);
        assertEquals(0, a.compareTo(a));
        assertEquals(-1, a.compareTo(b));
        assertEquals(1, b.compareTo(a));
        assertEquals(1, b.compareTo(c));
        assertEquals(-1, c.compareTo(b));
    }

    private static float[] randomVector(Random random, int dim) {
        float[] vec = new float[dim];
        for (int i = 0; i < dim; i++) {
            vec[i] = random.nextFloat();
        }
        return vec;
    }

    /**
     * Produces random vectors and caches them for random-access.
     */
    class RandomVectorValues extends VectorValues implements VectorValues.RandomAccess {

        private final int dimension;
        private final float[][] denseValues;
        private final float[][] values;
        private final ScoreFunction scoreFunction;

        final int numVectors;
        final int maxDoc;

        private int pos = -1;

        RandomVectorValues(int size, int dimension, Random random) {
            this.dimension = dimension;
            values = new float[size][];
            denseValues = new float[size][];
            int sz = 0;
            int md = -1;
            for (int offset = 0; offset < size; offset += random.nextInt(3) + 1) {
                values[offset] = randomVector(random, dimension);
                denseValues[sz++] = values[offset];
                md = offset;
            }
            numVectors = sz;
            maxDoc = md;
            scoreFunction = ScoreFunction.fromId(random.nextInt(2) + 1);
        }

        private RandomVectorValues(int dimension, ScoreFunction scoreFunction, float[][] denseValues, float[][] values, int size) {
            this.dimension = dimension;
            this.scoreFunction = scoreFunction;
            this.values = values;
            this.denseValues = denseValues;
            numVectors = size;
            maxDoc = values.length - 1;
        }

        public RandomVectorValues copy() {
            return new RandomVectorValues(dimension, scoreFunction, denseValues, values, numVectors);
        }

        @Override
        public int size() {
            return numVectors;
        }

        @Override
        public ScoreFunction scoreFunction() {
            return scoreFunction;
        }

        @Override
        public int dimension() {
            return dimension;
        }

        @Override
        public float[] vectorValue() {
            return values[pos];
        }

        @Override
        public RandomAccess randomAccess() {
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

}
