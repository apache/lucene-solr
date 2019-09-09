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
import java.nio.BufferUnderflowException;
import java.nio.FloatBuffer;
import java.util.Locale;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.VectorDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase;

import org.junit.Ignore;

/** Tests VectorDocValues */
public class TestVectorDocValues extends LuceneTestCase {

  /** 
   * Basic test of creating indexing and retrieving instances of vector doc values.
   */
  public void testVectorField() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null))) {
      add(iw, 1, 2, 3);
      try (DirectoryReader dr = DirectoryReader.open(iw)) {
        LeafReader r = getOnlyLeafReader(dr);

        // ok
        assertNotNull(DocValues.getBinary(r, "foo"));
        assertNotNull(VectorDocValues.get(r, "foo"));

        // errors
        expectThrows(IllegalStateException.class, () -> {
          DocValues.getNumeric(r, "foo");
        });
        expectThrows(IllegalStateException.class, () -> {
          DocValues.getSorted(r, "foo");
        });
        expectThrows(IllegalStateException.class, () -> {
          DocValues.getSortedSet(r, "foo");
        });
      }
    }
  }

  public void testDimensions() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null))) {
      add(iw, 1, 2, 3);
      iw.addDocument(new Document());
      add(iw, -1, 0, 1);
      add(iw, 0);
      add(iw, 0, 0, 0, 0);
      float[] vector = new float[3];
      try (DirectoryReader dr = DirectoryReader.open(iw)) {
        LeafReader r = getOnlyLeafReader(dr);
        VectorDocValues values = VectorDocValues.get(r, "foo");
        assertEquals(0, values.nextDoc());
        assertEquals(3, values.dimension());
        values.vector(vector);
        assertArrayEquals(new float[]{1, 2, 3}, vector, 0);
        // we skip doc 1, which had no values
        assertEquals(2, values.nextDoc());
        assertEquals(3, values.dimension());
        values.vector(vector);
        assertArrayEquals(new float[]{-1, 0, 1}, vector, 0);
        values.nextDoc();
        expectThrows(BufferUnderflowException.class, () -> values.vector(vector));
        // We ignore extra dimensions, but we should not
        values.nextDoc();
        values.vector(vector);
        assertArrayEquals(new float[]{0, 0, 0}, vector, 0);
      }
    }
  }

  /*
  private void assertArrayEquals(float[] expected, float[] actual, float delta) {
    assertEquals("lengths differ", expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals("mismatch at index " + i, actual[i], expected[i], delta);
    }
    }*/

  public void testAdvance() throws Exception {
    // We override advanceExact() and advance() to decode values and set the next value state: make sure that works
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null))) {
      add(iw, 1, 2, 3);
      iw.addDocument(new Document());
      iw.addDocument(new Document());
      add(iw, -1, 0, 1);
      iw.addDocument(new Document());
      float[] vector = new float[3];
      try (DirectoryReader dr = DirectoryReader.open(iw)) {
        LeafReader r = getOnlyLeafReader(dr);
        VectorDocValues values = VectorDocValues.get(r, "foo");
        // in the initial state we do no checking
        // expectThrows(ArrayIndexOutOfBoundsException.class, () -> values.vector(vector));
        // no values, but dimension is known
        assertEquals(3, values.dimension());
        // after successful advanceExact
        assertTrue(values.advanceExact(0));
        values.vector(vector);
        assertArrayEquals(new float[]{1, 2, 3}, vector, 0);
        // after unsuccessful advanceExact we do not check
        // assertFalse(values.advanceExact(1));
        // expectThrows(AssertionError.class, () -> values.vector(vector));
        // after successful advance
        assertEquals(3, values.advance(2));
        values.vector(vector);
        assertArrayEquals(new float[]{-1, 0, 1}, vector, 0);
        // after unsuccessful advance (no more docs)
        // assertEquals(DocValuesIterator.NO_MORE_DOCS, values.advance(4));
        // expectThrows(ArrayIndexOutOfBoundsException.class, () -> values.vector(vector));
      }
    }
  }

  public void testZeroLengthVector() throws Exception {
    // We disallow 0-length vector values
    expectThrows(IllegalArgumentException.class, () -> new VectorDocValuesField("foo", new float[0]));
  }

  private void add(IndexWriter iw, float ... values) throws IOException {
    Document doc = new Document();
    doc.add(new VectorDocValuesField("foo", values));
    iw.addDocument(doc);
  }

  // TODO: test performance of writing/reading and compare with an implementation that uses memory-mapped I/O to directly map an array of floats
  // TODO: implement vector-based matching using HNSW

  /*

   testPerf iterate values using nextValue()

   [junit4]   1> 100000 docs, dim=10; write time 219ms, read time 51ms
   [junit4]   1> 100000 docs, dim=10; write time 130ms, read time 56ms
   [junit4]   1> 100000 docs, dim=10; write time 139ms, read time 40ms
   [junit4]   1> 100000 docs, dim=100; write time 1016ms, read time 44ms
   [junit4]   1> 100000 docs, dim=100; write time 982ms, read time 130ms
   [junit4]   1> 100000 docs, dim=100; write time 994ms, read time 46ms
   [junit4]   1> 100000 docs, dim=1000; write time 8449ms, read time 254ms
   [junit4]   1> 100000 docs, dim=1000; write time 8796ms, read time 238ms
   [junit4]   1> 100000 docs, dim=1000; write time 8923ms, read time 230ms

   testPerf direct access to vector()

   [junit4]   1> 100000 docs, dim=10; write time 142ms, read time 48ms
   [junit4]   1> 100000 docs, dim=10; write time 158ms, read time 44ms
   [junit4]   1> 100000 docs, dim=10; write time 133ms, read time 40ms
   [junit4]   1> 100000 docs, dim=100; write time 909ms, read time 37ms
   [junit4]   1> 100000 docs, dim=100; write time 893ms, read time 37ms
   [junit4]   1> 100000 docs, dim=100; write time 898ms, read time 39ms
   [junit4]   1> 100000 docs, dim=1000; write time 8696ms, read time 154ms
   [junit4]   1> 100000 docs, dim=1000; write time 8689ms, read time 153ms
   [junit4]   1> 100000 docs, dim=1000; write time 8641ms, read time 149ms

   testPerf provide vector() using direct access to IndexInput and its ByteBuffer
            to avoid copying bytes, still copying floats into array

   [junit4]   1> 100000 docs, dim=10; write time 177ms, read time 45ms
   [junit4]   1> 100000 docs, dim=10; write time 151ms, read time 36ms
   [junit4]   1> 100000 docs, dim=10; write time 158ms, read time 29ms
   [junit4]   1> 100000 docs, dim=100; write time 917ms, read time 41ms
   [junit4]   1> 100000 docs, dim=100; write time 943ms, read time 39ms
   [junit4]   1> 100000 docs, dim=100; write time 927ms, read time 39ms
   [junit4]   1> 100000 docs, dim=1000; write time 9182ms, read time 131ms
   [junit4]   1> 100000 docs, dim=1000; write time 8855ms, read time 141ms
   [junit4]   1> 100000 docs, dim=1000; write time 8847ms, read time 128ms

   testPerf provide vectorBuffer() using direct access to IndexInput and its ByteBuffer
            repeated calls to VectorBuffer.get(int) ... 
   [junit4]   1> 100000 docs, dim=10; write time 168ms, read time 53ms
   [junit4]   1> 100000 docs, dim=10; write time 177ms, read time 32ms
   [junit4]   1> 100000 docs, dim=10; write time 158ms, read time 38ms
   [junit4]   1> 100000 docs, dim=100; write time 947ms, read time 48ms
   [junit4]   1> 100000 docs, dim=100; write time 918ms, read time 44ms
   [junit4]   1> 100000 docs, dim=100; write time 915ms, read time 42ms
   [junit4]   1> 100000 docs, dim=1000; write time 8894ms, read time 202ms
   [junit4]   1> 100000 docs, dim=1000; write time 8792ms, read time 199ms
   [junit4]   1> 100000 docs, dim=1000; write time 8941ms, read time 200ms

   testPerf SlicedVectorValues (plumbing through IndexInput, iterating and converting from int bits to float)
   [junit4]   1> 100000 docs, dim=10; write time 165ms, read time 44ms
   [junit4]   1> 100000 docs, dim=10; write time 138ms, read time 42ms
   [junit4]   1> 100000 docs, dim=10; write time 132ms, read time 37ms
   [junit4]   1> 100000 docs, dim=100; write time 931ms, read time 59ms
   [junit4]   1> 100000 docs, dim=100; write time 940ms, read time 61ms
   [junit4]   1> 100000 docs, dim=100; write time 926ms, read time 60ms
   [junit4]   1> 100000 docs, dim=1000; write time 8977ms, read time 624ms
   [junit4]   1> 100000 docs, dim=1000; write time 9003ms, read time 623ms
   [junit4]   1> 100000 docs, dim=1000; write time 8958ms, read time 624ms

   testRawPerf (Just an in-memory float array, not Lucene; as a best-case)

   [junit4]   1> 100000 docs, dim=10; write time 39ms, read time 0ms
   [junit4]   1> 100000 docs, dim=10; write time 41ms, read time 0ms
   [junit4]   1> 100000 docs, dim=10; write time 39ms, read time 0ms
   [junit4]   1> 100000 docs, dim=100; write time 392ms, read time 7ms
   [junit4]   1> 100000 docs, dim=100; write time 392ms, read time 7ms
   [junit4]   1> 100000 docs, dim=100; write time 392ms, read time 6ms
   [junit4]   1> 100000 docs, dim=1000; write time 3767ms, read time 70ms
   [junit4]   1> 100000 docs, dim=1000; write time 3770ms, read time 70ms
   [junit4]   1> 100000 docs, dim=1000; write time 3769ms, read time 70ms

  */
  
  @Ignore
  public void testPerf() throws Exception {
    // Write lots of vectors, then read them back
    //    int numDocs = atLeast(100_000);
    int numDocs = 100_000;
    // int iters = random().nextInt(5);
    int iters = 4;
    //perfTest(2, 5, 1);
    perfTest(numDocs, 10, iters);
    perfTest(numDocs, 100, iters);
    perfTest(numDocs, 1000, iters);
  }

  private void perfTest(int numDocs, int dimension, int iters) throws IOException {
    String field = "field";
    for (int iter = 0; iter < iters; iter++) {
      try (Directory dir = FSDirectory.open(createTempDir());
           IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig())) {
        float[] vector = new float[dimension];
        float[] sum = new float[dimension];
        long tStart = System.nanoTime();
        for (int i = 0; i < numDocs; i++) {
          for (int j = 0; j < dimension; j++) {
            vector[j] = random().nextFloat();
            sum[j] += vector[j];
          }
          /*
          for (int j = 0; j < dimension; j++) {
            System.out.print(vector[j] + " ");
          }
          System.out.println(" @" + i);
          */
          Document doc = new Document();
          doc.add(new VectorDocValuesField(field, vector));
          iw.addDocument(doc);
        }
        long tWrite = System.nanoTime();
        for (int j = 0; j < dimension; j++) {
          vector[j] = 0;
        }
        try (DirectoryReader dr = DirectoryReader.open(iw)) {
          for (LeafReaderContext lrc : dr.leaves()) {
            VectorDocValues vdv = VectorDocValues.get(lrc.reader(), field);
            //VectorDocValues vdv = VectorDocValues.getSliced(lrc.reader(), field);
            float[] values = new float[dimension];
            while (vdv.nextDoc() != DocValuesIterator.NO_MORE_DOCS) {
              vdv.vector(values);
              for (int i = 0; i < dimension; i++) {
                vector[i] += values[i];
              }
              /*
              FloatBuffer buf = vdv.vectorBuffer();
              int pos = buf.position();
              for (int i = 0, j = pos; i < dimension; i++, j++) {
                // System.out.print(buf.get(j) + " ");
                vector[i] += buf.get(j);
              }
              */
              /*
              System.out.println("");
              */
            }
          }
        }
        if (iter != 0) {
          long tRead = System.nanoTime();
          System.out.printf(Locale.ROOT,
                            "%d docs, dim=%d; write time %dms, read time %dms\n",
                            numDocs, dimension,
                            nsToMs(tWrite - tStart),
                            nsToMs(tRead - tWrite));
        }
        for (int i = 0; i < vector.length; i++) {
          assertEquals("wrong value for dimension " + i, sum[i], vector[i], 0);
        }
      }
    }
  }

  /*
  public void testRawPerf() throws Exception {
    int numDocs = 100000;
    int iters = 4;
    doRawPerf(numDocs, 10, iters);
    doRawPerf(numDocs, 100, iters);
    doRawPerf(numDocs, 1000, iters);
  }
  */
  
  public void doRawPerf(int numDocs, int dimension, int iters) throws Exception {
    float[] buffer = new float[dimension * numDocs];
    for (int iter = 0; iter < iters; iter++) {
      long tStart = System.nanoTime();
      for (int i = 0; i < numDocs * dimension; i++) {
        buffer[i++] = random().nextFloat();
      }
      long tWrite = System.nanoTime();
      float[] vector = new float[dimension];
      for (int i = 0; i < numDocs; i++) {
        for (int j = 0, k = i * dimension; j < dimension; j++, k++) {
          vector[j] += buffer[k];
        }
      }
      if (iter != 0) {
        long tRead = System.nanoTime();
        System.out.printf("%d docs, dim=%d; write time %dms, read time %dms\n",
                          numDocs, dimension,
                          nsToMs(tWrite - tStart),
                          nsToMs(tRead - tWrite));
      }
    } 
  }

  private static long nsToMs(long ns) {
    return ns / 1_000_000;
  }
}
