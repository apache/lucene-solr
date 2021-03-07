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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestPooledDocValuesReader extends LuceneTestCase {

  public void testNumeric() throws IOException {

    Directory dir = newDirectory();
    RandomIndexWriter w =  new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      if (rarely() == false && i > 1) {
        doc.add(new NumericDocValuesField("numeric1", random().nextLong()));
        int count = atLeast(3);
        for (int j = 0; j < count; j++) {
          doc.add(new SortedNumericDocValuesField("multinumeric1", random().nextLong()));
        }
      }
      doc.add(new NumericDocValuesField("numeric2", i));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    for (LeafReaderContext c : r.leaves()) {
      PooledDocValuesReader pool = new PooledDocValuesReader(c, f -> f.endsWith("numeric1"));
      LeafReaderContext pooledContext = pool.getPooledContext();
      assertEquals(pooledContext.docBase, c.docBase);
      assertEquals(pooledContext.ord, c.ord);

      NumericDocValues control = c.reader().getNumericDocValues("numeric1");
      NumericDocValues pooled1 = pooledContext.reader().getNumericDocValues("numeric1");
      NumericDocValues pooled2 = pooledContext.reader().getNumericDocValues("numeric1");
      NumericDocValues secondary = pooledContext.reader().getNumericDocValues("numeric2");
      NumericDocValues multi1 = SortedNumericSelector.wrap(
          pooledContext.reader().getSortedNumericDocValues("multinumeric1"),
          SortedNumericSelector.Type.MAX, SortField.Type.LONG);
      NumericDocValues multi2 = SortedNumericSelector.wrap(
          pooledContext.reader().getSortedNumericDocValues("multinumeric1"),
          SortedNumericSelector.Type.MAX, SortField.Type.LONG);
      NumericDocValues multicontrol = SortedNumericSelector.wrap(
          c.reader().getSortedNumericDocValues("multinumeric1"),
          SortedNumericSelector.Type.MAX, SortField.Type.LONG);

      assertSame(pooled1, pooled2);
      assertNull(pooledContext.reader().getNumericDocValues("numeric3"));
      assertNull(pooledContext.reader().getSortedSetDocValues("numeric1"));

      secondary.nextDoc();  // shouldn't throw an exception because unpooled

      for (int i = 0; i < c.reader().maxDoc(); i++) {
        if (rarely()) {
          continue; // skip a value
        }
        final int doc = i;
        pool.advanceAll(i);
        boolean present = control.advanceExact(i);
        if (present) {
          assertEquals(i, pooled1.advance(i));
          assertEquals(i, pooled2.advance(i));
          assertEquals(control.longValue(), pooled1.longValue());
          assertEquals(control.longValue(), pooled2.longValue());
        }
        else {
          assertFalse(pooled1.advanceExact(i));
          assertFalse(pooled2.advanceExact(i));
        }
        expectThrows(IllegalStateException.class, () -> pooled1.advanceExact(doc + 1));
        expectThrows(IllegalStateException.class, () -> pooled1.advance(doc + 1));
        expectThrows(IllegalStateException.class, pooled1::nextDoc);
        present = multicontrol.advanceExact(i);
        if (present) {
          assertEquals(i, multi1.advance(i));
          assertEquals(i, multi2.advance(i));
          assertEquals(multicontrol.longValue(), multi1.longValue());
          assertEquals(multicontrol.longValue(), multi2.longValue());
        }
        else {
          assertFalse(multi1.advanceExact(i));
          assertFalse(multi2.advanceExact(i));
        }
        expectThrows(IllegalStateException.class, () -> multi1.advanceExact(doc + 1));
        expectThrows(IllegalStateException.class, () -> multi1.advance(doc + 1));
        expectThrows(IllegalStateException.class, multi1::nextDoc);
      }
    }

    r.close();
    w.close();
    dir.close();

  }

  public void testSortedNumeric() throws IOException {

    Directory dir = newDirectory();
    RandomIndexWriter w =  new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      if (rarely() == false && i > 0) {
        int valueCount = atLeast(1);
        for (int j = 0; j < valueCount; j++) {
          doc.add(new SortedNumericDocValuesField("numeric1", random().nextLong()));
        }
      }
      doc.add(new SortedNumericDocValuesField("numeric2", i));
      doc.add(new SortedNumericDocValuesField("numeric2", i + 1));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    for (LeafReaderContext c : r.leaves()) {
      PooledDocValuesReader pool = new PooledDocValuesReader(c, "numeric1"::equals);
      LeafReaderContext pooledContext = pool.getPooledContext();
      assertEquals(pooledContext.docBase, c.docBase);
      assertEquals(pooledContext.ord, c.ord);

      SortedNumericDocValues control = c.reader().getSortedNumericDocValues("numeric1");
      SortedNumericDocValues pooled1 = pooledContext.reader().getSortedNumericDocValues("numeric1");
      SortedNumericDocValues pooled2 = pooledContext.reader().getSortedNumericDocValues("numeric1");
      SortedNumericDocValues unpooled = pooledContext.reader().getSortedNumericDocValues("numeric2");

      assertNull(pooledContext.reader().getSortedNumericDocValues("numeric3"));
      assertNull(pooledContext.reader().getBinaryDocValues("numeric1"));
      unpooled.nextDoc(); // shouldn't throw an exception because unpooled

      for (int i = 0; i < c.reader().maxDoc(); i++) {
        if (rarely()) {
          continue; // skip a value
        }
        final int doc = i;
        pool.advanceAll(i);
        boolean present = control.advanceExact(i);
        if (present) {
          assertEquals(i, pooled1.advance(i));
          assertEquals(i, pooled2.advance(i));
          assertEquals(control.docValueCount(), pooled1.docValueCount());
          assertEquals(control.docValueCount(), pooled2.docValueCount());
          long[] values = new long[control.docValueCount()];
          for (int j = 0; j < control.docValueCount(); j++) {
            values[j] = control.nextValue();
            assertEquals(values[j], pooled1.nextValue());
          }
          for (int j = 0; j < values.length; j++) {
            assertEquals(values[j], pooled2.nextValue());
          }
          expectThrows(IllegalStateException.class, () -> pooled1.advanceExact(doc + 1));
          expectThrows(IllegalStateException.class, () -> pooled1.advance(doc + 1));
          expectThrows(IllegalStateException.class, pooled1::nextDoc);
        }
        else {
          assertFalse(pooled1.advanceExact(i));
          assertFalse(pooled2.advanceExact(i));
          expectThrows(IllegalStateException.class, () -> pooled1.advance(doc + 1));
          expectThrows(IllegalStateException.class, () -> pooled1.advanceExact(doc + 1));
          expectThrows(IllegalStateException.class, pooled1::nextDoc);
        }
      }
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testBinary() throws IOException {

    Directory dir = newDirectory();
    RandomIndexWriter w =  new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      if (rarely() == false && i > 0) {
        doc.add(new BinaryDocValuesField("binary1", TestUtil.randomBinaryTerm(random())));
        doc.add(new BinaryDocValuesField("binary2", TestUtil.randomBinaryTerm(random())));
      }
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    for (LeafReaderContext c : r.leaves()) {
      PooledDocValuesReader pool = new PooledDocValuesReader(c, "binary1"::equals);
      LeafReaderContext pooledContext = pool.getPooledContext();
      assertEquals(pooledContext.docBase, c.docBase);
      assertEquals(pooledContext.ord, c.ord);

      BinaryDocValues control = c.reader().getBinaryDocValues("binary1");
      BinaryDocValues pooled1 = pooledContext.reader().getBinaryDocValues("binary1");
      BinaryDocValues pooled2 = pooledContext.reader().getBinaryDocValues("binary1");
      BinaryDocValues unpooled = pooledContext.reader().getBinaryDocValues("binary2");

      assertSame(pooled1, pooled2);
      assertNull(pooledContext.reader().getNumericDocValues("binary1"));
      assertNull(pooledContext.reader().getBinaryDocValues("binary3"));

      unpooled.nextDoc();   // shouldn't throw an exception because unpooled

      for (int i = 0; i < c.reader().maxDoc(); i++) {
        if (rarely()) {
          continue; // skip a value
        }
        final int doc = i;
        pool.advanceAll(i);
        boolean present = control.advanceExact(i);
        if (present) {
          assertEquals(i, pooled1.advance(i));
          assertEquals(i, pooled2.advance(i));
          assertEquals(control.binaryValue(), pooled1.binaryValue());
          assertEquals(control.binaryValue(), pooled2.binaryValue());
          expectThrows(IllegalStateException.class, () -> pooled1.advanceExact(doc + 1));
          expectThrows(IllegalStateException.class, () -> pooled1.advance(doc + 1));
          expectThrows(IllegalStateException.class, pooled1::nextDoc);
        }
        else {
          assertFalse(pooled1.advanceExact(i));
          assertFalse(pooled2.advanceExact(i));
          expectThrows(IllegalStateException.class, () -> pooled1.advance(doc + 1));
          expectThrows(IllegalStateException.class, () -> pooled1.advanceExact(doc + 1));
          expectThrows(IllegalStateException.class, pooled1::nextDoc);
        }
      }
    }

    r.close();
    w.close();
    dir.close();

  }

  public void testSorted() throws IOException {

    Directory dir = newDirectory();
    RandomIndexWriter w =  new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      if (rarely() == false && i > 0) {
        doc.add(new SortedDocValuesField("sorted1", TestUtil.randomBinaryTerm(random())));
        doc.add(new SortedDocValuesField("sorted2", TestUtil.randomBinaryTerm(random())));
        int count = atLeast(3);
        for (int j = 0; j < count; j++) {
          doc.add(new SortedSetDocValuesField("multisorted1", TestUtil.randomBinaryTerm(random())));
        }
      }
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    for (LeafReaderContext c : r.leaves()) {
      PooledDocValuesReader pool = new PooledDocValuesReader(c, f -> f.endsWith("sorted1"));
      LeafReaderContext pooledContext = pool.getPooledContext();
      assertEquals(pooledContext.docBase, c.docBase);
      assertEquals(pooledContext.ord, c.ord);

      SortedDocValues control = c.reader().getSortedDocValues("sorted1");
      SortedDocValues pooled1 = pooledContext.reader().getSortedDocValues("sorted1");
      SortedDocValues pooled2 = pooledContext.reader().getSortedDocValues("sorted1");
      SortedDocValues unpooled = pooledContext.reader().getSortedDocValues("sorted2");
      SortedDocValues multicontrol = SortedSetSelector.wrap(
          c.reader().getSortedSetDocValues("multisorted1"), SortedSetSelector.Type.MIDDLE_MAX);
      SortedDocValues multi1 = SortedSetSelector.wrap(
          pooledContext.reader().getSortedSetDocValues("multisorted1"), SortedSetSelector.Type.MIDDLE_MAX);
      SortedDocValues multi2 = SortedSetSelector.wrap(
          pooledContext.reader().getSortedSetDocValues("multisorted1"), SortedSetSelector.Type.MIDDLE_MAX);

      assertSame(pooled1, pooled2);
      assertNull(pooledContext.reader().getBinaryDocValues("sorted3"));
      assertNull(pooledContext.reader().getNumericDocValues("sorted1"));
      assertEquals(control.getValueCount(), pooled1.getValueCount());

      unpooled.nextDoc(); // shouldn't throw an exception because unpooled

      for (int i = 0; i < c.reader().maxDoc(); i++) {
        if (rarely()) {
          continue; // skip a value
        }
        final int doc = i;
        pool.advanceAll(i);
        boolean present = control.advanceExact(i);
        if (present) {
          assertEquals(i, pooled1.advance(i));
          assertEquals(i, pooled2.advance(i));
          assertEquals(control.binaryValue(), pooled1.binaryValue());
          assertEquals(control.binaryValue(), pooled2.binaryValue());
          assertEquals(control.ordValue(), pooled1.ordValue());
          assertEquals(control.ordValue(), pooled2.ordValue());
        } else {
          assertFalse(pooled1.advanceExact(i));
          assertFalse(pooled2.advanceExact(i));
        }
        expectThrows(IllegalStateException.class, () -> pooled1.advance(doc + 1));
        expectThrows(IllegalStateException.class, () -> pooled1.advanceExact(doc + 1));
        expectThrows(IllegalStateException.class, pooled1::nextDoc);
        present = multicontrol.advanceExact(i);
        if (present) {
          assertEquals(i, multi1.advance(i));
          assertEquals(i, multi2.advance(i));
          assertEquals(multicontrol.binaryValue(), multi1.binaryValue());
          assertEquals(multicontrol.binaryValue(), multi2.binaryValue());
          assertEquals(multicontrol.ordValue(), multi1.ordValue());
          assertEquals(multicontrol.ordValue(), multi2.ordValue());
        }
        else {
          assertFalse(multi1.advanceExact(i));
          assertFalse(multi2.advanceExact(i));
        }
        expectThrows(IllegalStateException.class, () -> multi1.advance(doc + 1));
        expectThrows(IllegalStateException.class, () -> multi1.advanceExact(doc + 1));
        expectThrows(IllegalStateException.class, multi1::nextDoc);
      }
    }

    r.close();
    w.close();
    dir.close();

  }

  public void testSortedSet() throws IOException {

    Directory dir = newDirectory();
    RandomIndexWriter w =  new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      if (rarely() == false && i > 0) {
        int valueCount = atLeast(1);
        for (int j = 0; j < valueCount; j++) {
          doc.add(new SortedSetDocValuesField("sorted1", TestUtil.randomBinaryTerm(random())));
          doc.add(new SortedSetDocValuesField("sorted2", TestUtil.randomBinaryTerm(random())));
        }
      }
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    for (LeafReaderContext c : r.leaves()) {
      PooledDocValuesReader pool = new PooledDocValuesReader(c, "sorted1"::equals);
      LeafReaderContext pooledContext = pool.getPooledContext();
      assertEquals(pooledContext.docBase, c.docBase);
      assertEquals(pooledContext.ord, c.ord);

      SortedSetDocValues control = c.reader().getSortedSetDocValues("sorted1");
      SortedSetDocValues pooled1 = pooledContext.reader().getSortedSetDocValues("sorted1");
      SortedSetDocValues pooled2 = pooledContext.reader().getSortedSetDocValues("sorted1");

      assertEquals(control.getValueCount(), pooled1.getValueCount());
      assertEquals(control.getValueCount(), pooled2.getValueCount());

      assertNull(pooledContext.reader().getNumericDocValues("sorted1"));
      assertNull(pooledContext.reader().getSortedSetDocValues("sorted3"));

      SortedSetDocValues unpooled = pooledContext.reader().getSortedSetDocValues("sorted2");
      unpooled.nextDoc(); // shouldn't throw an exception because it's unpooled

      for (int i = 0; i < c.reader().maxDoc(); i++) {
        if (rarely()) {
          continue; // skip a value
        }
        final int doc = i;
        pool.advanceAll(i);
        boolean present = control.advanceExact(i);
        if (present) {
          assertEquals(i, pooled1.advance(i));
          assertEquals(i, pooled2.advance(i));
          long[] values = new long[4];
          int count = 0;
          for (long ord = control.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = control.nextOrd()) {
            values = ArrayUtil.grow(values, count + 1);
            values[count] = ord;
            count++;
            assertEquals(ord, pooled1.nextOrd());
          }
          for (int j = 0; j < count; j++) {
            assertEquals(values[j], pooled2.nextOrd());
          }
          expectThrows(IllegalStateException.class, () -> pooled1.advanceExact(doc + 1));
          expectThrows(IllegalStateException.class, () -> pooled1.advance(doc + 1));
          expectThrows(IllegalStateException.class, pooled1::nextDoc);
        }
        else {
          assertFalse(pooled1.advanceExact(i));
          assertFalse(pooled2.advanceExact(i));
          expectThrows(IllegalStateException.class, () -> pooled1.advance(doc + 1));
          expectThrows(IllegalStateException.class, () -> pooled1.advanceExact(doc + 1));
          expectThrows(IllegalStateException.class, pooled1::nextDoc);
        }
      }
    }

    r.close();
    w.close();
    dir.close();

  }

}
