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
package org.apache.lucene.codecs.asserting;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.AssertingLeafReader;
import org.apache.lucene.index.AssertingLeafReader.AssertingRandomAccessOrds;
import org.apache.lucene.index.AssertingLeafReader.AssertingSortedSetDocValues;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.TestUtil;

/**
 * Just like the default but with additional asserts.
 */
public class AssertingDocValuesFormat extends DocValuesFormat {
  private final DocValuesFormat in = TestUtil.getDefaultDocValuesFormat();
  
  public AssertingDocValuesFormat() {
    super("Asserting");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    DocValuesConsumer consumer = in.fieldsConsumer(state);
    assert consumer != null;
    return new AssertingDocValuesConsumer(consumer, state.segmentInfo.maxDoc());
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    assert state.fieldInfos.hasDocValues();
    DocValuesProducer producer = in.fieldsProducer(state);
    assert producer != null;
    return new AssertingDocValuesProducer(producer, state.segmentInfo.maxDoc());
  }
  
  static class AssertingDocValuesConsumer extends DocValuesConsumer {
    private final DocValuesConsumer in;
    private final int maxDoc;
    
    AssertingDocValuesConsumer(DocValuesConsumer in, int maxDoc) {
      this.in = in;
      this.maxDoc = maxDoc;
    }

    @Override
    public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
      int count = 0;
      for (Number v : values) {
        count++;
      }
      assert count == maxDoc;
      TestUtil.checkIterator(values.iterator(), maxDoc, true);
      in.addNumericField(field, values);
    }
    
    @Override
    public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
      int count = 0;
      for (BytesRef b : values) {
        assert b == null || b.isValid();
        count++;
      }
      assert count == maxDoc;
      TestUtil.checkIterator(values.iterator(), maxDoc, true);
      in.addBinaryField(field, values);
    }
    
    @Override
    public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
      int valueCount = 0;
      BytesRef lastValue = null;
      for (BytesRef b : values) {
        assert b != null;
        assert b.isValid();
        if (valueCount > 0) {
          assert b.compareTo(lastValue) > 0;
        }
        lastValue = BytesRef.deepCopyOf(b);
        valueCount++;
      }
      assert valueCount <= maxDoc;
      
      FixedBitSet seenOrds = new FixedBitSet(valueCount);
      
      int count = 0;
      for (Number v : docToOrd) {
        assert v != null;
        int ord = v.intValue();
        assert ord >= -1 && ord < valueCount;
        if (ord >= 0) {
          seenOrds.set(ord);
        }
        count++;
      }
      
      assert count == maxDoc;
      assert seenOrds.cardinality() == valueCount;
      TestUtil.checkIterator(values.iterator(), valueCount, false);
      TestUtil.checkIterator(docToOrd.iterator(), maxDoc, false);
      in.addSortedField(field, values, docToOrd);
    }
    
    @Override
    public void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException {
      long valueCount = 0;
      Iterator<Number> valueIterator = values.iterator();
      for (Number count : docToValueCount) {
        assert count != null;
        assert count.intValue() >= 0;
        valueCount += count.intValue();
        long previous = Long.MIN_VALUE;
        for (int i = 0; i < count.intValue(); i++) {
          assert valueIterator.hasNext();
          Number next = valueIterator.next();
          assert next != null;
          long nextValue = next.longValue();
          assert nextValue >= previous;
          previous = nextValue;
        }
      }
      assert valueIterator.hasNext() == false;
      TestUtil.checkIterator(docToValueCount.iterator(), maxDoc, false);
      TestUtil.checkIterator(values.iterator(), valueCount, false);
      in.addSortedNumericField(field, docToValueCount, values);
    }
    
    @Override
    public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
      long valueCount = 0;
      BytesRef lastValue = null;
      for (BytesRef b : values) {
        assert b != null;
        assert b.isValid();
        if (valueCount > 0) {
          assert b.compareTo(lastValue) > 0;
        }
        lastValue = BytesRef.deepCopyOf(b);
        valueCount++;
      }
      
      int docCount = 0;
      long ordCount = 0;
      LongBitSet seenOrds = new LongBitSet(valueCount);
      Iterator<Number> ordIterator = ords.iterator();
      for (Number v : docToOrdCount) {
        assert v != null;
        int count = v.intValue();
        assert count >= 0;
        docCount++;
        ordCount += count;
        
        long lastOrd = -1;
        for (int i = 0; i < count; i++) {
          Number o = ordIterator.next();
          assert o != null;
          long ord = o.longValue();
          assert ord >= 0 && ord < valueCount;
          assert ord > lastOrd : "ord=" + ord + ",lastOrd=" + lastOrd;
          seenOrds.set(ord);
          lastOrd = ord;
        }
      }
      assert ordIterator.hasNext() == false;
      
      assert docCount == maxDoc;
      assert seenOrds.cardinality() == valueCount;
      TestUtil.checkIterator(values.iterator(), valueCount, false);
      TestUtil.checkIterator(docToOrdCount.iterator(), maxDoc, false);
      TestUtil.checkIterator(ords.iterator(), ordCount, false);
      in.addSortedSetField(field, values, docToOrdCount, ords);
    }
    
    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }
  }
  
  static class AssertingDocValuesProducer extends DocValuesProducer {
    private final DocValuesProducer in;
    private final int maxDoc;
    
    AssertingDocValuesProducer(DocValuesProducer in, int maxDoc) {
      this.in = in;
      this.maxDoc = maxDoc;
      // do a few simple checks on init
      assert toString() != null;
      assert ramBytesUsed() >= 0;
      assert getChildResources() != null;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == DocValuesType.NUMERIC;
      NumericDocValues values = in.getNumeric(field);
      assert values != null;
      return new AssertingLeafReader.AssertingNumericDocValues(values, maxDoc);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == DocValuesType.BINARY;
      BinaryDocValues values = in.getBinary(field);
      assert values != null;
      return new AssertingLeafReader.AssertingBinaryDocValues(values, maxDoc);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == DocValuesType.SORTED;
      SortedDocValues values = in.getSorted(field);
      assert values != null;
      return new AssertingLeafReader.AssertingSortedDocValues(values, maxDoc);
    }
    
    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == DocValuesType.SORTED_NUMERIC;
      SortedNumericDocValues values = in.getSortedNumeric(field);
      assert values != null;
      return new AssertingLeafReader.AssertingSortedNumericDocValues(values, maxDoc);
    }
    
    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == DocValuesType.SORTED_SET;
      SortedSetDocValues values = in.getSortedSet(field);
      assert values != null;
      if (values instanceof RandomAccessOrds) {
        return new AssertingRandomAccessOrds((RandomAccessOrds) values, maxDoc);
      } else {
        return new AssertingSortedSetDocValues(values, maxDoc);
      }
    }
    
    @Override
    public Bits getDocsWithField(FieldInfo field) throws IOException {
      assert field.getDocValuesType() != DocValuesType.NONE;
      Bits bits = in.getDocsWithField(field);
      assert bits != null;
      assert bits.length() == maxDoc;
      return new AssertingLeafReader.AssertingBits(bits);
    }

    @Override
    public void close() throws IOException {
      in.close();
      in.close(); // close again
    }

    @Override
    public long ramBytesUsed() {
      long v = in.ramBytesUsed();
      assert v >= 0;
      return v;
    }

    @Override
    public Collection<Accountable> getChildResources() {
      Collection<Accountable> res = in.getChildResources();
      TestUtil.checkReadOnly(res);
      return res;
    }

    @Override
    public void checkIntegrity() throws IOException {
      in.checkIntegrity();
    }
    
    @Override
    public DocValuesProducer getMergeInstance() throws IOException {
      return new AssertingDocValuesProducer(in.getMergeInstance(), maxDoc);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + in.toString() + ")";
    }
  }
}
