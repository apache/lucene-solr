package org.apache.lucene.codecs.asserting;

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

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene42.Lucene42DocValuesFormat;
import org.apache.lucene.index.AssertingAtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OpenBitSet;

/**
 * Just like {@link Lucene42DocValuesFormat} but with additional asserts.
 */
public class AssertingDocValuesFormat extends DocValuesFormat {
  private final DocValuesFormat in = new Lucene42DocValuesFormat();
  
  public AssertingDocValuesFormat() {
    super("Asserting");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    DocValuesConsumer consumer = in.fieldsConsumer(state);
    assert consumer != null;
    return new AssertingDocValuesConsumer(consumer, state.segmentInfo.getDocCount());
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    assert state.fieldInfos.hasDocValues();
    DocValuesProducer producer = in.fieldsProducer(state);
    assert producer != null;
    return new AssertingDocValuesProducer(producer, state.segmentInfo.getDocCount());
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
        assert v != null;
        count++;
      }
      assert count == maxDoc;
      checkIterator(values.iterator(), maxDoc);
      in.addNumericField(field, values);
    }
    
    @Override
    public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
      int count = 0;
      for (BytesRef b : values) {
        assert b != null;
        assert b.isValid();
        count++;
      }
      assert count == maxDoc;
      checkIterator(values.iterator(), maxDoc);
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
        assert ord >= 0 && ord < valueCount;
        seenOrds.set(ord);
        count++;
      }
      
      assert count == maxDoc;
      assert seenOrds.cardinality() == valueCount;
      checkIterator(values.iterator(), valueCount);
      checkIterator(docToOrd.iterator(), maxDoc);
      in.addSortedField(field, values, docToOrd);
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
      OpenBitSet seenOrds = new OpenBitSet(valueCount);
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
      checkIterator(values.iterator(), valueCount);
      checkIterator(docToOrdCount.iterator(), maxDoc);
      checkIterator(ords.iterator(), ordCount);
      in.addSortedSetField(field, values, docToOrdCount, ords);
    }

    private <T> void checkIterator(Iterator<T> iterator, long expectedSize) {
      for (long i = 0; i < expectedSize; i++) {
        boolean hasNext = iterator.hasNext();
        assert hasNext;
        T v = iterator.next();
        assert v != null;
        try {
          iterator.remove();
          throw new AssertionError("broken iterator (supports remove): " + iterator);
        } catch (UnsupportedOperationException expected) {
          // ok
        }
      }
      assert !iterator.hasNext();
      try {
        iterator.next();
        throw new AssertionError("broken iterator (allows next() when hasNext==false) " + iterator);
      } catch (NoSuchElementException expected) {
        // ok
      }
    }
    
    @Override
    public void close() throws IOException {
      in.close();
    }
  }
  
  static class AssertingDocValuesProducer extends DocValuesProducer {
    private final DocValuesProducer in;
    private final int maxDoc;
    
    AssertingDocValuesProducer(DocValuesProducer in, int maxDoc) {
      this.in = in;
      this.maxDoc = maxDoc;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == FieldInfo.DocValuesType.NUMERIC || 
             field.getNormType() == FieldInfo.DocValuesType.NUMERIC;
      NumericDocValues values = in.getNumeric(field);
      assert values != null;
      return new AssertingAtomicReader.AssertingNumericDocValues(values, maxDoc);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == FieldInfo.DocValuesType.BINARY;
      BinaryDocValues values = in.getBinary(field);
      assert values != null;
      return new AssertingAtomicReader.AssertingBinaryDocValues(values, maxDoc);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == FieldInfo.DocValuesType.SORTED;
      SortedDocValues values = in.getSorted(field);
      assert values != null;
      return new AssertingAtomicReader.AssertingSortedDocValues(values, maxDoc);
    }
    
    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
      assert field.getDocValuesType() == FieldInfo.DocValuesType.SORTED_SET;
      SortedSetDocValues values = in.getSortedSet(field);
      assert values != null;
      return new AssertingAtomicReader.AssertingSortedSetDocValues(values, maxDoc);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }
}
