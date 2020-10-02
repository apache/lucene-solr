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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.AssertingLeafReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

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
    return new AssertingDocValuesProducer(producer, state.segmentInfo.maxDoc(), false);
  }
  
  static class AssertingDocValuesConsumer extends DocValuesConsumer {
    private final DocValuesConsumer in;
    private final int maxDoc;
    
    AssertingDocValuesConsumer(DocValuesConsumer in, int maxDoc) {
      this.in = in;
      this.maxDoc = maxDoc;
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      NumericDocValues values = valuesProducer.getNumeric(field);

      int docID;
      int lastDocID = -1;
      while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
        assert docID >= 0 && docID < maxDoc;
        assert docID > lastDocID;
        lastDocID = docID;
        long value = values.longValue();
      }
      
      in.addNumericField(field, valuesProducer);
    }
    
    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      BinaryDocValues values = valuesProducer.getBinary(field);
      
      int docID;
      int lastDocID = -1;
      while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
        assert docID >= 0 && docID < maxDoc;
        assert docID > lastDocID;
        lastDocID = docID;
        BytesRef value = values.binaryValue();
        assert value.isValid();
      }

      in.addBinaryField(field, valuesProducer);
    }
    
    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      SortedDocValues values = valuesProducer.getSorted(field);

      int valueCount = values.getValueCount();
      assert valueCount <= maxDoc;
      BytesRef lastValue = null;
      for (int ord=0;ord<valueCount;ord++) {
        BytesRef b = values.lookupOrd(ord);
        assert b != null;
        assert b.isValid();
        if (ord > 0) {
          assert b.compareTo(lastValue) > 0;
        }
        lastValue = BytesRef.deepCopyOf(b);
      }
      
      FixedBitSet seenOrds = new FixedBitSet(valueCount);
      
      int docID;
      int lastDocID = -1;
      while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
        assert docID >= 0 && docID < maxDoc;
        assert docID > lastDocID;
        lastDocID = docID;
        int ord = values.ordValue();
        assert ord >= 0 && ord < valueCount;
        seenOrds.set(ord);
      }
      
      assert seenOrds.cardinality() == valueCount;
      in.addSortedField(field, valuesProducer);
    }
    
    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);

      long valueCount = 0;
      int lastDocID = -1;
      while (true) {
        int docID = values.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        assert values.docID() > lastDocID;
        lastDocID = values.docID();
        int count = values.docValueCount();
        assert count > 0;
        valueCount += count;
        long previous = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
          long nextValue = values.nextValue();
          assert nextValue >= previous;
          previous = nextValue;
        }
      }
      in.addSortedNumericField(field, valuesProducer);
    }
    
    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      SortedSetDocValues values = valuesProducer.getSortedSet(field);

      long valueCount = values.getValueCount();
      BytesRef lastValue = null;
      for (long i=0;i<valueCount;i++) {
        BytesRef b = values.lookupOrd(i);
        assert b != null;
        assert b.isValid();
        if (i > 0) {
          assert b.compareTo(lastValue) > 0;
        }
        lastValue = BytesRef.deepCopyOf(b);
      }
      
      int docCount = 0;
      LongBitSet seenOrds = new LongBitSet(valueCount);
      while (true) {
        int docID = values.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        docCount++;
        
        long lastOrd = -1;
        while (true) {
          long ord = values.nextOrd();
          if (ord == SortedSetDocValues.NO_MORE_ORDS) {
            break;
          }
          assert ord >= 0 && ord < valueCount: "ord=" + ord + " is not in bounds 0 .." + (valueCount-1);
          assert ord > lastOrd : "ord=" + ord + ",lastOrd=" + lastOrd;
          seenOrds.set(ord);
          lastOrd = ord;
        }
      }
      
      assert seenOrds.cardinality() == valueCount;
      in.addSortedSetField(field, valuesProducer);
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
    private final boolean merging;
    private final Thread creationThread;
    
    AssertingDocValuesProducer(DocValuesProducer in, int maxDoc, boolean merging) {
      this.in = in;
      this.maxDoc = maxDoc;
      this.merging = merging;
      this.creationThread = Thread.currentThread();
      // do a few simple checks on init
      assert toString() != null;
      assert ramBytesUsed() >= 0;
      assert getChildResources() != null;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
      if (merging) {
        AssertingCodec.assertThread("DocValuesProducer", creationThread);
      }
      assert field.getDocValuesType() == DocValuesType.NUMERIC;
      NumericDocValues values = in.getNumeric(field);
      assert values != null;
      return new AssertingLeafReader.AssertingNumericDocValues(values, maxDoc);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
      if (merging) {
        AssertingCodec.assertThread("DocValuesProducer", creationThread);
      }
      assert field.getDocValuesType() == DocValuesType.BINARY;
      BinaryDocValues values = in.getBinary(field);
      assert values != null;
      return new AssertingLeafReader.AssertingBinaryDocValues(values, maxDoc);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
      if (merging) {
        AssertingCodec.assertThread("DocValuesProducer", creationThread);
      }
      assert field.getDocValuesType() == DocValuesType.SORTED;
      SortedDocValues values = in.getSorted(field);
      assert values != null;
      return new AssertingLeafReader.AssertingSortedDocValues(values, maxDoc);
    }
    
    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
      if (merging) {
        AssertingCodec.assertThread("DocValuesProducer", creationThread);
      }
      assert field.getDocValuesType() == DocValuesType.SORTED_NUMERIC;
      SortedNumericDocValues values = in.getSortedNumeric(field);
      assert values != null;
      return AssertingLeafReader.AssertingSortedNumericDocValues.create(values, maxDoc);
    }
    
    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
      if (merging) {
        AssertingCodec.assertThread("DocValuesProducer", creationThread);
      }
      assert field.getDocValuesType() == DocValuesType.SORTED_SET;
      SortedSetDocValues values = in.getSortedSet(field);
      assert values != null;
      return AssertingLeafReader.AssertingSortedSetDocValues.create(values, maxDoc);
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
    public DocValuesProducer getMergeInstance() {
      return new AssertingDocValuesProducer(in.getMergeInstance(), maxDoc, true);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + in.toString() + ")";
    }
  }
}
