package org.apache.lucene.codecs.memory;

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

import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDVProducer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextSimpleDocValuesFormat.SimpleTextDocValuesReader;
import org.apache.lucene.codecs.simpletext.SimpleTextSimpleDocValuesFormat.SimpleTextDocValuesWriter;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;

/** Indexes doc values to disk and loads them in RAM at
 *  search time. */

public class MemoryDocValuesFormat extends SimpleDocValuesFormat {

  public MemoryDocValuesFormat() {
    super("Memory");
  }

  @Override
  public SimpleDVConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    // nocommit use a more efficient format ;):
    return new SimpleTextDocValuesWriter(state, "dat");
  }

  @Override
  public SimpleDVProducer fieldsProducer(SegmentReadState state) throws IOException {
    final SimpleDVProducer producer = new SimpleTextDocValuesReader(state, "dat");

    return new SimpleDVProducer() {

      @Override
      public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        NumericDocValues valuesIn = producer.getNumeric(field);

        // nocommit more ram efficient
        final int maxDoc = valuesIn.size();
        final long minValue = valuesIn.minValue();
        final long maxValue = valuesIn.maxValue();

        final long[] values = new long[maxDoc];
        for(int docID=0;docID<maxDoc;docID++) {
          values[docID] = valuesIn.get(docID);
        }

        return new NumericDocValues() {

          @Override
          public long get(int docID) {
            return values[docID];
          }

          @Override
          public int size() {
            return maxDoc;
          }

          @Override
          public long minValue() {
            return minValue;
          }

          @Override
          public long maxValue() {
            return maxValue;
          }
        };
      }
      
      @Override
      public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        BinaryDocValues valuesIn = producer.getBinary(field);
        final int maxDoc = valuesIn.size();
        final int maxLength = valuesIn.maxLength();
        final boolean fixedLength = valuesIn.isFixedLength();
        // nocommit more ram efficient
        final byte[][] values = new byte[maxDoc][];
        BytesRef scratch = new BytesRef();
        for(int docID=0;docID<maxDoc;docID++) {
          valuesIn.get(docID, scratch);
          values[docID] = new byte[scratch.length];
          System.arraycopy(scratch.bytes, scratch.offset, values[docID], 0, scratch.length);
        }

        return new BinaryDocValues() {

          @Override
          public void get(int docID, BytesRef result) {
            result.bytes = values[docID];
            result.offset = 0;
            result.length = result.bytes.length;
          }

          @Override
          public int size() {
            return maxDoc;
          }

          @Override
          public boolean isFixedLength() {
            return fixedLength;
          }

          @Override
          public int maxLength() {
            return maxLength;
          }
        };
      }

      @Override
      public SortedDocValues getSorted(FieldInfo field) throws IOException {
        SortedDocValues valuesIn = producer.getSorted(field);
        final int maxDoc = valuesIn.size();
        final int maxLength = valuesIn.maxLength();
        final boolean fixedLength = valuesIn.isFixedLength();
        final int valueCount = valuesIn.getValueCount();

        // nocommit used packed ints and so on
        final byte[][] values = new byte[valueCount][];
        BytesRef scratch = new BytesRef();
        for(int ord=0;ord<values.length;ord++) {
          valuesIn.lookupOrd(ord, scratch);
          values[ord] = new byte[scratch.length];
          System.arraycopy(scratch.bytes, scratch.offset, values[ord], 0, scratch.length);
        }

        final int[] docToOrd = new int[maxDoc];
        for(int docID=0;docID<maxDoc;docID++) {
          docToOrd[docID] = valuesIn.getOrd(docID);
        }
        return new SortedDocValues() {

          @Override
          public int getOrd(int docID) {
            return docToOrd[docID];
          }

          @Override
          public void lookupOrd(int ord, BytesRef result) {
            result.bytes = values[ord];
            result.offset = 0;
            result.length = result.bytes.length;
          }

          @Override
          public int getValueCount() {
            return valueCount;
          }

          @Override
          public int size() {
            return maxDoc;
          }

          @Override
          public boolean isFixedLength() {
            return fixedLength;
          }

          @Override
          public int maxLength() {
            return maxLength;
          }
        };
      }

      @Override
      public SimpleDVProducer clone() {
        // We are already thread-safe:
        return this;
      }

      @Override
      public void close() throws IOException {
        producer.close();
      }
    };
  }
}
