package org.apache.lucene.codecs;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

// prototype streaming DV api
public abstract class SimpleDVConsumer implements Closeable {
  // TODO: are any of these params too "infringing" on codec?
  // we want codec to get necessary stuff from IW, but trading off against merge complexity.

  // nocommit should we pass SegmentWriteState...?
  public abstract NumericDocValuesConsumer addNumericField(FieldInfo field, long minValue, long maxValue) throws IOException;
  public abstract BinaryDocValuesConsumer addBinaryField(FieldInfo field, boolean fixedLength, int maxLength) throws IOException;
  // nocommit: figure out whats fair here.
  public abstract SortedDocValuesConsumer addSortedField(FieldInfo field, int valueCount, boolean fixedLength, int maxLength) throws IOException;

  // nocommit bogus forceNorms param:
  public void merge(MergeState mergeState, boolean forceNorms) throws IOException {
    for (FieldInfo field : mergeState.fieldInfos) {
      if ((!forceNorms && field.hasDocValues()) || (forceNorms && field.isIndexed() && !field.omitsNorms())) {
        mergeState.fieldInfo = field;
        //System.out.println("merge field=" + field.name + " forceNorms=" + forceNorms);
        // nocommit a field can never have doc values AND norms!?
        DocValues.Type type = forceNorms ? DocValues.Type.FIXED_INTS_8 : field.getDocValuesType();
        switch(type) {
          case VAR_INTS:
          case FIXED_INTS_8:
          case FIXED_INTS_16:
          case FIXED_INTS_32:
          case FIXED_INTS_64:
          case FLOAT_64:
          case FLOAT_32:
            mergeNumericField(mergeState, forceNorms);
            break;
          case BYTES_VAR_SORTED:
          case BYTES_FIXED_SORTED:
          case BYTES_VAR_DEREF:
          case BYTES_FIXED_DEREF:
            mergeSortedField(mergeState);
            break;
          case BYTES_VAR_STRAIGHT:
          case BYTES_FIXED_STRAIGHT:
            mergeBinaryField(mergeState);
            break;
          default:
            throw new AssertionError();
        }
      }
    }
  }

  // nocommit bogus forceNorms:
  // dead simple impl: codec can optimize
  protected void mergeNumericField(MergeState mergeState, boolean forceNorms) throws IOException {
    // first compute min and max value of live ones to be merged.
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    for (AtomicReader reader : mergeState.readers) {
      final int maxDoc = reader.maxDoc();
      final Bits liveDocs = reader.getLiveDocs();
      //System.out.println("merge field=" + mergeState.fieldInfo.name);
      NumericDocValues docValues = forceNorms ? reader.simpleNormValues(mergeState.fieldInfo.name) : reader.getNumericDocValues(mergeState.fieldInfo.name);
      if (docValues == null) {
        // nocommit this isn't correct i think?  ie this one
        // segment may have no docs containing this
        // field... and that doesn't mean norms are omitted ...
        //assert !forceNorms;
        docValues = new NumericDocValues.EMPTY(maxDoc);
      }
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs == null || liveDocs.get(i)) {
          long val = docValues.get(i);
          minValue = Math.min(val, minValue);
          maxValue = Math.max(val, maxValue);
        }
        mergeState.checkAbort.work(300);
      }
    }
    // now we can merge
    NumericDocValuesConsumer field = addNumericField(mergeState.fieldInfo, minValue, maxValue);
    field.merge(mergeState, forceNorms);
  }
  
  // dead simple impl: codec can optimize
  protected void mergeBinaryField(MergeState mergeState) throws IOException {
    // first compute fixedLength and maxLength of live ones to be merged.
    // nocommit: messy, and can be simplified by using docValues.maxLength/fixedLength in many cases.
    boolean fixedLength = true;
    int maxLength = -1;
    BytesRef bytes = new BytesRef();
    for (AtomicReader reader : mergeState.readers) {
      final int maxDoc = reader.maxDoc();
      final Bits liveDocs = reader.getLiveDocs();
      BinaryDocValues docValues = reader.getBinaryDocValues(mergeState.fieldInfo.name);
      if (docValues == null) {
        docValues = new BinaryDocValues.EMPTY(maxDoc);
      }
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs == null || liveDocs.get(i)) {
          docValues.get(i, bytes);
          if (maxLength == -1) {
            maxLength = bytes.length;
          } else {
            fixedLength &= bytes.length == maxLength;
            maxLength = Math.max(bytes.length, maxLength);
          }
        }
        mergeState.checkAbort.work(300);
      }
    }
    // now we can merge
    assert maxLength >= 0; // could this happen (nothing to do?)
    BinaryDocValuesConsumer field = addBinaryField(mergeState.fieldInfo, fixedLength, maxLength);
    field.merge(mergeState);
  }

  protected void mergeSortedField(MergeState mergeState) throws IOException {
    SortedDocValuesConsumer.Merger merger = new SortedDocValuesConsumer.Merger();
    merger.merge(mergeState);
    SortedDocValuesConsumer consumer = addSortedField(mergeState.fieldInfo, merger.numMergedTerms, merger.fixedLength >= 0, merger.maxLength);
    consumer.merge(mergeState, merger);
  }
}
