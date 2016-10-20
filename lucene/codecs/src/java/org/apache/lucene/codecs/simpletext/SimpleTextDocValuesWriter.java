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
package org.apache.lucene.codecs.simpletext;


import java.io.IOException;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;

class SimpleTextDocValuesWriter extends DocValuesConsumer {
  final static BytesRef END     = new BytesRef("END");
  final static BytesRef FIELD   = new BytesRef("field ");
  final static BytesRef TYPE    = new BytesRef("  type ");
  // used for numerics
  final static BytesRef MINVALUE = new BytesRef("  minvalue ");
  final static BytesRef PATTERN  = new BytesRef("  pattern ");
  // used for bytes
  final static BytesRef LENGTH = new BytesRef("length ");
  final static BytesRef MAXLENGTH = new BytesRef("  maxlength ");
  // used for sorted bytes
  final static BytesRef NUMVALUES = new BytesRef("  numvalues ");
  final static BytesRef ORDPATTERN = new BytesRef("  ordpattern ");
  
  IndexOutput data;
  final BytesRefBuilder scratch = new BytesRefBuilder();
  final int numDocs;
  private final Set<String> fieldsSeen = new HashSet<>(); // for asserting
  
  public SimpleTextDocValuesWriter(SegmentWriteState state, String ext) throws IOException {
    // System.out.println("WRITE: " + IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ext) + " " + state.segmentInfo.maxDoc() + " docs");
    data = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ext), state.context);
    numDocs = state.segmentInfo.maxDoc();
  }

  // for asserting
  private boolean fieldSeen(String field) {
    assert !fieldsSeen.contains(field): "field \"" + field + "\" was added more than once during flush";
    fieldsSeen.add(field);
    return true;
  }

  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    assert fieldSeen(field.name);
    assert field.getDocValuesType() == DocValuesType.NUMERIC || field.hasNorms();
    writeFieldEntry(field, DocValuesType.NUMERIC);

    // first pass to find min/max
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    NumericDocValues values = valuesProducer.getNumeric(field);
    int numValues = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      long v = values.longValue();
      minValue = Math.min(minValue, v);
      maxValue = Math.max(maxValue, v);
      numValues++;
    }
    if (numValues != numDocs) {
      minValue = Math.min(minValue, 0);
      maxValue = Math.max(maxValue, 0);
    }
    
    // write our minimum value to the .dat, all entries are deltas from that
    SimpleTextUtil.write(data, MINVALUE);
    SimpleTextUtil.write(data, Long.toString(minValue), scratch);
    SimpleTextUtil.writeNewline(data);
    
    // build up our fixed-width "simple text packed ints"
    // format
    BigInteger maxBig = BigInteger.valueOf(maxValue);
    BigInteger minBig = BigInteger.valueOf(minValue);
    BigInteger diffBig = maxBig.subtract(minBig);
    int maxBytesPerValue = diffBig.toString().length();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < maxBytesPerValue; i++) {
      sb.append('0');
    }
    
    // write our pattern to the .dat
    SimpleTextUtil.write(data, PATTERN);
    SimpleTextUtil.write(data, sb.toString(), scratch);
    SimpleTextUtil.writeNewline(data);

    final String patternString = sb.toString();
    
    final DecimalFormat encoder = new DecimalFormat(patternString, new DecimalFormatSymbols(Locale.ROOT));
    
    int numDocsWritten = 0;

    // second pass to write the values
    values = valuesProducer.getNumeric(field);
    for (int i = 0; i < numDocs; ++i) {
      if (values.docID() < i) {
        values.nextDoc();
        assert values.docID() >= i;
      }
      long value = values.docID() != i ? 0 : values.longValue();
      assert value >= minValue;
      Number delta = BigInteger.valueOf(value).subtract(BigInteger.valueOf(minValue));
      String s = encoder.format(delta);
      assert s.length() == patternString.length();
      SimpleTextUtil.write(data, s, scratch);
      SimpleTextUtil.writeNewline(data);
      if (values.docID() != i) {
        SimpleTextUtil.write(data, "F", scratch);
      } else {
        SimpleTextUtil.write(data, "T", scratch);
      }
      SimpleTextUtil.writeNewline(data);
      numDocsWritten++;
      assert numDocsWritten <= numDocs;
    }

    assert numDocs == numDocsWritten: "numDocs=" + numDocs + " numDocsWritten=" + numDocsWritten;
  }

  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    assert fieldSeen(field.name);
    assert field.getDocValuesType() == DocValuesType.BINARY;
    doAddBinaryField(field, valuesProducer);
  }

  private void doAddBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    int maxLength = 0;
    BinaryDocValues values = valuesProducer.getBinary(field);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      maxLength = Math.max(maxLength, values.binaryValue().length);
    }
    writeFieldEntry(field, DocValuesType.BINARY);

    // write maxLength
    SimpleTextUtil.write(data, MAXLENGTH);
    SimpleTextUtil.write(data, Integer.toString(maxLength), scratch);
    SimpleTextUtil.writeNewline(data);
    
    int maxBytesLength = Long.toString(maxLength).length();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < maxBytesLength; i++) {
      sb.append('0');
    }
    // write our pattern for encoding lengths
    SimpleTextUtil.write(data, PATTERN);
    SimpleTextUtil.write(data, sb.toString(), scratch);
    SimpleTextUtil.writeNewline(data);
    final DecimalFormat encoder = new DecimalFormat(sb.toString(), new DecimalFormatSymbols(Locale.ROOT));

    values = valuesProducer.getBinary(field);
    int numDocsWritten = 0;
    for (int i = 0; i < numDocs; ++i) {
      if (values.docID() < i) {
        values.nextDoc();
        assert values.docID() >= i;
      }
      // write length
      final int length = values.docID() != i ? 0 : values.binaryValue().length;
      SimpleTextUtil.write(data, LENGTH);
      SimpleTextUtil.write(data, encoder.format(length), scratch);
      SimpleTextUtil.writeNewline(data);
        
      // write bytes -- don't use SimpleText.write
      // because it escapes:
      if (values.docID() == i) {
        BytesRef value = values.binaryValue();
        data.writeBytes(value.bytes, value.offset, value.length);
      }

      // pad to fit
      for (int j = length; j < maxLength; j++) {
        data.writeByte((byte)' ');
      }
      SimpleTextUtil.writeNewline(data);
      if (values.docID() != i) {
        SimpleTextUtil.write(data, "F", scratch);
      } else {
        SimpleTextUtil.write(data, "T", scratch);
      }
      SimpleTextUtil.writeNewline(data);
      numDocsWritten++;
    }

    assert numDocs == numDocsWritten;
  }
  
  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    assert fieldSeen(field.name);
    assert field.getDocValuesType() == DocValuesType.SORTED;
    writeFieldEntry(field, DocValuesType.SORTED);

    int valueCount = 0;
    int maxLength = -1;
    TermsEnum terms = valuesProducer.getSorted(field).termsEnum();
    for(BytesRef value = terms.next(); value != null; value = terms.next()) {
      maxLength = Math.max(maxLength, value.length);
      valueCount++;
    }

    // write numValues
    SimpleTextUtil.write(data, NUMVALUES);
    SimpleTextUtil.write(data, Integer.toString(valueCount), scratch);
    SimpleTextUtil.writeNewline(data);
    
    // write maxLength
    SimpleTextUtil.write(data, MAXLENGTH);
    SimpleTextUtil.write(data, Integer.toString(maxLength), scratch);
    SimpleTextUtil.writeNewline(data);
    
    int maxBytesLength = Integer.toString(maxLength).length();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < maxBytesLength; i++) {
      sb.append('0');
    }
    
    // write our pattern for encoding lengths
    SimpleTextUtil.write(data, PATTERN);
    SimpleTextUtil.write(data, sb.toString(), scratch);
    SimpleTextUtil.writeNewline(data);
    final DecimalFormat encoder = new DecimalFormat(sb.toString(), new DecimalFormatSymbols(Locale.ROOT));
    
    int maxOrdBytes = Long.toString(valueCount+1L).length();
    sb.setLength(0);
    for (int i = 0; i < maxOrdBytes; i++) {
      sb.append('0');
    }
    
    // write our pattern for ords
    SimpleTextUtil.write(data, ORDPATTERN);
    SimpleTextUtil.write(data, sb.toString(), scratch);
    SimpleTextUtil.writeNewline(data);
    final DecimalFormat ordEncoder = new DecimalFormat(sb.toString(), new DecimalFormatSymbols(Locale.ROOT));

    // for asserts:
    int valuesSeen = 0;

    terms = valuesProducer.getSorted(field).termsEnum();
    for(BytesRef value = terms.next(); value != null; value = terms.next()) {
      // write length
      SimpleTextUtil.write(data, LENGTH);
      SimpleTextUtil.write(data, encoder.format(value.length), scratch);
      SimpleTextUtil.writeNewline(data);
        
      // write bytes -- don't use SimpleText.write
      // because it escapes:
      data.writeBytes(value.bytes, value.offset, value.length);

      // pad to fit
      for (int i = value.length; i < maxLength; i++) {
        data.writeByte((byte)' ');
      }
      SimpleTextUtil.writeNewline(data);
      valuesSeen++;
      assert valuesSeen <= valueCount;
    }

    assert valuesSeen == valueCount;

    SortedDocValues values = valuesProducer.getSorted(field);
    for (int i = 0; i < numDocs; ++i) {
      if (values.docID() < i) {
        values.nextDoc();
        assert values.docID() >= i;
      }
      int ord = -1;
      if (values.docID() == i) {
        ord = values.ordValue();
      }
      SimpleTextUtil.write(data, ordEncoder.format(ord+1L), scratch);
      SimpleTextUtil.writeNewline(data);
    }
  }

  @Override
  public void addSortedNumericField(FieldInfo field, final DocValuesProducer valuesProducer) throws IOException {
    assert fieldSeen(field.name);
    assert field.getDocValuesType() == DocValuesType.SORTED_NUMERIC;
    doAddBinaryField(field, new EmptyDocValuesProducer() {
      @Override
      public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
        return new BinaryDocValues() {

          @Override
          public int nextDoc() throws IOException {
            int doc = values.nextDoc();
            setCurrentDoc();
            return doc;
          }

          @Override
          public int docID() {
            return values.docID();
          }

          @Override
          public long cost() {
            return values.cost();
          }

          @Override
          public int advance(int target) throws IOException {
            int doc = values.advance(target);
            setCurrentDoc();
            return doc;
          }

          final StringBuilder builder = new StringBuilder();
          BytesRef binaryValue;

          private void setCurrentDoc() throws IOException {
            if (docID() == NO_MORE_DOCS) {
              return;
            }
            builder.setLength(0);
            for (int i = 0, count = values.docValueCount(); i < count; ++i) {
              if (i > 0) {
                builder.append(',');
              }
              builder.append(Long.toString(values.nextValue()));
            }
            binaryValue = new BytesRef(builder.toString());
          }

          @Override
          public BytesRef binaryValue() throws IOException {
            return binaryValue;
          }
        };
      }
    });
  }

  @Override
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    assert fieldSeen(field.name);
    assert field.getDocValuesType() == DocValuesType.SORTED_SET;
    writeFieldEntry(field, DocValuesType.SORTED_SET);

    long valueCount = 0;
    int maxLength = 0;
    TermsEnum terms = valuesProducer.getSortedSet(field).termsEnum();
    for(BytesRef value = terms.next(); value != null; value = terms.next()) {
      maxLength = Math.max(maxLength, value.length);
      valueCount++;
    }

    // write numValues
    SimpleTextUtil.write(data, NUMVALUES);
    SimpleTextUtil.write(data, Long.toString(valueCount), scratch);
    SimpleTextUtil.writeNewline(data);
    
    // write maxLength
    SimpleTextUtil.write(data, MAXLENGTH);
    SimpleTextUtil.write(data, Integer.toString(maxLength), scratch);
    SimpleTextUtil.writeNewline(data);
    
    int maxBytesLength = Integer.toString(maxLength).length();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < maxBytesLength; i++) {
      sb.append('0');
    }
    
    // write our pattern for encoding lengths
    SimpleTextUtil.write(data, PATTERN);
    SimpleTextUtil.write(data, sb.toString(), scratch);
    SimpleTextUtil.writeNewline(data);
    final DecimalFormat encoder = new DecimalFormat(sb.toString(), new DecimalFormatSymbols(Locale.ROOT));
    
    // compute ord pattern: this is funny, we encode all values for all docs to find the maximum length
    int maxOrdListLength = 0;
    StringBuilder sb2 = new StringBuilder();
    SortedSetDocValues values = valuesProducer.getSortedSet(field);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      sb2.setLength(0);
      for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
        if (sb2.length() > 0) {
          sb2.append(",");
        }
        sb2.append(Long.toString(ord));
      }
      maxOrdListLength = Math.max(maxOrdListLength, sb2.length());
    }
     
    sb2.setLength(0);
    for (int i = 0; i < maxOrdListLength; i++) {
      sb2.append('X');
    }
    
    // write our pattern for ord lists
    SimpleTextUtil.write(data, ORDPATTERN);
    SimpleTextUtil.write(data, sb2.toString(), scratch);
    SimpleTextUtil.writeNewline(data);
    
    // for asserts:
    long valuesSeen = 0;

    terms = valuesProducer.getSortedSet(field).termsEnum();
    for(BytesRef value = terms.next(); value != null; value = terms.next()) {
      // write length
      SimpleTextUtil.write(data, LENGTH);
      SimpleTextUtil.write(data, encoder.format(value.length), scratch);
      SimpleTextUtil.writeNewline(data);
        
      // write bytes -- don't use SimpleText.write
      // because it escapes:
      data.writeBytes(value.bytes, value.offset, value.length);

      // pad to fit
      for (int i = value.length; i < maxLength; i++) {
        data.writeByte((byte)' ');
      }
      SimpleTextUtil.writeNewline(data);
      valuesSeen++;
      assert valuesSeen <= valueCount;
    }

    assert valuesSeen == valueCount;

    values = valuesProducer.getSortedSet(field);
    
    // write the ords for each doc comma-separated
    for (int i = 0; i < numDocs; ++i) {
      if (values.docID() < i) {
        values.nextDoc();
        assert values.docID() >= i;
      }
      sb2.setLength(0);
      if (values.docID() == i) {
        for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
          if (sb2.length() > 0) {
            sb2.append(",");
          }
          sb2.append(Long.toString(ord));
        }
      }
      // now pad to fit: these are numbers so spaces work well. reader calls trim()
      int numPadding = maxOrdListLength - sb2.length();
      for (int j = 0; j < numPadding; j++) {
        sb2.append(' ');
      }
      SimpleTextUtil.write(data, sb2.toString(), scratch);
      SimpleTextUtil.writeNewline(data);
    }
  }

  /** write the header for this field */
  private void writeFieldEntry(FieldInfo field, DocValuesType type) throws IOException {
    SimpleTextUtil.write(data, FIELD);
    SimpleTextUtil.write(data, field.name, scratch);
    SimpleTextUtil.writeNewline(data);
    
    SimpleTextUtil.write(data, TYPE);
    SimpleTextUtil.write(data, type.toString(), scratch);
    SimpleTextUtil.writeNewline(data);
  }
  
  @Override
  public void close() throws IOException {
    if (data != null) {
      boolean success = false;
      try {
        assert !fieldsSeen.isEmpty();
        // TODO: sheisty to do this here?
        SimpleTextUtil.write(data, END);
        SimpleTextUtil.writeNewline(data);
        SimpleTextUtil.writeChecksum(data, scratch);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(data);
        } else {
          IOUtils.closeWhileHandlingException(data);
        }
        data = null;
      }
    }
  }
}
