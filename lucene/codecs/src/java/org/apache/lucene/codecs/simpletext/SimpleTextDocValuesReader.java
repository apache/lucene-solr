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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesWriter.END;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesWriter.FIELD;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesWriter.LENGTH;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesWriter.MAXLENGTH;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesWriter.MINVALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesWriter.NUMVALUES;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesWriter.ORDPATTERN;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesWriter.PATTERN;
import static org.apache.lucene.codecs.simpletext.SimpleTextDocValuesWriter.TYPE;

class SimpleTextDocValuesReader extends DocValuesProducer {

  private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(SimpleTextDocValuesReader.class)
      + RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

  static class OneField {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OneField.class);
    long dataStartFilePointer;
    String pattern;
    String ordPattern;
    int maxLength;
    boolean fixedLength;
    long minValue;
    long numValues;
  }

  final int maxDoc;
  final IndexInput data;
  final BytesRefBuilder scratch = new BytesRefBuilder();
  final Map<String,OneField> fields = new HashMap<>();
  
  public SimpleTextDocValuesReader(SegmentReadState state, String ext) throws IOException {
    // System.out.println("dir=" + state.directory + " seg=" + state.segmentInfo.name + " file=" + IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ext));
    data = state.directory.openInput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ext), state.context);
    maxDoc = state.segmentInfo.maxDoc();
    while(true) {
      readLine();
      //System.out.println("READ field=" + scratch.utf8ToString());
      if (scratch.get().equals(END)) {
        break;
      }
      assert startsWith(FIELD) : scratch.get().utf8ToString();
      String fieldName = stripPrefix(FIELD);
      //System.out.println("  field=" + fieldName);

      OneField field = new OneField();
      fields.put(fieldName, field);

      readLine();
      assert startsWith(TYPE) : scratch.get().utf8ToString();

      DocValuesType dvType = DocValuesType.valueOf(stripPrefix(TYPE));
      assert dvType != DocValuesType.NONE;
      if (dvType == DocValuesType.NUMERIC) {
        readLine();
        assert startsWith(MINVALUE): "got " + scratch.get().utf8ToString() + " field=" + fieldName + " ext=" + ext;
        field.minValue = Long.parseLong(stripPrefix(MINVALUE));
        readLine();
        assert startsWith(PATTERN);
        field.pattern = stripPrefix(PATTERN);
        field.dataStartFilePointer = data.getFilePointer();
        data.seek(data.getFilePointer() + (1+field.pattern.length()+2) * maxDoc);
      } else if (dvType == DocValuesType.BINARY) {
        readLine();
        assert startsWith(MAXLENGTH);
        field.maxLength = Integer.parseInt(stripPrefix(MAXLENGTH));
        readLine();
        assert startsWith(PATTERN);
        field.pattern = stripPrefix(PATTERN);
        field.dataStartFilePointer = data.getFilePointer();
        data.seek(data.getFilePointer() + (9+field.pattern.length()+field.maxLength+2) * maxDoc);
      } else if (dvType == DocValuesType.SORTED || dvType == DocValuesType.SORTED_SET) {
        readLine();
        assert startsWith(NUMVALUES);
        field.numValues = Long.parseLong(stripPrefix(NUMVALUES));
        readLine();
        assert startsWith(MAXLENGTH);
        field.maxLength = Integer.parseInt(stripPrefix(MAXLENGTH));
        readLine();
        assert startsWith(PATTERN);
        field.pattern = stripPrefix(PATTERN);
        readLine();
        assert startsWith(ORDPATTERN);
        field.ordPattern = stripPrefix(ORDPATTERN);
        field.dataStartFilePointer = data.getFilePointer();
        data.seek(data.getFilePointer() + (9+field.pattern.length()+field.maxLength) * field.numValues + (1+field.ordPattern.length())*maxDoc);
      } else {
        throw new AssertionError();
      }
    }

    // We should only be called from above if at least one
    // field has DVs:
    assert !fields.isEmpty();
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo fieldInfo) throws IOException {
    IntFunction<Long> values = getNumericNonIterator(fieldInfo);
    if (values == null) {
      return null;
    } else {
      DocValuesIterator docsWithField = getNumericDocsWithField(fieldInfo);
      return new NumericDocValues() {
        
        @Override
        public int nextDoc() throws IOException {
          return docsWithField.nextDoc();
        }
        
        @Override
        public int docID() {
          return docsWithField.docID();
        }
        
        @Override
        public long cost() {
          return docsWithField.cost();
        }
        
        @Override
        public int advance(int target) throws IOException {
          return docsWithField.advance(target);
        }
        
        @Override
        public boolean advanceExact(int target) throws IOException {
          return docsWithField.advanceExact(target);
        }
        
        @Override
        public long longValue() throws IOException {
          return values.apply(docsWithField.docID());
        }
      };
    }
  }
  
  IntFunction<Long> getNumericNonIterator(FieldInfo fieldInfo) throws IOException {
    final OneField field = fields.get(fieldInfo.name);
    assert field != null;

    // SegmentCoreReaders already verifies this field is
    // valid:
    assert field != null: "field=" + fieldInfo.name + " fields=" + fields;

    final IndexInput in = data.clone();
    final BytesRefBuilder scratch = new BytesRefBuilder();
    final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));

    decoder.setParseBigDecimal(true);

    return new IntFunction<Long>() {
      @Override
      public Long apply(int docID) {
        try {
          //System.out.println(Thread.currentThread().getName() + ": get docID=" + docID + " in=" + in);
          if (docID < 0 || docID >= maxDoc) {
            throw new IndexOutOfBoundsException("docID must be 0 .. " + (maxDoc-1) + "; got " + docID);
          }
          in.seek(field.dataStartFilePointer + (1+field.pattern.length()+2)*docID);
          SimpleTextUtil.readLine(in, scratch);
          //System.out.println("parsing delta: " + scratch.utf8ToString());
          BigDecimal bd;
          try {
            bd = (BigDecimal) decoder.parse(scratch.get().utf8ToString());
          } catch (ParseException pe) {
            throw new CorruptIndexException("failed to parse BigDecimal value", in, pe);
          }
          SimpleTextUtil.readLine(in, scratch); // read the line telling us if it's real or not
          return BigInteger.valueOf(field.minValue).add(bd.toBigIntegerExact()).longValue();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    };
  }

  private static abstract class DocValuesIterator extends DocIdSetIterator {
    abstract boolean advanceExact(int target) throws IOException;
  }

  private DocValuesIterator getNumericDocsWithField(FieldInfo fieldInfo) throws IOException {
    final OneField field = fields.get(fieldInfo.name);
    final IndexInput in = data.clone();
    final BytesRefBuilder scratch = new BytesRefBuilder();
    return new DocValuesIterator() {
      
      int doc = -1;
      
      @Override
      public int nextDoc() throws IOException {
        return advance(docID() + 1);
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public long cost() {
        return maxDoc;
      }
      
      @Override
      public int advance(int target) throws IOException {
        for (int i = target; i < maxDoc; ++i) {
          in.seek(field.dataStartFilePointer + (1+field.pattern.length()+2)*i);
          SimpleTextUtil.readLine(in, scratch); // data
          SimpleTextUtil.readLine(in, scratch); // 'T' or 'F'
          if (scratch.byteAt(0) == (byte) 'T') {
            return doc = i;
          }
        }
        return doc = NO_MORE_DOCS;
      }

      @Override
      boolean advanceExact(int target) throws IOException {
        this.doc = target;
        in.seek(field.dataStartFilePointer + (1+field.pattern.length()+2)*target);
        SimpleTextUtil.readLine(in, scratch); // data
        SimpleTextUtil.readLine(in, scratch); // 'T' or 'F'
        return scratch.byteAt(0) == (byte) 'T';
      }
    };
  }
  
  @Override
  public synchronized BinaryDocValues getBinary(FieldInfo fieldInfo) throws IOException {
    final OneField field = fields.get(fieldInfo.name);

    // SegmentCoreReaders already verifies this field is
    // valid:
    assert field != null;

    final IndexInput in = data.clone();
    final BytesRefBuilder scratch = new BytesRefBuilder();
    final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));

    DocValuesIterator docsWithField = getBinaryDocsWithField(fieldInfo);
    
    IntFunction<BytesRef> values = new IntFunction<BytesRef>() {
      final BytesRefBuilder term = new BytesRefBuilder();

      @Override
      public BytesRef apply(int docID) {
        try {
          if (docID < 0 || docID >= maxDoc) {
            throw new IndexOutOfBoundsException("docID must be 0 .. " + (maxDoc-1) + "; got " + docID);
          }
          in.seek(field.dataStartFilePointer + (9+field.pattern.length() + field.maxLength+2)*docID);
          SimpleTextUtil.readLine(in, scratch);
          assert StringHelper.startsWith(scratch.get(), LENGTH);
          int len;
          try {
            len = decoder.parse(new String(scratch.bytes(), LENGTH.length, scratch.length() - LENGTH.length, StandardCharsets.UTF_8)).intValue();
          } catch (ParseException pe) {
            throw new CorruptIndexException("failed to parse int length", in, pe);
          }
          term.grow(len);
          term.setLength(len);
          in.readBytes(term.bytes(), 0, len);
          return term.get();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    };
    return new BinaryDocValues() {
      
      @Override
      public int nextDoc() throws IOException {
        return docsWithField.nextDoc();
      }
      
      @Override
      public int docID() {
        return docsWithField.docID();
      }
      
      @Override
      public long cost() {
        return docsWithField.cost();
      }
      
      @Override
      public int advance(int target) throws IOException {
        return docsWithField.advance(target);
      }
      
      @Override
      public boolean advanceExact(int target) throws IOException {
        return docsWithField.advanceExact(target);
      }
      
      @Override
      public BytesRef binaryValue() throws IOException {
        return values.apply(docsWithField.docID());
      }
    };
  }

  private DocValuesIterator getBinaryDocsWithField(FieldInfo fieldInfo) throws IOException {
    final OneField field = fields.get(fieldInfo.name);
    final IndexInput in = data.clone();
    final BytesRefBuilder scratch = new BytesRefBuilder();
    final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));

    return new DocValuesIterator() {
      
      int doc = -1;
      
      @Override
      public int nextDoc() throws IOException {
        return advance(docID() + 1);
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public long cost() {
        return maxDoc;
      }
      
      @Override
      public int advance(int target) throws IOException {
        for (int i = target; i < maxDoc; ++i) {
          in.seek(field.dataStartFilePointer + (9+field.pattern.length() + field.maxLength+2)*i);
          SimpleTextUtil.readLine(in, scratch);
          assert StringHelper.startsWith(scratch.get(), LENGTH);
          int len;
          try {
            len = decoder.parse(new String(scratch.bytes(), LENGTH.length, scratch.length() - LENGTH.length, StandardCharsets.UTF_8)).intValue();
          } catch (ParseException pe) {
            throw new CorruptIndexException("failed to parse int length", in, pe);
          }
          // skip past bytes
          byte bytes[] = new byte[len];
          in.readBytes(bytes, 0, len);
          SimpleTextUtil.readLine(in, scratch); // newline
          SimpleTextUtil.readLine(in, scratch); // 'T' or 'F'
          if (scratch.byteAt(0) == (byte) 'T') {
            return doc = i;
          }
        }
        return doc = NO_MORE_DOCS;
      }

      @Override
      boolean advanceExact(int target) throws IOException {
        this.doc = target;
        in.seek(field.dataStartFilePointer + (9+field.pattern.length() + field.maxLength+2)*target);
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch.get(), LENGTH);
        int len;
        try {
          len = decoder.parse(new String(scratch.bytes(), LENGTH.length, scratch.length() - LENGTH.length, StandardCharsets.UTF_8)).intValue();
        } catch (ParseException pe) {
          throw new CorruptIndexException("failed to parse int length", in, pe);
        }
        // skip past bytes
        byte bytes[] = new byte[len];
        in.readBytes(bytes, 0, len);
        SimpleTextUtil.readLine(in, scratch); // newline
        SimpleTextUtil.readLine(in, scratch); // 'T' or 'F'
        return scratch.byteAt(0) == (byte) 'T';
      }
    };
  }

  @Override
  public SortedDocValues getSorted(FieldInfo fieldInfo) throws IOException {
    final OneField field = fields.get(fieldInfo.name);

    // SegmentCoreReaders already verifies this field is
    // valid:
    assert field != null;

    final IndexInput in = data.clone();
    final BytesRefBuilder scratch = new BytesRefBuilder();
    final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));
    final DecimalFormat ordDecoder = new DecimalFormat(field.ordPattern, new DecimalFormatSymbols(Locale.ROOT));

    return new SortedDocValues() {

      int doc = -1;

      @Override
      public int nextDoc() throws IOException {
        return advance(docID() + 1);
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public long cost() {
        return maxDoc;
      }

      int ord;

      @Override
      public int advance(int target) throws IOException {
        for (int i = target; i < maxDoc; ++i) {
          in.seek(field.dataStartFilePointer + field.numValues * (9 + field.pattern.length() + field.maxLength) + i * (1 + field.ordPattern.length()));
          SimpleTextUtil.readLine(in, scratch);
          try {
            ord = (int) ordDecoder.parse(scratch.get().utf8ToString()).longValue()-1;
          } catch (ParseException pe) {
            throw new CorruptIndexException("failed to parse ord", in, pe);
          }
          if (ord >= 0) {
            return doc = i;
          }
        }
        return doc = NO_MORE_DOCS;
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        this.doc = target;
        in.seek(field.dataStartFilePointer + field.numValues * (9 + field.pattern.length() + field.maxLength) + target * (1 + field.ordPattern.length()));
        SimpleTextUtil.readLine(in, scratch);
        try {
          ord = (int) ordDecoder.parse(scratch.get().utf8ToString()).longValue()-1;
        } catch (ParseException pe) {
          throw new CorruptIndexException("failed to parse ord", in, pe);
        }
        return ord >= 0;
      }

      @Override
      public int ordValue() {
        return ord;
      }
      
      final BytesRefBuilder term = new BytesRefBuilder();
      
      @Override
      public BytesRef lookupOrd(int ord) throws IOException {
        if (ord < 0 || ord >= field.numValues) {
          throw new IndexOutOfBoundsException("ord must be 0 .. " + (field.numValues-1) + "; got " + ord);
        }
        in.seek(field.dataStartFilePointer + ord * (9 + field.pattern.length() + field.maxLength));
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch.get(), LENGTH): "got " + scratch.get().utf8ToString() + " in=" + in;
        int len;
        try {
          len = decoder.parse(new String(scratch.bytes(), LENGTH.length, scratch.length() - LENGTH.length, StandardCharsets.UTF_8)).intValue();
        } catch (ParseException pe) {
          throw new CorruptIndexException("failed to parse int length", in, pe);
        }
        term.grow(len);
        term.setLength(len);
        in.readBytes(term.bytes(), 0, len);
        return term.get();
      }
      
      @Override
      public int getValueCount() {
        return (int)field.numValues;
      }
    };
  }
  
  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    final BinaryDocValues binary = getBinary(field);
    return new SortedNumericDocValues() {
      
      @Override
      public int nextDoc() throws IOException {
        int doc = binary.nextDoc();
        setCurrentDoc();
        return doc;
      }
      
      @Override
      public int docID() {
        return binary.docID();
      }
      
      @Override
      public long cost() {
        return binary.cost();
      }
      
      @Override
      public int advance(int target) throws IOException {
        int doc = binary.advance(target);
        setCurrentDoc();
        return doc;
      }
      
      @Override
      public boolean advanceExact(int target) throws IOException {
        if (binary.advanceExact(target)) {
          setCurrentDoc();
          return true;
        }
        return false;
      }
      
      long values[];
      int index;
      
      private void setCurrentDoc() throws IOException {
        if (docID() == NO_MORE_DOCS) {
          return;
        }
        String csv = binary.binaryValue().utf8ToString();
        if (csv.length() == 0) {
          values = new long[0];
        } else {
          String s[] = csv.split(",");
          values = new long[s.length];
          for (int i = 0; i < values.length; i++) {
            values[i] = Long.parseLong(s[i]);
          }
        }
        index = 0;
      }
      
      @Override
      public long nextValue() throws IOException {
        return values[index++];
      }
      
      @Override
      public int docValueCount() {
        return values.length;
      }
    };
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo fieldInfo) throws IOException {
    final OneField field = fields.get(fieldInfo.name);

    // SegmentCoreReaders already verifies this field is
    // valid:
    assert field != null;

    final IndexInput in = data.clone();
    final BytesRefBuilder scratch = new BytesRefBuilder();
    final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));
    
    return new SortedSetDocValues() {
      
      String[] currentOrds = new String[0];
      int currentIndex = 0;
      final BytesRefBuilder term = new BytesRefBuilder();
      int doc = -1;
      
      @Override
      public int nextDoc() throws IOException {
        return advance(doc + 1);
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public long cost() {
        return maxDoc;
      }
      
      @Override
      public int advance(int target) throws IOException {
        for (int i = target; i < maxDoc; ++i) {
          in.seek(field.dataStartFilePointer + field.numValues * (9 + field.pattern.length() + field.maxLength) + i * (1 + field.ordPattern.length()));
          SimpleTextUtil.readLine(in, scratch);
          String ordList = scratch.get().utf8ToString().trim();
          if (ordList.isEmpty() == false) {
            currentOrds = ordList.split(",");
            currentIndex = 0;
            return doc = i;
          }
        }
        return doc = NO_MORE_DOCS;
      }
      
      @Override
      public boolean advanceExact(int target) throws IOException {
        in.seek(field.dataStartFilePointer + field.numValues * (9 + field.pattern.length() + field.maxLength) + target * (1 + field.ordPattern.length()));
        SimpleTextUtil.readLine(in, scratch);
        String ordList = scratch.get().utf8ToString().trim();
        doc = target;
        if (ordList.isEmpty() == false) {
          currentOrds = ordList.split(",");
          currentIndex = 0;
          return true;
        }
        return false;
      }
      
      @Override
      public long nextOrd() throws IOException {
        if (currentIndex == currentOrds.length) {
          return NO_MORE_ORDS;
        } else {
          return Long.parseLong(currentOrds[currentIndex++]);
        }
      }
      
      @Override
      public BytesRef lookupOrd(long ord) throws IOException {
        if (ord < 0 || ord >= field.numValues) {
          throw new IndexOutOfBoundsException("ord must be 0 .. " + (field.numValues-1) + "; got " + ord);
        }
        in.seek(field.dataStartFilePointer + ord * (9 + field.pattern.length() + field.maxLength));
        SimpleTextUtil.readLine(in, scratch);
        assert StringHelper.startsWith(scratch.get(), LENGTH): "got " + scratch.get().utf8ToString() + " in=" + in;
        int len;
        try {
          len = decoder.parse(new String(scratch.bytes(), LENGTH.length, scratch.length() - LENGTH.length, StandardCharsets.UTF_8)).intValue();
        } catch (ParseException pe) {
          throw new CorruptIndexException("failed to parse int length", in, pe);
        }
        term.grow(len);
        term.setLength(len);
        in.readBytes(term.bytes(), 0, len);
        return term.get();
      }
      
      @Override
      public long getValueCount() {
        return field.numValues;
      }
    };
  }
  
  @Override
  public void close() throws IOException {
    data.close();
  }

  /** Used only in ctor: */
  private void readLine() throws IOException {
    SimpleTextUtil.readLine(data, scratch);
    //System.out.println("line: " + scratch.utf8ToString());
  }

  /** Used only in ctor: */
  private boolean startsWith(BytesRef prefix) {
    return StringHelper.startsWith(scratch.get(), prefix);
  }

  /** Used only in ctor: */
  private String stripPrefix(BytesRef prefix) {
    return new String(scratch.bytes(), prefix.length, scratch.length() - prefix.length, StandardCharsets.UTF_8);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(scratch.bytes())
        + fields.size() * (RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2L + OneField.BASE_RAM_BYTES_USED);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + fields.size() + ")";
  }

  @Override
  public void checkIntegrity() throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    IndexInput clone = data.clone();
    clone.seek(0);
    // checksum is fixed-width encoded with 20 bytes, plus 1 byte for newline (the space is included in SimpleTextUtil.CHECKSUM):
    long footerStartPos = data.length() - (SimpleTextUtil.CHECKSUM.length + 21);
    ChecksumIndexInput input = new BufferedChecksumIndexInput(clone);
    while (true) {
      SimpleTextUtil.readLine(input, scratch);
      if (input.getFilePointer() >= footerStartPos) {
        // Make sure we landed at precisely the right location:
        if (input.getFilePointer() != footerStartPos) {
          throw new CorruptIndexException("SimpleText failure: footer does not start at expected position current=" + input.getFilePointer() + " vs expected=" + footerStartPos, input);
        }
        SimpleTextUtil.checkFooter(input);
        break;
      }
    }
  }
}
