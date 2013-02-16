package org.apache.lucene.codecs.simpletext;

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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
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

  static class OneField {
    long dataStartFilePointer;
    String pattern;
    String ordPattern;
    int maxLength;
    boolean fixedLength;
    long minValue;
    long numValues;
  };

  final int maxDoc;
  final IndexInput data;
  final BytesRef scratch = new BytesRef();
  final Map<String,OneField> fields = new HashMap<String,OneField>();
  
  public SimpleTextDocValuesReader(SegmentReadState state, String ext) throws IOException {
    //System.out.println("dir=" + state.directory + " seg=" + state.segmentInfo.name + " ext=" + ext);
    data = state.directory.openInput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ext), state.context);
    maxDoc = state.segmentInfo.getDocCount();
    while(true) {
      readLine();
      //System.out.println("READ field=" + scratch.utf8ToString());
      if (scratch.equals(END)) {
        break;
      }
      assert startsWith(FIELD) : scratch.utf8ToString();
      String fieldName = stripPrefix(FIELD);
      //System.out.println("  field=" + fieldName);
      FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldName);
      assert fieldInfo != null;

      OneField field = new OneField();
      fields.put(fieldName, field);

      readLine();
      assert startsWith(TYPE) : scratch.utf8ToString();

      DocValuesType dvType = DocValuesType.valueOf(stripPrefix(TYPE));
      assert dvType != null;
      if (dvType == DocValuesType.NUMERIC) {
        readLine();
        assert startsWith(MINVALUE): "got " + scratch.utf8ToString() + " field=" + fieldName + " ext=" + ext;
        field.minValue = Long.parseLong(stripPrefix(MINVALUE));
        readLine();
        assert startsWith(PATTERN);
        field.pattern = stripPrefix(PATTERN);
        field.dataStartFilePointer = data.getFilePointer();
        data.seek(data.getFilePointer() + (1+field.pattern.length()) * maxDoc);
      } else if (dvType == DocValuesType.BINARY) {
        readLine();
        assert startsWith(MAXLENGTH);
        field.maxLength = Integer.parseInt(stripPrefix(MAXLENGTH));
        readLine();
        assert startsWith(PATTERN);
        field.pattern = stripPrefix(PATTERN);
        field.dataStartFilePointer = data.getFilePointer();
        data.seek(data.getFilePointer() + (9+field.pattern.length()+field.maxLength) * maxDoc);
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
    final OneField field = fields.get(fieldInfo.name);
    assert field != null;

    // SegmentCoreReaders already verifies this field is
    // valid:
    assert field != null: "field=" + fieldInfo.name + " fields=" + fields;

    final IndexInput in = data.clone();
    final BytesRef scratch = new BytesRef();
    final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));

    decoder.setParseBigDecimal(true);

    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        try {
          //System.out.println(Thread.currentThread().getName() + ": get docID=" + docID + " in=" + in);
          if (docID < 0 || docID >= maxDoc) {
            throw new IndexOutOfBoundsException("docID must be 0 .. " + (maxDoc-1) + "; got " + docID);
          }
          in.seek(field.dataStartFilePointer + (1+field.pattern.length())*docID);
          SimpleTextUtil.readLine(in, scratch);
          //System.out.println("parsing delta: " + scratch.utf8ToString());
          BigDecimal bd;
          try {
            bd = (BigDecimal) decoder.parse(scratch.utf8ToString());
          } catch (ParseException pe) {
            CorruptIndexException e = new CorruptIndexException("failed to parse BigDecimal value");
            e.initCause(pe);
            throw e;
          }
          return BigInteger.valueOf(field.minValue).add(bd.toBigIntegerExact()).longValue();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    };
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo fieldInfo) throws IOException {
    final OneField field = fields.get(fieldInfo.name);

    // SegmentCoreReaders already verifies this field is
    // valid:
    assert field != null;

    final IndexInput in = data.clone();
    final BytesRef scratch = new BytesRef();
    final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));

    return new BinaryDocValues() {
      @Override
      public void get(int docID, BytesRef result) {
        try {
          if (docID < 0 || docID >= maxDoc) {
            throw new IndexOutOfBoundsException("docID must be 0 .. " + (maxDoc-1) + "; got " + docID);
          }
          in.seek(field.dataStartFilePointer + (9+field.pattern.length() + field.maxLength)*docID);
          SimpleTextUtil.readLine(in, scratch);
          assert StringHelper.startsWith(scratch, LENGTH);
          int len;
          try {
            len = decoder.parse(new String(scratch.bytes, scratch.offset + LENGTH.length, scratch.length - LENGTH.length, "UTF-8")).intValue();
          } catch (ParseException pe) {
            CorruptIndexException e = new CorruptIndexException("failed to parse int length");
            e.initCause(pe);
            throw e;
          }
          result.bytes = new byte[len];
          result.offset = 0;
          result.length = len;
          in.readBytes(result.bytes, 0, len);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
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
    final BytesRef scratch = new BytesRef();
    final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));
    final DecimalFormat ordDecoder = new DecimalFormat(field.ordPattern, new DecimalFormatSymbols(Locale.ROOT));

    return new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        if (docID < 0 || docID >= maxDoc) {
          throw new IndexOutOfBoundsException("docID must be 0 .. " + (maxDoc-1) + "; got " + docID);
        }
        try {
          in.seek(field.dataStartFilePointer + field.numValues * (9 + field.pattern.length() + field.maxLength) + docID * (1 + field.ordPattern.length()));
          SimpleTextUtil.readLine(in, scratch);
          try {
            return ordDecoder.parse(scratch.utf8ToString()).intValue();
          } catch (ParseException pe) {
            CorruptIndexException e = new CorruptIndexException("failed to parse ord");
            e.initCause(pe);
            throw e;
          }
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }

      @Override
      public void lookupOrd(int ord, BytesRef result) {
        try {
          if (ord < 0 || ord >= field.numValues) {
            throw new IndexOutOfBoundsException("ord must be 0 .. " + (field.numValues-1) + "; got " + ord);
          }
          in.seek(field.dataStartFilePointer + ord * (9 + field.pattern.length() + field.maxLength));
          SimpleTextUtil.readLine(in, scratch);
          assert StringHelper.startsWith(scratch, LENGTH): "got " + scratch.utf8ToString() + " in=" + in;
          int len;
          try {
            len = decoder.parse(new String(scratch.bytes, scratch.offset + LENGTH.length, scratch.length - LENGTH.length, "UTF-8")).intValue();
          } catch (ParseException pe) {
            CorruptIndexException e = new CorruptIndexException("failed to parse int length");
            e.initCause(pe);
            throw e;
          }
          result.bytes = new byte[len];
          result.offset = 0;
          result.length = len;
          in.readBytes(result.bytes, 0, len);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }

      @Override
      public int getValueCount() {
        return (int)field.numValues;
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
    final BytesRef scratch = new BytesRef();
    final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));
    
    return new SortedSetDocValues() {
      String[] currentOrds = new String[0];
      int currentIndex = 0;
      
      @Override
      public long nextOrd() {
        if (currentIndex == currentOrds.length) {
          return NO_MORE_ORDS;
        } else {
          return Long.parseLong(currentOrds[currentIndex++]);
        }
      }

      @Override
      public void setDocument(int docID) {
        if (docID < 0 || docID >= maxDoc) {
          throw new IndexOutOfBoundsException("docID must be 0 .. " + (maxDoc-1) + "; got " + docID);
        }
        try {
          in.seek(field.dataStartFilePointer + field.numValues * (9 + field.pattern.length() + field.maxLength) + docID * (1 + field.ordPattern.length()));
          SimpleTextUtil.readLine(in, scratch);
          String ordList = scratch.utf8ToString().trim();
          if (ordList.isEmpty()) {
            currentOrds = new String[0];
          } else {
            currentOrds = ordList.split(",");
          }
          currentIndex = 0;
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }

      @Override
      public void lookupOrd(long ord, BytesRef result) {
        try {
          if (ord < 0 || ord >= field.numValues) {
            throw new IndexOutOfBoundsException("ord must be 0 .. " + (field.numValues-1) + "; got " + ord);
          }
          in.seek(field.dataStartFilePointer + ord * (9 + field.pattern.length() + field.maxLength));
          SimpleTextUtil.readLine(in, scratch);
          assert StringHelper.startsWith(scratch, LENGTH): "got " + scratch.utf8ToString() + " in=" + in;
          int len;
          try {
            len = decoder.parse(new String(scratch.bytes, scratch.offset + LENGTH.length, scratch.length - LENGTH.length, "UTF-8")).intValue();
          } catch (ParseException pe) {
            CorruptIndexException e = new CorruptIndexException("failed to parse int length");
            e.initCause(pe);
            throw e;
          }
          result.bytes = new byte[len];
          result.offset = 0;
          result.length = len;
          in.readBytes(result.bytes, 0, len);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
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
    return StringHelper.startsWith(scratch, prefix);
  }

  /** Used only in ctor: */
  private String stripPrefix(BytesRef prefix) throws IOException {
    return new String(scratch.bytes, scratch.offset + prefix.length, scratch.length - prefix.length, "UTF-8");
  }
}
