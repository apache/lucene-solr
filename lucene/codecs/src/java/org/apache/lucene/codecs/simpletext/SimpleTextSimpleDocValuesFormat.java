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
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDVProducer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;


/**
 * plain text doc values format.
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextSimpleDocValuesFormat extends SimpleDocValuesFormat {
  final static BytesRef END     = new BytesRef("END");
  final static BytesRef FIELD   = new BytesRef("field ");
  // used for numerics
  final static BytesRef MINVALUE = new BytesRef("  minvalue ");
  final static BytesRef PATTERN  = new BytesRef("  pattern ");
  // used for bytes
  final static BytesRef LENGTH = new BytesRef("length ");
  final static BytesRef MAXLENGTH = new BytesRef("  maxlength ");
  // used for sorted bytes
  final static BytesRef FIXEDLENGTH = new BytesRef("  fixedlength ");
  final static BytesRef NUMVALUES = new BytesRef("  numvalues ");
  final static BytesRef ORDPATTERN = new BytesRef("  ordpattern ");
  
  public SimpleTextSimpleDocValuesFormat() {
    super("SimpleText");
  }

  @Override
  public SimpleDVConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new SimpleTextDocValuesWriter(state, "dat");
  }

  @Override
  public SimpleDVProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new SimpleTextDocValuesReader(state, "dat");
  }
  
  /** the .dat file contains the data.
   *  for numbers this is a "fixed-width" file, for example a single byte range:
   *  <pre>
   *  field myField
   *    minvalue 0
   *    pattern 000
   *  005
   *  234
   *  123
   *  ...
   *  </pre>
   *  so a document's value (delta encoded from minvalue) can be retrieved by 
   *  seeking to startOffset + (1+pattern.length())*docid. The extra 1 is the newline.
   *  
   *  for bytes this is also a "fixed-width" file, for example:
   *  <pre>
   *  field myField
   *    maxlength 6
   *    pattern 0
   *  length 6
   *  foobar[space][space]
   *  length 3
   *  baz[space][space][space][space][space]
   *  ...
   *  </pre>
   *  so a doc's value can be retrieved by seeking to startOffset + (9+pattern.length+maxlength)*doc
   *  the extra 9 is 2 newlines, plus "length " itself.
   *  
   *  for sorted bytes this is a fixed-width file, for example:
   *  <pre>
   *  field myField
   *    numvalues 10
   *    maxLength 8
   *    pattern 0
   *    ordpattern 00
   *  length 6
   *  foobar[space][space]
   *  length 3
   *  baz[space][space][space][space][space]
   *  ...
   *  03
   *  06
   *  01
   *  10
   *  ...
   *  </pre>
   *  so the "ord section" begins at startOffset + (9+pattern.length+maxlength)*numValues.
   *  a document's ord can be retrieved by seeking to "ord section" + (1+ordpattern.length())*docid
   *  an ord's value can be retrieved by seeking to startOffset + (9+pattern.length+maxlength)*ord
   *   
   *  the reader can just scan this file when it opens, skipping over the data blocks
   *  and saving the offset/etc for each field. 
   */
  // nocommit not public
  public static class SimpleTextDocValuesWriter extends SimpleDVConsumer {
    final IndexOutput data;
    final BytesRef scratch = new BytesRef();
    final int numDocs;
    // nocommit
    final boolean isNorms;
    private final Set<String> fieldsSeen = new HashSet<String>(); // for asserting
    
    public SimpleTextDocValuesWriter(SegmentWriteState state, String ext) throws IOException {
      //System.out.println("WRITE: " + IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ext) + " " + state.segmentInfo.getDocCount() + " docs");
      data = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ext), state.context);
      numDocs = state.segmentInfo.getDocCount();
      isNorms = ext.equals("slen");
    }

    // for asserting
    private boolean fieldSeen(String field) {
      assert !fieldsSeen.contains(field): "field \"" + field + "\" was added more than once during flush";
      fieldsSeen.add(field);
      return true;
    }

    @Override
    public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
      assert fieldSeen(field.name);
      // nocommit: this must be multiple asserts
      //assert (field.getDocValuesType() != null && (DocValues.isNumber(field.getDocValuesType()) || DocValues.isFloat(field.getDocValuesType()))) ||
      //  (field.getNormType() != null && (DocValues.isNumber(field.getNormType()) || DocValues.isFloat(field.getNormType()))): "field=" + field.name;
      writeFieldEntry(field);

      // first pass to find min/max
      long minValue = Long.MAX_VALUE;
      long maxValue = Long.MIN_VALUE;
      for(Number n : values) {
        long v = n.longValue();
        minValue = Math.min(minValue, v);
        maxValue = Math.max(maxValue, v);
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
      for(Number n : values) {
        long value = n.longValue();
        assert value >= minValue;
        Number delta = BigInteger.valueOf(value).subtract(BigInteger.valueOf(minValue));
        String s = encoder.format(delta);
        assert s.length() == patternString.length();
        SimpleTextUtil.write(data, s, scratch);
        SimpleTextUtil.writeNewline(data);
        numDocsWritten++;
        assert numDocsWritten <= numDocs;
      }

      assert numDocs == numDocsWritten: "numDocs=" + numDocs + " numDocsWritten=" + numDocsWritten;
    }

    @Override
    public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
      assert fieldSeen(field.name);
      assert field.getDocValuesType() == DocValuesType.BINARY;
      assert !isNorms;
      int maxLength = 0;
      for(BytesRef value : values) {
        maxLength = Math.max(maxLength, value.length);
      }
      writeFieldEntry(field);

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

      int numDocsWritten = 0;
      for(BytesRef value : values) {
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
        numDocsWritten++;
      }

      assert numDocs == numDocsWritten;
    }
    
    @Override
    public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
      assert fieldSeen(field.name);
      assert field.getDocValuesType() == DocValuesType.SORTED;
      assert !isNorms;
      writeFieldEntry(field);

      int valueCount = 0;
      int maxLength = -1;
      for(BytesRef value : values) {
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
      
      int maxOrdBytes = Integer.toString(valueCount).length();
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

      for(BytesRef value : values) {
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

      for(Number ord : docToOrd) {
        SimpleTextUtil.write(data, ordEncoder.format(ord.intValue()), scratch);
        SimpleTextUtil.writeNewline(data);
      }
    }

    /** write the header for this field */
    private void writeFieldEntry(FieldInfo field) throws IOException {
      SimpleTextUtil.write(data, FIELD);
      SimpleTextUtil.write(data, field.name, scratch);
      SimpleTextUtil.writeNewline(data);
    }
    
    @Override
    public void close() throws IOException {
      boolean success = false;
      try {
        assert !fieldsSeen.isEmpty();
        // TODO: sheisty to do this here?
        SimpleTextUtil.write(data, END);
        SimpleTextUtil.writeNewline(data);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(data);
        } else {
          IOUtils.closeWhileHandlingException(data);
        }
      }
    }
  };

  // nocommit once we do "in ram cache of direct source"
  // ... and hopeuflly under SCR control ... then if app
  // asks for direct soruce but it was already cached in ram
  // ... we should use the ram cached one!  we don't do this
  // correctly today ...

  // nocommit make sure we test "all docs have 0 value",
  // "all docs have empty BytesREf"

  // nocommit not public
  public static class SimpleTextDocValuesReader extends SimpleDVProducer {

    static class OneField {
      FieldInfo fieldInfo;
      long dataStartFilePointer;
      String pattern;
      String ordPattern;
      int maxLength;
      boolean fixedLength;
      long minValue;
      int numValues;
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

        field.fieldInfo = fieldInfo;
        //System.out.println("  field=" + fieldName);

        // nocommit hack hack hack!!:
        DocValuesType dvType = ext.equals("slen") ? DocValuesType.NUMERIC : fieldInfo.getDocValuesType();
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
        } else if (dvType == DocValuesType.SORTED) {
          readLine();
          assert startsWith(NUMVALUES);
          field.numValues = Integer.parseInt(stripPrefix(NUMVALUES));
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
}
