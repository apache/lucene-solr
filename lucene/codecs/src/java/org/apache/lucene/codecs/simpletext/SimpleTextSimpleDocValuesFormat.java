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

import org.apache.lucene.codecs.BinaryDocValuesConsumer;
import org.apache.lucene.codecs.NumericDocValuesConsumer;
import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDVProducer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
import org.apache.lucene.codecs.SortedDocValuesConsumer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
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
  final static BytesRef MAXVALUE = new BytesRef("  maxvalue ");
  final static BytesRef PATTERN  = new BytesRef("  pattern ");
  // used for bytes
  final static BytesRef FIXEDLENGTH = new BytesRef("  fixedlength ");
  final static BytesRef MAXLENGTH = new BytesRef("  maxlength ");
  final static BytesRef LENGTH = new BytesRef("length ");
  // used for sorted bytes
  final static BytesRef NUMVALUES = new BytesRef("  numvalues ");
  final static BytesRef ORDPATTERN = new BytesRef("  ordpattern ");

  @Override
  public SimpleDVConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new SimpleTextDocValuesWriter(state.directory, state.segmentInfo, state.context);
  }

  @Override
  public SimpleDVProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new SimpleTextDocValuesReader(state.fieldInfos, state.directory, state.segmentInfo, state.context);
  }
  
  /** the .dat file contains the data.
   *  for numbers this is a "fixed-width" file, for example a single byte range:
   *  <pre>
   *  field myField
   *    minvalue 0
   *    maxvalue 234
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
   *    fixedlength false
   *    maxlength 8
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
  static class SimpleTextDocValuesWriter extends SimpleDVConsumer {
    final IndexOutput data;
    final BytesRef scratch = new BytesRef();
    final int numDocs;
    private final Set<String> fieldsSeen = new HashSet<String>(); // for asserting
    
    SimpleTextDocValuesWriter(Directory dir, SegmentInfo si, IOContext context) throws IOException {
      data = dir.createOutput(IndexFileNames.segmentFileName(si.name, "", "dat"), context);
      numDocs = si.getDocCount();
    }

    // for asserting
    private boolean fieldSeen(String field) {
      assert !fieldsSeen.contains(field): "field \"" + field + "\" was added more than once during flush";
      fieldsSeen.add(field);
      return true;
    }

    @Override
    public NumericDocValuesConsumer addNumericField(FieldInfo field, final long minValue, long maxValue) throws IOException {
      assert fieldSeen(field.name);
      writeFieldEntry(field);
      
      // write our minimum value to the .dat, all entries are deltas from that
      SimpleTextUtil.write(data, MINVALUE);
      SimpleTextUtil.write(data, Long.toString(minValue), scratch);
      SimpleTextUtil.writeNewline(data);
      
      SimpleTextUtil.write(data, MAXVALUE);
      SimpleTextUtil.write(data, Long.toString(maxValue), scratch);
      SimpleTextUtil.writeNewline(data);

      assert maxValue >= minValue;

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
      return new NumericDocValuesConsumer() {
        int numDocsWritten = 0;

        @Override
        public void add(long value) throws IOException {
          assert value >= minValue;
          Number delta = BigInteger.valueOf(value).subtract(BigInteger.valueOf(minValue));
          String s = encoder.format(delta);
          assert s.length() == patternString.length();
          SimpleTextUtil.write(data, s, scratch);
          SimpleTextUtil.writeNewline(data);
          numDocsWritten++;
          assert numDocsWritten <= numDocs;
        }

        @Override
        public void finish() throws IOException {
          assert numDocs == numDocsWritten: "numDocs=" + numDocs + " numDocsWritten=" + numDocsWritten;
        }
      };
    }

    @Override
    public BinaryDocValuesConsumer addBinaryField(FieldInfo field, boolean fixedLength, final int maxLength) throws IOException {
      assert fieldSeen(field.name);
      writeFieldEntry(field);
      // write fixedlength
      SimpleTextUtil.write(data, FIXEDLENGTH);
      SimpleTextUtil.write(data, Boolean.toString(fixedLength), scratch);
      SimpleTextUtil.writeNewline(data);
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
      
      return new BinaryDocValuesConsumer() {
        int numDocsWritten = 0;
        
        @Override
        public void add(BytesRef value) throws IOException {
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

        @Override
        public void finish() throws IOException {
          assert numDocs == numDocsWritten;
        }
      };
    }
    
    @Override
    public SortedDocValuesConsumer addSortedField(FieldInfo field, final int valueCount, boolean fixedLength, final int maxLength) throws IOException {
      assert fieldSeen(field.name);
      writeFieldEntry(field);
      // write numValues
      SimpleTextUtil.write(data, NUMVALUES);
      SimpleTextUtil.write(data, Integer.toString(valueCount), scratch);
      SimpleTextUtil.writeNewline(data);
      
      // write fixedlength
      SimpleTextUtil.write(data, FIXEDLENGTH);
      SimpleTextUtil.write(data, Boolean.toString(fixedLength), scratch);
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

      return new SortedDocValuesConsumer() {

        // for asserts:
        private int valuesSeen;
        
        @Override
        public void addValue(BytesRef value) throws IOException {
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

        @Override
        public void addDoc(int ord) throws IOException {
          SimpleTextUtil.write(data, ordEncoder.format(ord), scratch);
          SimpleTextUtil.writeNewline(data);
        }

        @Override
        public void finish() throws IOException {}
      };
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

  static class SimpleTextDocValuesReader extends SimpleDVProducer {

    static class OneField {
      FieldInfo fieldInfo;
      long dataStartFilePointer;
      String pattern;
      String ordPattern;
      int maxLength;
      boolean fixedLength;
      long minValue;
      long maxValue;
      int numValues;
    };

    final int maxDoc;
    final IndexInput data;
    final BytesRef scratch = new BytesRef();
    final Map<String,OneField> fields = new HashMap<String,OneField>();
    
    SimpleTextDocValuesReader(FieldInfos fieldInfos, Directory dir, SegmentInfo si, IOContext context) throws IOException {
      //System.out.println("dir=" + dir + " seg=" + si.name);
      data = dir.openInput(IndexFileNames.segmentFileName(si.name, "", "dat"), context);
      maxDoc = si.getDocCount();
      while(true) {
        readLine();
        //System.out.println("READ field=" + scratch.utf8ToString());
        if (scratch.equals(END)) {
          break;
        }
        assert startsWith(FIELD) : scratch.utf8ToString();
        String fieldName = stripPrefix(FIELD);
        //System.out.println("  field=" + fieldName);
        FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);
        assert fieldInfo != null;

        OneField field = new OneField();
        fields.put(fieldName, field);

        field.fieldInfo = fieldInfo;
        
        DocValues.Type dvType = fieldInfo.getDocValuesType();
        assert dvType != null;
        if (DocValues.isNumber(dvType) || DocValues.isFloat(dvType)) {
          readLine();
          assert startsWith(MINVALUE);
          field.minValue = Long.parseLong(stripPrefix(MINVALUE));
          readLine();
          assert startsWith(MAXVALUE);
          field.maxValue = Long.parseLong(stripPrefix(MAXVALUE));
          readLine();
          assert startsWith(PATTERN);
          field.pattern = stripPrefix(PATTERN);
          field.dataStartFilePointer = data.getFilePointer();
          data.seek(data.getFilePointer() + (1+field.pattern.length()) * maxDoc);
        } else if (DocValues.isBytes(dvType)) {
          readLine();
          assert startsWith(FIXEDLENGTH);
          field.fixedLength = Boolean.parseBoolean(stripPrefix(FIXEDLENGTH));
          readLine();
          assert startsWith(MAXLENGTH);
          field.maxLength = Integer.parseInt(stripPrefix(MAXLENGTH));
          readLine();
          assert startsWith(PATTERN);
          field.pattern = stripPrefix(PATTERN);
          field.dataStartFilePointer = data.getFilePointer();
          data.seek(data.getFilePointer() + (9+field.pattern.length()+field.maxLength) * maxDoc);
        } else if (DocValues.isSortedBytes(dvType)) {
          readLine();
          assert startsWith(NUMVALUES);
          field.numValues = Integer.parseInt(stripPrefix(NUMVALUES));
          readLine();
          assert startsWith(FIXEDLENGTH);
          field.fixedLength = Boolean.parseBoolean(stripPrefix(FIXEDLENGTH));
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
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo fieldInfo) throws IOException {
      final OneField field = fields.get(fieldInfo.name);

      // SegmentCoreReaders already verifies this field is
      // valid:
      assert field != null;

      final IndexInput in = data.clone();
      final BytesRef scratch = new BytesRef();
      final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));

      decoder.setParseBigDecimal(true);

      return new NumericDocValues() {
        @Override
        public long get(int docID) {
          try {
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

        @Override
        public long minValue() {
          return field.minValue;
        }

        @Override
        public long maxValue() {
          return field.maxValue;
        }

        @Override
        public int size() {
          return maxDoc;
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

        @Override
        public int size() {
          return maxDoc;
        }

        @Override
        public boolean isFixedLength() {
          return field.fixedLength;
        }

        @Override
        public int maxLength() {
          return field.maxLength;
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

        @Override
        public int getValueCount() {
          return field.numValues;
        }

        @Override
        public int size() {
          return maxDoc;
        }

        @Override
        public boolean isFixedLength() {
          return field.fixedLength;
        }

        @Override
        public int maxLength() {
          return field.maxLength;
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
