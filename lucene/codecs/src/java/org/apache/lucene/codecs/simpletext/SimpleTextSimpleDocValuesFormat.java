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
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.ParsePosition;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.codecs.BinaryDocValuesConsumer;
import org.apache.lucene.codecs.DocValuesArraySource;
import org.apache.lucene.codecs.NumericDocValuesConsumer;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
import org.apache.lucene.codecs.SortedDocValuesConsumer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.PackedInts;


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
  final static BytesRef MAXLENGTH = new BytesRef("  maxlength ");
  final static BytesRef LENGTH = new BytesRef("length ");
  // used for sorted bytes
  final static BytesRef NUMVALUES = new BytesRef("  numvalues");
  final static BytesRef ORDPATTERN = new BytesRef("  ordpattern");

  @Override
  public SimpleDVConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new SimpleTextDocValuesWriter(state.directory, state.segmentInfo, state.context);
  }

  @Override
  public PerDocProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new SimpleTextDocValuesReader(state.fieldInfos, state.dir, state.segmentInfo, state.context);
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
   *    maxlength 8
   *    pattern 0
   *  length 6
   *  foobar[space][space]
   *  length 3
   *  baz[space][space][space][space][space]
   *  ...
   *  </pre>
   *  so an ord's value can be retrieved by seeking to startOffset + (9+pattern.length+maxlength)*ord
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
    
    SimpleTextDocValuesWriter(Directory dir, SegmentInfo si, IOContext context) throws IOException {
      data = dir.createOutput(IndexFileNames.segmentFileName(si.name, "", "dat"), context);
    }

    @Override
    public NumericDocValuesConsumer addNumericField(FieldInfo field, final long minValue, long maxValue) throws IOException {
      writeFieldEntry(field);
      
      // write our minimum value to the .dat, all entries are deltas from that
      SimpleTextUtil.write(data, MINVALUE);
      SimpleTextUtil.write(data, Long.toString(minValue), scratch);
      SimpleTextUtil.writeNewline(data);

      // build up our fixed-width "simple text packed ints" format
      int maxBytesPerValue = Long.toString(maxValue - minValue).length();
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < maxBytesPerValue; i++) {
        sb.append('0');
      }
      
      // write our pattern to the .dat
      SimpleTextUtil.write(data, PATTERN);
      SimpleTextUtil.write(data, sb.toString(), scratch);
      SimpleTextUtil.writeNewline(data);
      
      final DecimalFormat encoder = new DecimalFormat(sb.toString(), new DecimalFormatSymbols(Locale.ROOT));
      return new NumericDocValuesConsumer() {
        int numDocsWritten = 0;

        @Override
        public void add(long value) throws IOException {
          long delta = value - minValue;
          SimpleTextUtil.write(data, encoder.format(delta), scratch);
          SimpleTextUtil.writeNewline(data);
          numDocsWritten++;
        }

        @Override
        public void finish(FieldInfos fieldInfos, int numDocs) throws IOException {
          assert numDocs == numDocsWritten;
          // nocommit: hopefully indexwriter is responsible for "filling" like it does stored fields!
        }
      };
    }

    @Override
    public BinaryDocValuesConsumer addBinaryField(FieldInfo field, boolean fixedLength, final int maxLength) throws IOException {
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
      
      return new BinaryDocValuesConsumer() {
        int numDocsWritten = 0;
        
        @Override
        public void add(BytesRef value) throws IOException {
          // write length
          SimpleTextUtil.write(data, LENGTH);
          SimpleTextUtil.write(data, encoder.format(value.length), scratch);
          SimpleTextUtil.writeNewline(data);
          
          // write bytes
          SimpleTextUtil.write(data, value);
          // pad to fit
          for (int i = value.length; i < maxLength; i++) {
            data.writeByte((byte)' ');
          }
          SimpleTextUtil.writeNewline(data);
          numDocsWritten++;
        }

        @Override
        public void finish(FieldInfos fis, int numDocs) throws IOException {
          assert numDocs == numDocsWritten;
          // nocommit: hopefully indexwriter is responsible for "filling" like it does stored fields!
        }
      };
    }
    
    // nocommit
    @Override
    public SortedDocValuesConsumer addSortedField(FieldInfo field, int valueCount, boolean fixedLength, final int maxLength) throws IOException {
      writeFieldEntry(field);
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

      return new SortedDocValuesConsumer() {
        
        @Override
        public void addValue(BytesRef value) throws IOException {
          // write length
          SimpleTextUtil.write(data, LENGTH);
          SimpleTextUtil.write(data, encoder.format(value.length), scratch);
          SimpleTextUtil.writeNewline(data);
          
          // write bytes
          SimpleTextUtil.write(data, value);
          // pad to fit
          for (int i = value.length; i < maxLength; i++) {
            data.writeByte((byte)' ');
          }
          SimpleTextUtil.writeNewline(data);
        }

        @Override
        public void addDoc(int ord) throws IOException {
          SimpleTextUtil.write(data, encoder.format(ord), scratch);
          SimpleTextUtil.writeNewline(data);
        }
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

  static class SimpleTextDocValuesReader extends PerDocProducer {

    static class OneField {
      FieldInfo fieldInfo;
      long dataStartFilePointer;
      String pattern;
      String ordPattern;
      int maxLength;
      long minValue;
      int numValues;
    };

    final int maxDoc;
    final IndexInput data;
    final BytesRef scratch = new BytesRef();
    final Map<String,OneField> fields = new HashMap<String,OneField>();
    
    SimpleTextDocValuesReader(FieldInfos fieldInfos, Directory dir, SegmentInfo si, IOContext context) throws IOException {
      data = dir.openInput(IndexFileNames.segmentFileName(si.name, "", "dat"), context);
      maxDoc = si.getDocCount();
      while(true) {
        readLine();
        if (scratch.equals(END)) {
          break;
        }
        assert startsWith(FIELD) : scratch.utf8ToString();
        String fieldName = stripPrefix(FIELD);
        FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);
        assert fieldInfo != null;

        OneField field = new OneField();
        fields.put(fieldName, field);

        field.fieldInfo = fieldInfo;
        
        DocValues.Type dvType = fieldInfo.getDocValuesType();
        assert dvType != null;
        if (DocValues.isNumber(dvType)) {
          readLine();
          assert startsWith(MINVALUE);
          field.minValue = Long.parseLong(stripPrefix(MINVALUE));
          readLine();
          assert startsWith(PATTERN);
          field.pattern = stripPrefix(PATTERN);
          field.dataStartFilePointer = data.getFilePointer();
          data.seek(data.getFilePointer() + (1+field.pattern.length()) * maxDoc);
        } else if (DocValues.isBytes(dvType)) {
          readLine();
          assert startsWith(MAXLENGTH);
          field.maxLength = Integer.parseInt(stripPrefix(MAXLENGTH));
          readLine();
          assert startsWith(PATTERN);
          field.pattern = stripPrefix(PATTERN);
          field.dataStartFilePointer = data.getFilePointer();
          data.seek(data.getFilePointer() + (9+field.pattern.length()+field.maxLength) * maxDoc);
          break;
        } else if (DocValues.isSortedBytes(dvType)) {
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
          // nocommit: we need to seek past the data section!!!!
        } else if (DocValues.isFloat(dvType)) {
          // nocommit
        } else {
          throw new AssertionError();
        }
      }
    }

    class SimpleTextDocValues extends DocValues {
      private final OneField field;

      public SimpleTextDocValues(OneField field) {
        this.field = field;
      }

      // nocommit provide a simple default Source impl that
      // loads DirectSource and pulls things into RAM; we
      // need producer API to provide the min/max value,
      // fixed/max length, etc.

      @Override
      public Source loadSource() throws IOException {
        // nocommit todo
        DocValues.Type dvType = field.fieldInfo.getDocValuesType();
        if (DocValues.isNumber(dvType)) {
          Source source = loadDirectSource();
          System.out.println(maxDoc);
          long[] values = new long[maxDoc];
          for(int docID=0;docID<maxDoc;docID++) {
            values[docID] = source.getInt(docID);
          }
          return DocValuesArraySource.forType(DocValues.Type.FIXED_INTS_64).newFromArray(values);
        } else if (DocValues.isBytes(dvType)) {
          Source source = loadDirectSource();
          final byte[][] values = new byte[maxDoc][];
          for(int docID=0;docID<maxDoc;docID++) {
            // nocommit: who passes null!!!
            BytesRef value = source.getBytes(docID, new BytesRef());
            byte[] bytes = new byte[value.length];
            System.arraycopy(value.bytes, value.offset, bytes, 0, value.length);
            values[docID] = bytes;
          }

          return new Source(dvType) {
            @Override
            public BytesRef getBytes(int docID, BytesRef result) {
              result.bytes = values[docID];
              result.offset = 0;
              result.length = result.bytes.length;
              return result;
            }
          };

        } else if (DocValues.isSortedBytes(dvType)) {
          SortedSource source = (SortedSource) loadDirectSource();
          final byte[][] values = new byte[field.numValues][];
          BytesRef scratch = new BytesRef();
          for(int ord=0;ord<field.numValues;ord++) {
            source.getByOrd(ord, scratch);
            values[ord] = new byte[scratch.length];
            System.arraycopy(scratch.bytes, scratch.offset, values[ord], 0, scratch.length);
          }

          final int[] ords = new int[maxDoc];
          for(int docID=0;docID<maxDoc;docID++) {
            ords[docID] = source.ord(docID);
          }

          return new SortedSource(dvType, BytesRef.getUTF8SortedAsUnicodeComparator()) {
            @Override
            public int ord(int docID) {
              return ords[docID];
            }

            @Override
            public BytesRef getByOrd(int ord, BytesRef result) {
              result.bytes = values[ord];
              result.offset = 0;
              result.length = result.bytes.length;
              return result;
            }

            @Override
            public int getValueCount() {
              return field.numValues;
            }

            @Override
            public PackedInts.Reader getDocToOrd() {
              return null;
            }
          };

        } else if (DocValues.isFloat(dvType)) {
          // nocommit
          return null;
        } else {
          throw new AssertionError();
        }
      }

      @Override
      public DocValues.Type getType() {
        return field.fieldInfo.getDocValuesType();
      }

      @Override
      public Source loadDirectSource() throws IOException {
        DocValues.Type dvType = field.fieldInfo.getDocValuesType();
        final IndexInput in = data.clone();
        final BytesRef scratch = new BytesRef();
        final DecimalFormat decoder = new DecimalFormat(field.pattern, new DecimalFormatSymbols(Locale.ROOT));

        if (DocValues.isNumber(dvType)) {
          return new Source(dvType) {
            @Override
            public long getInt(int docID) {
              try {
                // nocommit bounds check docID?  spooky
                // because if we don't you can maybe get
                // value from the wrong field ...
                in.seek(field.dataStartFilePointer + (1+field.pattern.length())*docID);
                SimpleTextUtil.readLine(in, scratch);
                System.out.println("parsing delta: " + scratch.utf8ToString());
                return field.minValue + decoder.parse(scratch.utf8ToString(), new ParsePosition(0)).longValue();
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
            }
          };
        } else if (DocValues.isBytes(dvType)) {
          return new Source(dvType) {
            @Override
            public BytesRef getBytes(int docID, BytesRef result) {
              try {
                // nocommit bounds check docID?  spooky
                // because if we don't you can maybe get
                // value from the wrong field ...
                in.seek(field.dataStartFilePointer + (9+field.pattern.length() + field.maxLength)*docID);
                SimpleTextUtil.readLine(in, scratch);
                assert StringHelper.startsWith(scratch, LENGTH);
                int len;
                try {
                  len = decoder.parse(new String(scratch.bytes, scratch.offset + LENGTH.length, scratch.length - LENGTH.length, "UTF-8")).intValue();
                } catch (ParseException pe) {
                  throw new RuntimeException(pe);
                }
                result.bytes = new byte[len];
                result.offset = 0;
                result.length = len;
                in.readBytes(result.bytes, 0, len);
                return result;
              } catch (IOException ioe) {
                // nocommit should .get() just throw IOE...
                throw new RuntimeException(ioe);
              }
            }
          };
        } else if (DocValues.isSortedBytes(dvType)) {

          final DecimalFormat ordDecoder = new DecimalFormat(field.ordPattern, new DecimalFormatSymbols(Locale.ROOT));

          return new SortedSource(dvType, BytesRef.getUTF8SortedAsUnicodeComparator()) {
            @Override
            public int ord(int docID) {
              try {
                in.seek(field.dataStartFilePointer + field.numValues * (9 + field.pattern.length() + field.maxLength) + (1 + field.ordPattern.length()) * docID);
                SimpleTextUtil.readLine(in, scratch);
                return ordDecoder.parse(scratch.utf8ToString(), new ParsePosition(0)).intValue();
              } catch (IOException ioe) {
                // nocommit should .get() just throw IOE...
                throw new RuntimeException(ioe);
              }
            }

            @Override
            public BytesRef getByOrd(int ord, BytesRef result) {
              try {
                in.seek(field.dataStartFilePointer + ord * (9 + field.pattern.length() + field.maxLength));
                SimpleTextUtil.readLine(in, scratch);
                assert StringHelper.startsWith(scratch, LENGTH);
                int len;
                try {
                  len = decoder.parse(new String(scratch.bytes, scratch.offset + LENGTH.length, scratch.length - LENGTH.length, "UTF-8")).intValue();
                } catch (ParseException pe) {
                  throw new RuntimeException(pe);
                }
                result.bytes = new byte[len];
                result.offset = 0;
                result.length = len;
                in.readBytes(result.bytes, 0, len);
                return result;
              } catch (IOException ioe) {
                // nocommit should .get() just throw IOE...
                throw new RuntimeException(ioe);
              }
            }

            @Override
            public int getValueCount() {
              return field.numValues;
            }

            @Override
            public PackedInts.Reader getDocToOrd() {
              return null;
            }
          };
        } else if (DocValues.isFloat(dvType)) {
          // nocommit
          return null;
        } else {
          throw new AssertionError();
        }
      }
    }

    @Override
    public DocValues docValues(String fieldName) {
      OneField field = fields.get(fieldName);
      if (field == null) {
        return null;
      }
      return new SimpleTextDocValues(field);
    }

    @Override
    public void close() throws IOException {
      data.close();
    }

    private void readLine() throws IOException {
      SimpleTextUtil.readLine(data, scratch);
    }

    private boolean startsWith(BytesRef prefix) {
      return StringHelper.startsWith(scratch, prefix);
    }

    private String stripPrefix(BytesRef prefix) throws IOException {
      return new String(scratch.bytes, scratch.offset + prefix.length, scratch.length - prefix.length, "UTF-8");
    }
  }
}
