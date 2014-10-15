package org.apache.lucene.codecs.lucene50;

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
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Lucene 5.0 Field Infos format.
 * <p>
 * <p>Field names are stored in the field info file, with suffix <tt>.fnm</tt>.</p>
 * <p>FieldInfos (.fnm) --&gt; Header,FieldsCount, &lt;FieldName,FieldNumber,
 * FieldBits,DocValuesBits,DocValuesGen,Attributes&gt; <sup>FieldsCount</sup>,Footer</p>
 * <p>Data types:
 * <ul>
 *   <li>Header --&gt; {@link CodecUtil#checkSegmentHeader SegmentHeader}</li>
 *   <li>FieldsCount --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>FieldName --&gt; {@link DataOutput#writeString String}</li>
 *   <li>FieldBits, DocValuesBits --&gt; {@link DataOutput#writeByte Byte}</li>
 *   <li>FieldNumber --&gt; {@link DataOutput#writeInt VInt}</li>
 *   <li>Attributes --&gt; {@link DataOutput#writeStringStringMap Map&lt;String,String&gt;}</li>
 *   <li>DocValuesGen --&gt; {@link DataOutput#writeLong(long) Int64}</li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * </p>
 * Field Descriptions:
 * <ul>
 *   <li>FieldsCount: the number of fields in this file.</li>
 *   <li>FieldName: name of the field as a UTF-8 String.</li>
 *   <li>FieldNumber: the field's number. Note that unlike previous versions of
 *       Lucene, the fields are not numbered implicitly by their order in the
 *       file, instead explicitly.</li>
 *   <li>FieldBits: a byte containing field options.
 *       <ul>
 *         <li>The low-order bit is one for indexed fields, and zero for non-indexed
 *             fields.</li>
 *         <li>The second lowest-order bit is one for fields that have term vectors
 *             stored, and zero for fields without term vectors.</li>
 *         <li>If the third lowest order-bit is set (0x4), offsets are stored into
 *             the postings list in addition to positions.</li>
 *         <li>Fourth bit is unused.</li>
 *         <li>If the fifth lowest-order bit is set (0x10), norms are omitted for the
 *             indexed field.</li>
 *         <li>If the sixth lowest-order bit is set (0x20), payloads are stored for the
 *             indexed field.</li>
 *         <li>If the seventh lowest-order bit is set (0x40), term frequencies and
 *             positions omitted for the indexed field.</li>
 *         <li>If the eighth lowest-order bit is set (0x80), positions are omitted for the
 *             indexed field.</li>
 *       </ul>
 *    </li>
 *    <li>DocValuesBits: a byte containing per-document value types. The type
 *        recorded as two four-bit integers, with the high-order bits representing
 *        <code>norms</code> options, and the low-order bits representing 
 *        {@code DocValues} options. Each four-bit integer can be decoded as such:
 *        <ul>
 *          <li>0: no DocValues for this field.</li>
 *          <li>1: NumericDocValues. ({@link DocValuesType#NUMERIC})</li>
 *          <li>2: BinaryDocValues. ({@code DocValuesType#BINARY})</li>
 *          <li>3: SortedDocValues. ({@code DocValuesType#SORTED})</li>
 *        </ul>
 *    </li>
 *    <li>DocValuesGen is the generation count of the field's DocValues. If this is -1,
 *        there are no DocValues updates to that field. Anything above zero means there 
 *        are updates stored by {@link DocValuesFormat}.</li>
 *    <li>Attributes: a key-value map of codec-private attributes.</li>
 * </ul>
 *
 * @lucene.experimental
 */
public final class Lucene50FieldInfosFormat extends FieldInfosFormat {

  /** Sole constructor. */
  public Lucene50FieldInfosFormat() {
  }
  
  @Override
  public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, Lucene50FieldInfosFormat.EXTENSION);
    try (ChecksumIndexInput input = directory.openChecksumInput(fileName, context)) {
      Throwable priorE = null;
      FieldInfo infos[] = null;
      try {
        CodecUtil.checkSegmentHeader(input, Lucene50FieldInfosFormat.CODEC_NAME, 
                                     Lucene50FieldInfosFormat.FORMAT_START, 
                                     Lucene50FieldInfosFormat.FORMAT_CURRENT,
                                     segmentInfo.getId(), segmentSuffix);
        
        final int size = input.readVInt(); //read in the size
        infos = new FieldInfo[size];
        
        for (int i = 0; i < size; i++) {
          String name = input.readString();
          final int fieldNumber = input.readVInt();
          if (fieldNumber < 0) {
            throw new CorruptIndexException("invalid field number for field: " + name + ", fieldNumber=" + fieldNumber, input);
          }
          byte bits = input.readByte();
          boolean isIndexed = (bits & Lucene50FieldInfosFormat.IS_INDEXED) != 0;
          boolean storeTermVector = (bits & Lucene50FieldInfosFormat.STORE_TERMVECTOR) != 0;
          boolean omitNorms = (bits & Lucene50FieldInfosFormat.OMIT_NORMS) != 0;
          boolean storePayloads = (bits & Lucene50FieldInfosFormat.STORE_PAYLOADS) != 0;
          final IndexOptions indexOptions;
          if (!isIndexed) {
            indexOptions = null;
          } else if ((bits & Lucene50FieldInfosFormat.OMIT_TERM_FREQ_AND_POSITIONS) != 0) {
            indexOptions = IndexOptions.DOCS_ONLY;
          } else if ((bits & Lucene50FieldInfosFormat.OMIT_POSITIONS) != 0) {
            indexOptions = IndexOptions.DOCS_AND_FREQS;
          } else if ((bits & Lucene50FieldInfosFormat.STORE_OFFSETS_IN_POSTINGS) != 0) {
            indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
          } else {
            indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
          }
          
          // DV Types are packed in one byte
          byte val = input.readByte();
          final DocValuesType docValuesType = getDocValuesType(input, (byte) (val & 0x0F));
          final long dvGen = input.readLong();
          final Map<String,String> attributes = input.readStringStringMap();
          try {
            infos[i] = new FieldInfo(name, isIndexed, fieldNumber, storeTermVector, omitNorms, storePayloads, 
                                     indexOptions, docValuesType, dvGen, Collections.unmodifiableMap(attributes));
            infos[i].checkConsistency();
          } catch (IllegalStateException e) {
            throw new CorruptIndexException("invalid fieldinfo for field: " + name + ", fieldNumber=" + fieldNumber, input, e);
          }
        }
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(input, priorE);
      }
      return new FieldInfos(infos);
    }
  }
  
  private static DocValuesType getDocValuesType(IndexInput input, byte b) throws IOException {
    if (b == 0) {
      return null;
    } else if (b == 1) {
      return DocValuesType.NUMERIC;
    } else if (b == 2) {
      return DocValuesType.BINARY;
    } else if (b == 3) {
      return DocValuesType.SORTED;
    } else if (b == 4) {
      return DocValuesType.SORTED_SET;
    } else if (b == 5) {
      return DocValuesType.SORTED_NUMERIC;
    } else {
      throw new CorruptIndexException("invalid docvalues byte: " + b, input);
    }
  }

  @Override
  public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, Lucene50FieldInfosFormat.EXTENSION);
    try (IndexOutput output = directory.createOutput(fileName, context)) {
      CodecUtil.writeSegmentHeader(output, Lucene50FieldInfosFormat.CODEC_NAME, Lucene50FieldInfosFormat.FORMAT_CURRENT, segmentInfo.getId(), segmentSuffix);
      output.writeVInt(infos.size());
      for (FieldInfo fi : infos) {
        fi.checkConsistency();
        IndexOptions indexOptions = fi.getIndexOptions();
        byte bits = 0x0;
        if (fi.hasVectors()) bits |= Lucene50FieldInfosFormat.STORE_TERMVECTOR;
        if (fi.omitsNorms()) bits |= Lucene50FieldInfosFormat.OMIT_NORMS;
        if (fi.hasPayloads()) bits |= Lucene50FieldInfosFormat.STORE_PAYLOADS;
        if (fi.isIndexed()) {
          bits |= Lucene50FieldInfosFormat.IS_INDEXED;
          assert indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !fi.hasPayloads();
          if (indexOptions == IndexOptions.DOCS_ONLY) {
            bits |= Lucene50FieldInfosFormat.OMIT_TERM_FREQ_AND_POSITIONS;
          } else if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
            bits |= Lucene50FieldInfosFormat.STORE_OFFSETS_IN_POSTINGS;
          } else if (indexOptions == IndexOptions.DOCS_AND_FREQS) {
            bits |= Lucene50FieldInfosFormat.OMIT_POSITIONS;
          }
        }
        output.writeString(fi.name);
        output.writeVInt(fi.number);
        output.writeByte(bits);

        // pack the DV type and hasNorms in one byte
        final byte dv = docValuesByte(fi.getDocValuesType());
        assert (dv & (~0xF)) == 0;
        output.writeByte(dv);
        output.writeLong(fi.getDocValuesGen());
        output.writeStringStringMap(fi.attributes());
      }
      CodecUtil.writeFooter(output);
    }
  }
  
  private static byte docValuesByte(DocValuesType type) {
    if (type == null) {
      return 0;
    } else if (type == DocValuesType.NUMERIC) {
      return 1;
    } else if (type == DocValuesType.BINARY) {
      return 2;
    } else if (type == DocValuesType.SORTED) {
      return 3;
    } else if (type == DocValuesType.SORTED_SET) {
      return 4;
    } else if (type == DocValuesType.SORTED_NUMERIC) {
      return 5;
    } else {
      throw new AssertionError();
    }
  }
  
  /** Extension of field infos */
  static final String EXTENSION = "fnm";
  
  // Codec header
  static final String CODEC_NAME = "Lucene50FieldInfos";
  static final int FORMAT_START = 0;
  static final int FORMAT_CURRENT = FORMAT_START;
  
  // Field flags
  static final byte IS_INDEXED = 0x1;
  static final byte STORE_TERMVECTOR = 0x2;
  static final byte STORE_OFFSETS_IN_POSTINGS = 0x4;
  static final byte OMIT_NORMS = 0x10;
  static final byte STORE_PAYLOADS = 0x20;
  static final byte OMIT_TERM_FREQ_AND_POSITIONS = 0x40;
  static final byte OMIT_POSITIONS = -128;
}
