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
package org.apache.lucene.codecs.lucene90;


import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Lucene 9.0 Field Infos format.
 * <p>Field names are stored in the field info file, with suffix <code>.fnm</code>.
 * <p>FieldInfos (.fnm) --&gt; Header,FieldsCount, &lt;FieldName,FieldNumber,
 * FieldBits,DocValuesBits,DocValuesGen,Attributes,DimensionCount,DimensionNumBytes&gt; <sup>FieldsCount</sup>,Footer
 * <p>Data types:
 * <ul>
 *   <li>Header --&gt; {@link CodecUtil#checkIndexHeader IndexHeader}</li>
 *   <li>FieldsCount --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>FieldName --&gt; {@link DataOutput#writeString String}</li>
 *   <li>FieldBits, IndexOptions, DocValuesBits --&gt; {@link DataOutput#writeByte Byte}</li>
 *   <li>FieldNumber, DimensionCount, DimensionNumBytes --&gt; {@link DataOutput#writeInt VInt}</li>
 *   <li>Attributes --&gt; {@link DataOutput#writeMapOfStrings Map&lt;String,String&gt;}</li>
 *   <li>DocValuesGen --&gt; {@link DataOutput#writeLong(long) Int64}</li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * Field Descriptions:
 * <ul>
 *   <li>FieldsCount: the number of fields in this file.</li>
 *   <li>FieldName: name of the field as a UTF-8 String.</li>
 *   <li>FieldNumber: the field's number. Note that unlike previous versions of
 *       Lucene, the fields are not numbered implicitly by their order in the
 *       file, instead explicitly.</li>
 *   <li>FieldBits: a byte containing field options.
 *     <ul>
 *       <li>The low order bit (0x1) is one for fields that have term vectors
 *           stored, and zero for fields without term vectors.</li>
 *       <li>If the second lowest order-bit is set (0x2), norms are omitted for the
 *           indexed field.</li>
 *       <li>If the third lowest-order bit is set (0x4), payloads are stored for the
 *           indexed field.</li>
 *     </ul>
 *   </li>
 *   <li>IndexOptions: a byte containing index options.
 *     <ul>
 *       <li>0: not indexed</li>
 *       <li>1: indexed as DOCS_ONLY</li>
 *       <li>2: indexed as DOCS_AND_FREQS</li>
 *       <li>3: indexed as DOCS_AND_FREQS_AND_POSITIONS</li>
 *       <li>4: indexed as DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS</li>
 *     </ul>
 *   </li>
 *   <li>DocValuesBits: a byte containing per-document value types. The type
 *       recorded as two four-bit integers, with the high-order bits representing
 *       <code>norms</code> options, and the low-order bits representing 
 *       {@code DocValues} options. Each four-bit integer can be decoded as such:
 *     <ul>
 *       <li>0: no DocValues for this field.</li>
 *       <li>1: NumericDocValues. ({@link DocValuesType#NUMERIC})</li>
 *       <li>2: BinaryDocValues. ({@code DocValuesType#BINARY})</li>
 *       <li>3: SortedDocValues. ({@code DocValuesType#SORTED})</li>
 *      </ul>
 *   </li>
 *   <li>DocValuesGen is the generation count of the field's DocValues. If this is -1,
 *       there are no DocValues updates to that field. Anything above zero means there 
 *       are updates stored by {@link DocValuesFormat}.</li>
 *   <li>Attributes: a key-value map of codec-private attributes.</li>
 *   <li>PointDimensionCount, PointNumBytes: these are non-zero only if the field is
 *       indexed as points, e.g. using {@link org.apache.lucene.document.LongPoint}</li>
 *   <li>VectorDimension: it is non-zero if the field is indexed as vectors.</li>
 *   <li>VectorDistFunction: a byte containing distance function used for similarity calculation.
 *     <ul>
 *       <li>0: no distance function is defined for this field.</li>
 *       <li>1: EUCLIDEAN distance. ({@link org.apache.lucene.index.VectorValues.ScoreFunction#EUCLIDEAN})</li>
 *       <li>2: DOT_PRODUCT score. ({@link org.apache.lucene.index.VectorValues.ScoreFunction#DOT_PRODUCT})</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * @lucene.experimental
 */
public final class Lucene90FieldInfosFormat extends FieldInfosFormat {

  /** Sole constructor. */
  public Lucene90FieldInfosFormat() {
  }
  
  @Override
  public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, EXTENSION);
    try (ChecksumIndexInput input = directory.openChecksumInput(fileName, context)) {
      Throwable priorE = null;
      FieldInfo infos[] = null;
      try {
        int version = CodecUtil.checkIndexHeader(input,
                                   Lucene90FieldInfosFormat.CODEC_NAME,
                                   Lucene90FieldInfosFormat.FORMAT_START,
                                   Lucene90FieldInfosFormat.FORMAT_CURRENT,
                                   segmentInfo.getId(), segmentSuffix);
        
        final int size = input.readVInt(); //read in the size
        infos = new FieldInfo[size];
        
        // previous field's attribute map, we share when possible:
        Map<String,String> lastAttributes = Collections.emptyMap();
        
        for (int i = 0; i < size; i++) {
          String name = input.readString();
          final int fieldNumber = input.readVInt();
          if (fieldNumber < 0) {
            throw new CorruptIndexException("invalid field number for field: " + name + ", fieldNumber=" + fieldNumber, input);
          }
          byte bits = input.readByte();
          boolean storeTermVector = (bits & STORE_TERMVECTOR) != 0;
          boolean omitNorms = (bits & OMIT_NORMS) != 0;
          boolean storePayloads = (bits & STORE_PAYLOADS) != 0;
          boolean isSoftDeletesField = (bits & SOFT_DELETES_FIELD) != 0;

          final IndexOptions indexOptions = getIndexOptions(input, input.readByte());
          
          // DV Types are packed in one byte
          final DocValuesType docValuesType = getDocValuesType(input, input.readByte());
          final long dvGen = input.readLong();
          Map<String,String> attributes = input.readMapOfStrings();
          // just use the last field's map if its the same
          if (attributes.equals(lastAttributes)) {
            attributes = lastAttributes;
          }
          lastAttributes = attributes;
          int pointDataDimensionCount = input.readVInt();
          int pointNumBytes;
          int pointIndexDimensionCount = pointDataDimensionCount;
          if (pointDataDimensionCount != 0) {
            if (version >= Lucene90FieldInfosFormat.FORMAT_SELECTIVE_INDEXING) {
              pointIndexDimensionCount = input.readVInt();
            }
            pointNumBytes = input.readVInt();
          } else {
            pointNumBytes = 0;
          }
          final int vectorDimension = input.readVInt();
          final VectorValues.ScoreFunction vectorDistFunc = getDistFunc(input, input.readByte());

          try {
            infos[i] = new FieldInfo(name, fieldNumber, storeTermVector, omitNorms, storePayloads, 
                                     indexOptions, docValuesType, dvGen, attributes,
                                     pointDataDimensionCount, pointIndexDimensionCount, pointNumBytes, vectorDimension, vectorDistFunc, isSoftDeletesField);
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
  
  static {
    // We "mirror" DocValues enum values with the constants below; let's try to ensure if we add a new DocValuesType while this format is
    // still used for writing, we remember to fix this encoding:
    assert DocValuesType.values().length == 6;
  }

  private static byte docValuesByte(DocValuesType type) {
    switch(type) {
    case NONE:
      return 0;
    case NUMERIC:
      return 1;
    case BINARY:
      return 2;
    case SORTED:
      return 3;
    case SORTED_SET:
      return 4;
    case SORTED_NUMERIC:
      return 5;
    default:
      // BUG
      throw new AssertionError("unhandled DocValuesType: " + type);
    }
  }

  private static DocValuesType getDocValuesType(IndexInput input, byte b) throws IOException {
    switch(b) {
    case 0:
      return DocValuesType.NONE;
    case 1:
      return DocValuesType.NUMERIC;
    case 2:
      return DocValuesType.BINARY;
    case 3:
      return DocValuesType.SORTED;
    case 4:
      return DocValuesType.SORTED_SET;
    case 5:
      return DocValuesType.SORTED_NUMERIC;
    default:
      throw new CorruptIndexException("invalid docvalues byte: " + b, input);
    }
  }

  private static VectorValues.ScoreFunction getDistFunc(IndexInput input, byte b) throws IOException {
    if (b < 0 || b >= VectorValues.ScoreFunction.values().length) {
      throw new CorruptIndexException("invalid distance function: " + b, input);
    }
    return VectorValues.ScoreFunction.values()[b];
  }

  static {
    // We "mirror" IndexOptions enum values with the constants below; let's try to ensure if we add a new IndexOption while this format is
    // still used for writing, we remember to fix this encoding:
    assert IndexOptions.values().length == 5;
  }

  private static byte indexOptionsByte(IndexOptions indexOptions) {
    switch (indexOptions) {
    case NONE:
      return 0;
    case DOCS:
      return 1;
    case DOCS_AND_FREQS:
      return 2;
    case DOCS_AND_FREQS_AND_POSITIONS:
      return 3;
    case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
      return 4;
    default:
      // BUG:
      throw new AssertionError("unhandled IndexOptions: " + indexOptions);
    }
  }
  
  private static IndexOptions getIndexOptions(IndexInput input, byte b) throws IOException {
    switch (b) {
    case 0:
      return IndexOptions.NONE;
    case 1:
      return IndexOptions.DOCS;
    case 2:
      return IndexOptions.DOCS_AND_FREQS;
    case 3:
      return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    case 4:
      return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
    default:
      // BUG
      throw new CorruptIndexException("invalid IndexOptions byte: " + b, input);
    }
  }

  @Override
  public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, EXTENSION);
    try (IndexOutput output = directory.createOutput(fileName, context)) {
      CodecUtil.writeIndexHeader(output, Lucene90FieldInfosFormat.CODEC_NAME, Lucene90FieldInfosFormat.FORMAT_CURRENT, segmentInfo.getId(), segmentSuffix);
      output.writeVInt(infos.size());
      for (FieldInfo fi : infos) {
        fi.checkConsistency();

        output.writeString(fi.name);
        output.writeVInt(fi.number);

        byte bits = 0x0;
        if (fi.hasVectors()) bits |= STORE_TERMVECTOR;
        if (fi.omitsNorms()) bits |= OMIT_NORMS;
        if (fi.hasPayloads()) bits |= STORE_PAYLOADS;
        if (fi.isSoftDeletesField()) bits |= SOFT_DELETES_FIELD;
        output.writeByte(bits);

        output.writeByte(indexOptionsByte(fi.getIndexOptions()));

        // pack the DV type and hasNorms in one byte
        output.writeByte(docValuesByte(fi.getDocValuesType()));
        output.writeLong(fi.getDocValuesGen());
        output.writeMapOfStrings(fi.attributes());
        output.writeVInt(fi.getPointDimensionCount());
        if (fi.getPointDimensionCount() != 0) {
          output.writeVInt(fi.getPointIndexDimensionCount());
          output.writeVInt(fi.getPointNumBytes());
        }
        output.writeVInt(fi.getVectorDimension());
        output.writeByte((byte) fi.getVectorScoreFunction().ordinal());
      }
      CodecUtil.writeFooter(output);
    }
  }
  
  /** Extension of field infos */
  static final String EXTENSION = "fnm";
  
  // Codec header
  static final String CODEC_NAME = "Lucene90FieldInfos";
  static final int FORMAT_START = 0;
  static final int FORMAT_SOFT_DELETES = 1;
  static final int FORMAT_SELECTIVE_INDEXING = 2;
  static final int FORMAT_CURRENT = FORMAT_SELECTIVE_INDEXING;
  
  // Field flags
  static final byte STORE_TERMVECTOR = 0x1;
  static final byte OMIT_NORMS = 0x2;
  static final byte STORE_PAYLOADS = 0x4;
  static final byte SOFT_DELETES_FIELD = 0x8;
}
