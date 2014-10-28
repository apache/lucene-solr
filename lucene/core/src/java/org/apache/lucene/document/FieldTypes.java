package org.apache.lucene.document;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.codecs.lucene50.Lucene50Codec;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;

// TODO
//   - explore what it'd be like to add other higher level types?
//     - BigInt, BigDecimal, IPV6
//   - setters for posinc/offset gaps?
//     - can we remove analyzer from IW?
//   - what about sparse fields... anything for us to do...
//   - payloads just stay write once in their own way?
//   - how to handle old indices w/ no field types yet?
//   - can we simplify how "low level schema" (FieldInfo) merges itself?
//     - only if this new API is the only way to add docs to IndexWriter ... hmm
//   - CheckIndex could optionally do more validation
//     - e.g. that the terms in a numeric field are correct (lead w/ prefix, 7 bit clean)
//     - or that terms in an ipv6 field always have 16 bytes
//   - add query time integration
//     - query parsers
//       - exc if invalid field name asked for
//       - numeric range queries "just work"
//     - creating SortField
//     - creating queries, catching invalid field names, no positions indexed, etc.
//     - SortField should verify FieldTypes.sortable is set for that field
//     - prox queries can verify field was indexed w/ positions
//     - normal queries can verify field was even indexed
//   - move analyzer out of IW/IWC into Field/FieldType/s only?
//   - we could go back to allowing pulling the Document from a reader, updating, re-indexing?  e.g. we can know all fields were stored, and
//     throw exc if not
//   - get save/load working
//   - why does STS fill offset...
//   - no more field reuse right?

// Lucene's secret schemas
//   FieldInfos/GlobalFieldNumbers
//   SortField.type
//   DocValuesType
//   subclassing QueryParsers
//   PerFieldPF/DVF
//   PerFieldSimilarityWrapper
//   PerFieldAnalyzerWrapper
//   oal.document

// nocommit but how can we randomized IWC for tests?

// nocommit maybe we should filter out our key from commit user data?  scary if apps can ... mess with it accidentally?  or just store it
// directly in SIS or someplace else

// nocommit move to oal.index?

// nocommit maxTokenLength?

// nocommit per-field norms format?  then we can commit these "tradeoffs"

// nocommit sortMissingFirst/Last

// nocommit default value?

// nocommit getTermFilter?

// nocommit default qp operator

// nocommit copy field?

// nocommit controlling compression of stored fields, norms

// nocommit back compat: how to handle pre-schema indices

// nocommit should we try to have "garbage collection" here?

// nocommit maybe have a settable global default for "stored"?

// nocommit can/should we validate field names here?

// nocommit can we somehow always store a "source"?

// nocommit make ValueType public?  add setter so you can set that too?

// language for the field?  (to default collator)

// nocommit sort order, sort options (e.g. for collation)
//   case sensitive, punc sensitive, accent sensitive
//   can we fold in ICUCollationDocValuesField somehow...

// nocommit suggesters

// nocommit index-time sorting should be here too

// nocommit sort by languages

// nocommit can we require use of analyzer factories?

// nocommit what schema options does solr offer

// nocommit accent removal and lowercasing for wildcards should just work

// separate analyzer for phrase queries in suggesters

// go through the process of adding a "new high schema type"

// nocommit Index class?  enforcing unique id, xlog?

// nocommit how to randomize IWC?  RIW?

// nocommit maybe we need change IW's setCommitData API to be "add/remove key/value from commit data"?

// nocommit unique/primary key ?

// nocommit must document / make sugar for creating IndexSearcher w/ sim from this class

// nocommit fix all change methods to call validate / rollback

// nocommit boolean, float16?

// nocommit can we move multi-field-ness out of IW?  so IW only gets a single instance of each field

// nocommit if you open a writer, just set up schema, close it, it won't save?

/** Records how each field is indexed, stored, etc.  This class persists
 *  its state using {@link IndexWriter#setCommitData}, using the
 *  {@link FieldTypes#FIELD_PROPERTIES_KEY} key. */

public class FieldTypes {

  /** Key used to store the field types inside {@link IndexWriter#setCommitData}. */
  public static final String FIELD_PROPERTIES_KEY = "field_properties";

  enum ValueType {
    TEXT,
    SHORT_TEXT,
    ATOM,
    INT,
    FLOAT,
    LONG,
    DOUBLE,
    BINARY,
    // nocommit primary_key?
  }

  private final boolean readOnly;

  private final Version indexCreatedVersion;

  final Map<String,FieldType> fields = new HashMap<>();

  // Null when we are readOnly:
  private final Analyzer defaultIndexAnalyzer;

  // Null when we are not readOnly:
  private final Analyzer defaultQueryAnalyzer;

  private final Similarity defaultSimilarity;

  /** Just like current oal.document.FieldType, except for each setting it can also record "not-yet-set". */
  static class FieldType implements IndexableFieldType {
    private final String name;

    public FieldType(String name) {
      this.name = name;
    }

    volatile ValueType valueType;
    volatile DocValuesType docValuesType;
    private volatile boolean docValuesTypeSet;

    // Expert: settings we pass to BlockTree to control how many terms are allowed in each block.
    volatile Integer blockTreeMinItemsInBlock;
    volatile Integer blockTreeMaxItemsInBlock;

    // Gaps to add between multiple values of the same field; if these are not set, we fallback to the Analyzer for that field.
    volatile Integer analyzerPositionGap;
    volatile Integer analyzerOffsetGap;

    // If the field is numeric, this is the precision step we use:
    volatile Integer numericPrecisionStep;
    
    // Whether this field's values are stored, or null if it's not yet set:
    private volatile Boolean stored;

    // Whether this field's values should be indexed for sorting (using doc values):
    private volatile Boolean sortable;
    private volatile Boolean sortReversed;

    // Whether this field's values should be indexed for fast ranges (using numeric field for now):
    private volatile Boolean fastRanges;

    // Whether this field may appear more than once per document:
    private volatile Boolean multiValued;

    // Whether this field's norms are indexed:
    private volatile Boolean indexNorms;

    private volatile Boolean storeTermVectors;
    private volatile Boolean storeTermVectorPositions;
    private volatile Boolean storeTermVectorOffsets;
    private volatile Boolean storeTermVectorPayloads;

    // Field is indexed if this != null:
    private volatile IndexOptions indexOptions;
    private volatile boolean indexOptionsSet;

    // nocommit: not great that we can't also set other formats... but we need per-field wrappers to do this, or we need to move
    // "per-field-ness" of these formats into here, or something:
    private volatile String postingsFormat;
    private volatile String docValuesFormat;

    private volatile Boolean highlighted;

    // NOTE: not persisted, because we don't have API for persisting arbitrary analyzers, or maybe we require AnalysisFactory is always used
    // (which we can serialize)?
    private volatile Analyzer queryAnalyzer;
    private volatile Analyzer indexAnalyzer;
    private volatile Similarity similarity;

    boolean validate() {
      if (valueType != null) {
        switch (valueType) {
        case INT:
        case FLOAT:
        case LONG:
        case DOUBLE:
          if (indexAnalyzer != null) {
            illegalState(name, "type " + valueType + " cannot have an indexAnalyzer");
          }
          if (queryAnalyzer != null) {
            illegalState(name, "type " + valueType + " cannot have a queryAnalyzer");
          }
          if (docValuesType != null && (docValuesType != DocValuesType.NUMERIC && docValuesType != DocValuesType.SORTED_NUMERIC)) {
            illegalState(name, "type " + valueType + " must use NUMERIC docValuesType (got: " + docValuesType + ")");
          }
          if (indexOptions != null && indexOptions.compareTo(IndexOptions.DOCS_ONLY) > 0) {
            illegalState(name, "type " + valueType + " cannot use indexOptions > DOCS_ONLY (got indexOptions " + indexOptions + ")");
          }
          break;
        case TEXT:
          if (sortable == Boolean.TRUE) {
            illegalState(name, "type " + valueType + " cannot sort");
          }
          if (fastRanges == Boolean.TRUE) {
            illegalState(name, "type " + valueType + " cannot optimize for range queries");
          }
          if (docValuesType != null) {
            illegalState(name, "type " + valueType + " cannot use docValuesType " + docValuesType);
          }
          break;
        case SHORT_TEXT:
          if (docValuesType != null && docValuesType != DocValuesType.BINARY && docValuesType != DocValuesType.SORTED && docValuesType != DocValuesType.SORTED_SET) {
            illegalState(name, "type " + valueType + " cannot use docValuesType " + docValuesType);
          }
          if (fastRanges == Boolean.TRUE) {
            illegalState(name, "type " + valueType + " cannot optimize for range queries");
          }
          break;
        case BINARY:
          if (indexAnalyzer != null) {
            illegalState(name, "type " + valueType + " cannot have an indexAnalyzer");
          }
          if (queryAnalyzer != null) {
            illegalState(name, "type " + valueType + " cannot have a queryAnalyzer");
          }
          if (docValuesType != null && docValuesType != DocValuesType.BINARY && docValuesType != DocValuesType.SORTED && docValuesType != DocValuesType.SORTED_SET) {
            illegalState(name, "type " + valueType + " must use BINARY docValuesType (got: " + docValuesType + ")");
          }
          if (fastRanges == Boolean.TRUE) {
            // TODO: auto prefix
            illegalState(name, "type " + valueType + " cannot optimize for range queries");
          }
          break;
        case ATOM:
          if (indexAnalyzer != null) {
            illegalState(name, "type " + valueType + " cannot have an indexAnalyzer");
          }
          if (queryAnalyzer != null) {
            illegalState(name, "type " + valueType + " cannot have a queryAnalyzer");
          }
          // nocommit make sure norms are disabled?
          if (indexOptions != null && indexOptions.compareTo(IndexOptions.DOCS_ONLY) > 0) {
            // nocommit too anal?
            illegalState(name, "type " + valueType + " can only be indexed as DOCS_ONLY; got " + indexOptions);
          }
          if (fastRanges == Boolean.TRUE) {
            // TODO: auto prefix
            illegalState(name, "type " + valueType + " cannot optimize for range queries");
          }
          break;
        default:
          throw new AssertionError("missing value type in switch");
        }
        // nocommit more checks
      }

      if (multiValued == Boolean.TRUE &&
          (docValuesType == DocValuesType.NUMERIC ||
           docValuesType == DocValuesType.SORTED ||
           docValuesType == DocValuesType.BINARY)) {
        illegalState(name, "DocValuesType=" + docValuesType + " cannot be multi-valued");
      }

      if (sortable == Boolean.TRUE && (docValuesTypeSet && (docValuesType == null || docValuesType == DocValuesType.BINARY))) {
        illegalState(name, "cannot sort when DocValuesType=" + docValuesType);
      }

      if (indexOptions == null) {
        if (blockTreeMinItemsInBlock != null) {
          illegalState(name, "can only setTermsDictBlockSize if the field is indexed");
        }
        if (indexAnalyzer != null) {
          illegalState(name, "can only setIndexAnalyzer if the field is indexed");
        }
        if (queryAnalyzer != null) {
          illegalState(name, "can only setQueryAnalyzer if the field is indexed");
        }
      } else {
        if (valueType != ValueType.TEXT && valueType != ValueType.SHORT_TEXT && indexAnalyzer != null) {
          illegalState(name, "can only setIndexAnalyzer for short text and large text fields; got valueType=" + valueType);
        }
        if (valueType != ValueType.TEXT && valueType != ValueType.SHORT_TEXT && queryAnalyzer != null) {
          illegalState(name, "can only setQueryAnalyzer for short text and large text fields; got valueType=" + valueType);
        }
      }

      if (analyzerPositionGap != null) {
        if (indexOptions == null) {
          illegalState(name, "can only setAnalyzerPositionGap if the field is indexed");
        }
        if (multiValued != Boolean.TRUE) {
          illegalState(name, "can only setAnalyzerPositionGap if the field is multi-valued");
        }
      }
      if (analyzerOffsetGap != null) {
        if (indexOptions == null) {
          illegalState(name, "can only setAnalyzerOffsetGap if the field is indexed");
        }
        if (multiValued != Boolean.TRUE) {
          illegalState(name, "can only setAnalyzerOffsetGap if the field is multi-valued");
        }
      }

      if (postingsFormat != null && blockTreeMinItemsInBlock != null) {
        illegalState(name, "cannot use both setTermsDictBlockSize and setPostingsFormat");
      }

      if (highlighted == Boolean.TRUE) {
        if (valueType != ValueType.TEXT && valueType != ValueType.SHORT_TEXT) {
          illegalState(name, "can only enable highlighting for TEXT or SHORT_TEXT fields; got valueType=" + valueType);
        }
        if (indexOptions != null && indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
          illegalState(name, "must index with IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS when highlighting is enabled");
        }
      }

      return true;
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("field \"");
      b.append(name);
      b.append("\":\n");
      b.append("  valueType: ");
      if (valueType != null) {
        b.append(valueType);
      } else {
        b.append("unset");
      }
      b.append('\n');

      if (valueType == ValueType.INT ||
          valueType == ValueType.FLOAT ||
          valueType == ValueType.LONG ||
          valueType == ValueType.DOUBLE) {
        b.append("  numericPrecisionsStep: ");
        if (numericPrecisionStep == null) {
          b.append(numericPrecisionStep);
        } else {
          b.append("  unset");
        }
        b.append('\n');
      }

      if (blockTreeMinItemsInBlock != null) {
        b.append("  term blocks: ");
        b.append(blockTreeMinItemsInBlock);
        b.append(" - ");
        b.append(blockTreeMaxItemsInBlock);
      }

      if (analyzerPositionGap != null) {
        b.append("  multi-valued position gap: ");
        b.append(analyzerPositionGap);
      }

      if (analyzerOffsetGap != null) {
        b.append("  multi-valued offset gap: ");
        b.append(analyzerOffsetGap);
      }

      if (multiValued == Boolean.TRUE) {
        b.append("  multiValued: true");
      }

      b.append("  stored: ");
      if (stored != null) {
        b.append(stored);
      } else {
        b.append("unset");
      }
      b.append('\n');

      b.append("  sortable: ");
      if (sortable != null) {
        b.append(sortable);
        if (sortReversed != null) {
          b.append(" reversed=");
          b.append(sortReversed);
        }
      } else {
        b.append("unset");
      }
      b.append('\n');

      b.append("  fastRanges: ");
      if (fastRanges != null) {
        b.append(fastRanges);
      } else {
        b.append("unset");
      }
      b.append('\n');

      b.append("  highlighted: ");
      if (highlighted != null) {
        b.append(highlighted);
      } else {
        b.append("unset");
      }
      b.append('\n');

      b.append("  docValuesType: ");
      if (docValuesTypeSet == false) {
        b.append("unset");
      } else if (docValuesType == null) {
        b.append("disabled");
      } else {
        b.append(docValuesType);
      }
      b.append('\n');

      b.append("  indexOptions: ");
      if (indexOptionsSet == false) {
        b.append("unset");
      } else if (indexOptions == null) {
        b.append("disabled");
      } else {
        b.append(indexOptions);
        if (storeTermVectors == Boolean.TRUE) {
          b.append("\n  termVectors: yes");
          if (storeTermVectorPositions) {
            b.append(" positions");
            if (storeTermVectorPayloads) {
              b.append(" payloads");
            }
          }
          if (storeTermVectorOffsets) {
            b.append(" offsets");
          }
        } else if (storeTermVectors == Boolean.FALSE) {
          b.append("\n  termVectors: no");
        } else {
          b.append("\n  termVectors: unset");
        }
      }
      b.append('\n');

      return b.toString();
    }

    @Override
    public boolean stored() {
      return stored == Boolean.TRUE;
    }

    @Override
    public boolean storeTermVectors() {
      return storeTermVectors == Boolean.TRUE;
    }

    @Override
    public boolean storeTermVectorOffsets() {
      return storeTermVectorOffsets == Boolean.TRUE;
    }

    @Override
    public boolean storeTermVectorPositions() {
      return storeTermVectorPositions == Boolean.TRUE;
    }

    @Override
    public boolean storeTermVectorPayloads() {
      return storeTermVectorPayloads == Boolean.TRUE;
    }

    @Override
    public boolean omitNorms() {
      return indexNorms == null || indexNorms.booleanValue() == false;
    }

    @Override
    public IndexOptions indexOptions() {
      return indexOptions;
    }

    @Override
    public DocValuesType docValueType() {
      return docValuesType;
    }

    void write(DataOutput out) throws IOException {
      // nocommit under codec control instead?
      out.writeString(name);

      if (valueType == null) {
        out.writeByte((byte) 0);
      } else {
        switch (valueType) {
        case TEXT:
          out.writeByte((byte) 1);
          break;
        case SHORT_TEXT:
          out.writeByte((byte) 2);
          break;
        case ATOM:
          out.writeByte((byte) 3);
          break;
        case INT:
          out.writeByte((byte) 4);
          break;
        case FLOAT:
          out.writeByte((byte) 5);
          break;
        case LONG:
          out.writeByte((byte) 6);
          break;
        case DOUBLE:
          out.writeByte((byte) 7);
          break;
        case BINARY:
          out.writeByte((byte) 8);
          break;
        default:
          throw new AssertionError("missing ValueType in switch");
        }
      }

      if (docValuesTypeSet == false) {
        assert docValuesType == null;
        out.writeByte((byte) 0);
      } else if (docValuesType == null) {
        out.writeByte((byte) 1);
      } else {
        switch (docValuesType) {
        case NUMERIC:
          out.writeByte((byte) 2);
          break;
        case BINARY:
          out.writeByte((byte) 3);
          break;
        case SORTED:
          out.writeByte((byte) 4);
          break;
        case SORTED_NUMERIC:
          out.writeByte((byte) 5);
          break;
        case SORTED_SET:
          out.writeByte((byte) 6);
          break;
        default:
          throw new AssertionError("missing DocValuesType in switch");
        }
      }

      writeNullableInteger(out, blockTreeMinItemsInBlock);
      writeNullableInteger(out, blockTreeMaxItemsInBlock);
      writeNullableInteger(out, analyzerPositionGap);
      writeNullableInteger(out, analyzerOffsetGap);
      writeNullableInteger(out, numericPrecisionStep);
      writeNullableBoolean(out, stored);
      writeNullableBoolean(out, sortable);
      writeNullableBoolean(out, sortReversed);
      writeNullableBoolean(out, multiValued);
      writeNullableBoolean(out, indexNorms);
      writeNullableBoolean(out, storeTermVectors);
      writeNullableBoolean(out, storeTermVectorPositions);
      writeNullableBoolean(out, storeTermVectorOffsets);
      writeNullableBoolean(out, storeTermVectorPayloads);

      if (indexOptionsSet == false) {
        assert indexOptions == null;
        out.writeByte((byte) 0);
      } else if (indexOptions == null) {
        out.writeByte((byte) 1);
      } else {
        switch(indexOptions) {
        case DOCS_ONLY:
          out.writeByte((byte) 2);
          break;
        case DOCS_AND_FREQS:
          out.writeByte((byte) 3);
          break;
        case DOCS_AND_FREQS_AND_POSITIONS:
          out.writeByte((byte) 4);
          break;
        case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
          out.writeByte((byte) 5);
          break;
        default:
          throw new AssertionError("missing IndexOptions in switch");
        }
      }

      writeNullableString(out, postingsFormat);
      writeNullableString(out, docValuesFormat);
      writeNullableBoolean(out, highlighted);
    }

    private static void writeNullableInteger(DataOutput out, Integer value) throws IOException {
      if (value == null) {
        out.writeByte((byte) 0);
      } else {
        out.writeByte((byte) 1);
        out.writeVInt(value.intValue());
      }
    }

    private static Integer readNullableInteger(DataInput in) throws IOException {
      if (in.readByte() == 0) {
        return null;
      } else {
        return in.readVInt();
      }
    }

    private static void writeNullableBoolean(DataOutput out, Boolean value) throws IOException {
      if (value == null) {
        out.writeByte((byte) 0);
      } else if (value == Boolean.TRUE) {
        out.writeByte((byte) 1);
      } else {
        out.writeByte((byte) 2);
      }
    }

    private static Boolean readNullableBoolean(DataInput in) throws IOException {
      byte b = in.readByte();
      if (b == 0) {
        return null;
      } else if (b == 1) {
        return Boolean.TRUE;
      } else if (b == 2) {
        return Boolean.FALSE;
      } else {
        throw new CorruptIndexException("invalid byte for nullable boolean: " + b, in);
      }
    }

    private static void writeNullableString(DataOutput out, String value) throws IOException {
      if (value == null) {
        out.writeByte((byte) 0);
      } else {
        out.writeByte((byte) 1);
        out.writeString(value);
      }
    }

    private static String readNullableString(DataInput in) throws IOException {
      byte b = in.readByte();
      if (b == 0) {
        return null;
      } else if (b == 1) {
        return in.readString();
      } else {
        throw new CorruptIndexException("invalid byte for nullable string: " + b, in);
      }
    }

    public FieldType(DataInput in) throws IOException {
      // nocommit under codec control instead?
      name = in.readString();
      byte b = in.readByte();
      switch (b) {
      case 0:
        valueType = null;
        break;
      case 1:
        valueType = ValueType.TEXT;
        break;
      case 2:
        valueType = ValueType.SHORT_TEXT;
        break;
      case 3:
        valueType = ValueType.ATOM;
        break;
      case 4:
        valueType = ValueType.INT;
        break;
      case 5:
        valueType = ValueType.FLOAT;
        break;
      case 6:
        valueType = ValueType.LONG;
        break;
      case 7:
        valueType = ValueType.DOUBLE;
        break;
      case 8:
        valueType = ValueType.BINARY;
        break;
      default:
        throw new CorruptIndexException("invalid byte for ValueType: " + b, in);
      }

      b = in.readByte();
      switch (b) {
      case 0:
        docValuesTypeSet = false;
        docValuesType = null;
        break;
      case 1:
        docValuesTypeSet = true;
        docValuesType = null;
        break;
      case 2:
        docValuesTypeSet = true;
        docValuesType = DocValuesType.NUMERIC;
        break;
      case 3:
        docValuesTypeSet = true;
        docValuesType = DocValuesType.BINARY;
        break;
      case 4:
        docValuesTypeSet = true;
        docValuesType = DocValuesType.SORTED;
        break;
      case 5:
        docValuesTypeSet = true;
        docValuesType = DocValuesType.SORTED_NUMERIC;
        break;
      case 6:
        docValuesTypeSet = true;
        docValuesType = DocValuesType.SORTED_SET;
        break;
      default:
        throw new CorruptIndexException("invalid byte for DocValuesType: " + b, in);
      }

      blockTreeMinItemsInBlock = readNullableInteger(in);
      blockTreeMaxItemsInBlock = readNullableInteger(in);
      analyzerPositionGap = readNullableInteger(in);
      analyzerOffsetGap = readNullableInteger(in);
      numericPrecisionStep = readNullableInteger(in);
      stored = readNullableBoolean(in);
      sortable = readNullableBoolean(in);
      sortReversed = readNullableBoolean(in);
      multiValued = readNullableBoolean(in);
      indexNorms = readNullableBoolean(in);
      storeTermVectors = readNullableBoolean(in);
      storeTermVectorPositions = readNullableBoolean(in);
      storeTermVectorOffsets = readNullableBoolean(in);
      storeTermVectorPayloads = readNullableBoolean(in);

      b = in.readByte();
      switch (b) {
      case 0:
        indexOptionsSet = false;
        indexOptions = null;
        break;
      case 1:
        indexOptionsSet = true;
        indexOptions = null;
        break;
      case 2:
        indexOptionsSet = true;
        indexOptions = IndexOptions.DOCS_ONLY;
        break;
      case 3:
        indexOptionsSet = true;
        indexOptions = IndexOptions.DOCS_AND_FREQS;
        break;
      case 4:
        indexOptionsSet = true;
        indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        break;
      case 5:
        indexOptionsSet = true;
        indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        break;
      default:
        throw new CorruptIndexException("invalid byte for IndexOptions: " + b, in);
      }

      postingsFormat = readNullableString(in);
      docValuesFormat = readNullableString(in);
      highlighted = readNullableBoolean(in);
    }
  }

  // nocommit move to oal.index and remove these ctors, so you must ask IW or IR for the FieldTypes

  /** Only invoked by IndexWriter directly.
   *
   * @lucene.internal */
  // nocommit lock this down so only IW can create?
  // nocommit throw corrupt ie if not isNewIndex but schema is missing
  public FieldTypes(IndexWriter writer, boolean isNewIndex, Analyzer defaultIndexAnalyzer, Similarity defaultSimilarity) throws IOException {
    this.readOnly = false;
    indexCreatedVersion = loadFields(writer.getCommitData(), isNewIndex);
    this.defaultIndexAnalyzer = defaultIndexAnalyzer;
    this.defaultQueryAnalyzer = null;
    this.defaultSimilarity = defaultSimilarity;
  }

  private FieldTypes(Map<String,String> commitData, Analyzer defaultQueryAnalyzer, Similarity defaultSimilarity) throws IOException {
    this.readOnly = true;
    indexCreatedVersion = loadFields(commitData, false);
    this.defaultIndexAnalyzer = null;
    this.defaultQueryAnalyzer = defaultQueryAnalyzer;
    this.defaultSimilarity = defaultSimilarity;
  }

  // nocommit messy we steal this from commitdata namespace...
  public static final String FIELD_TYPES_KEY = "FieldTypes";
  
  private Version loadFields(Map<String,String> commitUserData, boolean isNewIndex) throws IOException {
    // nocommit must deserialize current fields from commit data
    String currentFieldTypes = commitUserData.get(FIELD_TYPES_KEY);
    if (currentFieldTypes != null) {
      return readFromString(currentFieldTypes);
    } else if (isNewIndex == false) {
      throw new CorruptIndexException("FieldTyps is missing from index", "CommitUserData");
    } else {
      return Version.LATEST;
    }
  }

  /** Decodes String previously created by bytesToString. */
  private static byte[] stringToBytes(String s) {
    byte[] bytesIn = s.getBytes(IOUtils.CHARSET_UTF_8);
    byte[] bytesOut = new byte[bytesIn.length*7/8];
    int carry = 0;
    int carryBits = 0;
    int inUpto = 0;
    int outUpto = 0;
    while (inUpto < bytesIn.length) {
      carry |= (bytesIn[inUpto++] & 0xff) << carryBits;
      carryBits += 7;
      if (carryBits >= 8) {
        bytesOut[outUpto++] = (byte) (carry & 0xff);
        carry = carry >>> 8;
        carryBits -= 8;
      }
    }
    assert outUpto == bytesOut.length;
    return bytesOut;
  }

  // nocommit messy: this is because we stuff the schema into user data; we should instead safe "directly" somewhere (new gen'd file?
  // inside segments_N?)

  /** Encodes byte[] to 7-bit clean chars (ascii). */
  private static String bytesToString(byte[] bytesIn) {
    byte[] bytesOut = new byte[(6+bytesIn.length*8)/7];
    int carry = 0;
    int carryBits = 0;
    int inUpto = 0;
    int outUpto = 0;
    while (inUpto < bytesIn.length) {
      carry |= (bytesIn[inUpto++] & 0xff) << carryBits;
      carryBits += 8;
      while (carryBits >= 7) {
        bytesOut[outUpto++] = (byte) (carry & 0x7f);
        carry = carry >>> 7;
        carryBits -= 7;
      }
    }
    if (carryBits != 0) {
      assert carryBits <= 7;
      bytesOut[outUpto++] = (byte) (carry & 0x7f);
    }
    assert outUpto == bytesOut.length: "outUpto=" + outUpto + " bytesOut.length=" + bytesOut.length + " bytesIn.length=" + bytesIn.length + " carryBits=" + carryBits;

    return new String(bytesOut, IOUtils.CHARSET_UTF_8);
  }

  public synchronized void setPostingsFormat(String fieldName, String postingsFormat) {
    try {
      // Will throw exception if this postingsFormat is unrecognized:
      PostingsFormat.forName(postingsFormat);
    } catch (IllegalArgumentException iae) {
      // Insert field name into exc message
      IllegalArgumentException iae2 = new IllegalArgumentException("field \"" + fieldName + "\": " + iae.getMessage());
      iae2.initCause(iae);
      throw iae2;
    }

    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.postingsFormat = postingsFormat;
      fields.put(fieldName, current);
      changed();
    } else {
      current.postingsFormat = postingsFormat;
      changed();
    }
  }

  synchronized FieldType getFieldType(String fieldName) {
    FieldType fieldType = fields.get(fieldName);
    if (fieldType == null) {
      throw new IllegalArgumentException("field \"" + fieldName + "\" is not recognized");
    }
    return fieldType;
  }

  public synchronized String getPostingsFormat(String fieldName) {
    return getFieldType(fieldName).postingsFormat;
  }

  public synchronized void setDocValuesFormat(String fieldName, String docValuesFormat) {
    try {
      // Will throw exception if this docValuesFormat is unrecognized:
      DocValuesFormat.forName(docValuesFormat);
    } catch (IllegalArgumentException iae) {
      // Insert field name into exc message
      IllegalArgumentException iae2 = new IllegalArgumentException("field \"" + fieldName + "\": " + iae.getMessage());
      iae2.initCause(iae);
      throw iae2;
    }

    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.docValuesFormat = docValuesFormat;
      fields.put(fieldName, current);
      changed();
    } else {
      current.docValuesFormat = docValuesFormat;
      changed();
    }
  }

  private final Similarity similarity = new PerFieldSimilarityWrapper() {
      @Override
      public Similarity get(String fieldName) {
        FieldType field = fields.get(fieldName);
        if (field == null) {
          return defaultSimilarity;
        }
        if (field.similarity != null) {
          return field.similarity;
        } else {
          return defaultSimilarity;
        }
      }
    };

  // nocommit but how can we randomized Codec in tests?
  private final Codec codec = new Lucene50Codec() {
      // nocommit: too bad we can't just set every format here?  what if we fix this schema to record which format per field, and then
      // remove PerFieldXXXFormat...?
      @Override
      public PostingsFormat getPostingsFormatForField(String fieldName) {
        FieldType field = fields.get(fieldName);
        if (field == null) {
          return super.getPostingsFormatForField(fieldName);
        }
        if (field.postingsFormat != null) {
          return PostingsFormat.forName(field.postingsFormat);
        } else if (field.blockTreeMinItemsInBlock != null) {
          assert field.blockTreeMaxItemsInBlock != null;
          // nocommit do we now have cleaner API for this?  Ie "get me default PF, changing these settings"...
          return new Lucene50PostingsFormat(field.blockTreeMinItemsInBlock.intValue(),
                                            field.blockTreeMaxItemsInBlock.intValue());
        }
        return super.getPostingsFormatForField(fieldName); 
      }

      @Override
      public DocValuesFormat getDocValuesFormatForField(String fieldName) {
        FieldType field = fields.get(fieldName);
        if (field != null && field.docValuesFormat != null) {
          return DocValuesFormat.forName(field.docValuesFormat);
        }
        return super.getDocValuesFormatForField(fieldName); 
      }
    };

  private static final Analyzer KEYWORD_ANALYZER = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(final String fieldName) {
        return new TokenStreamComponents(new CoreKeywordTokenizer());
      }
    };

  private abstract class FieldTypeAnalyzer extends DelegatingAnalyzerWrapper {
    public FieldTypeAnalyzer() {
      super(Analyzer.PER_FIELD_REUSE_STRATEGY);
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
      FieldType field = fields.get(fieldName);
      if (field == null) {
        return defaultIndexAnalyzer.getPositionIncrementGap(fieldName);
      }
      if (field.analyzerPositionGap != null) {
        return field.analyzerPositionGap.intValue();
      } else if (field.indexAnalyzer != null) {
        return field.indexAnalyzer.getPositionIncrementGap(fieldName);
      } else {
        return defaultIndexAnalyzer.getPositionIncrementGap(fieldName);
      }
    }

    @Override
    public int getOffsetGap(String fieldName) {
      FieldType field = fields.get(fieldName);
      if (field == null) {
        return defaultIndexAnalyzer.getOffsetGap(fieldName);
      }
      if (field.analyzerOffsetGap != null) {
        return field.analyzerOffsetGap.intValue();
      } else if (field.indexAnalyzer != null) {
        return field.indexAnalyzer.getOffsetGap(fieldName);
      } else {
        return defaultIndexAnalyzer.getOffsetGap(fieldName);
      }
    }

    // nocommit what about wrapReader?
  }

  private final Analyzer indexAnalyzer = new FieldTypeAnalyzer() {
      @Override
      protected Analyzer getWrappedAnalyzer(String fieldName) {
        FieldType field = fields.get(fieldName);
        if (field == null) {
          return defaultIndexAnalyzer;
        }
        if (field.indexAnalyzer != null) {
          return field.indexAnalyzer;
        } else if (field.valueType == ValueType.ATOM) {
          // BUG
          illegalState(fieldName, "ATOM fields should not be analyzed during indexing");
        }
        return defaultIndexAnalyzer;
      }
    };

  private final Analyzer queryAnalyzer = new FieldTypeAnalyzer() {
      @Override
      protected Analyzer getWrappedAnalyzer(String fieldName) {
        FieldType field = fields.get(fieldName);
        if (field == null) {
          return defaultQueryAnalyzer;
        }
        if (field.queryAnalyzer != null) {
          return field.queryAnalyzer;
        } else if (field.valueType == ValueType.ATOM) {
          return KEYWORD_ANALYZER;
        }
        return defaultQueryAnalyzer;
      }
    };

  /** Returns {@link Similarity} that returns the per-field Similarity. */
  public Similarity getSimilarity() {
    return similarity;
  }

  /** Returns {@link Codec} that returns the per-field formats. */
  public Codec getCodec() {
    if (readOnly) {
      return null;
    } else {
      return codec;
    }
  }

  /** Returns {@link Analyzer} that returns the per-field analyzer for use during indexing. */
  public Analyzer getIndexAnalyzer() {
    if (readOnly) {
      return null;
    } else {
      return indexAnalyzer;
    }
  }

  /** Returns {@link Analyzer} that returns the per-field analyzer for use during searching. */
  public Analyzer getQueryAnalyzer() {
    return queryAnalyzer;
  }

  // nocommit we should note that the field has a specific analyzer set, and then throw exc if it didn't get set again after load

  /** NOTE: analyzer does not persist, so each time you create {@code FieldTypes} from
   *  {@linkIndexWriter} or {@link IndexReader} you must set all per-field analyzers again. */
  public synchronized void setAnalyzer(String fieldName, Analyzer analyzer) {
    setIndexAnalyzer(fieldName, analyzer);
    setQueryAnalyzer(fieldName, analyzer);
  }

  /** NOTE: analyzer does not persist, so each time you create {@code FieldTypes} from
   *  {@linkIndexWriter} or {@link IndexReader} you must set all per-field analyzers again. */
  public synchronized void setIndexAnalyzer(String fieldName, Analyzer analyzer) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.indexAnalyzer = analyzer;
      fields.put(fieldName, current);
      changed();
    } else if (current.indexAnalyzer == null) {
      boolean success = false;
      try {
        current.indexAnalyzer = analyzer;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.indexAnalyzer = null;
        }
      }
      changed();
    } else {
      illegalState(fieldName, "analyzer was already set");
    }
  }

  public synchronized Analyzer getIndexAnalyzer(String fieldName) {
    return getFieldType(fieldName).indexAnalyzer;
  }

  /** NOTE: analyzer does not persist, so each time you create {@code FieldTypes} from
   *  {@linkIndexWriter} or {@link IndexReader} you must set all per-field analyzers again. */
  public synchronized void setQueryAnalyzer(String fieldName, Analyzer analyzer) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.queryAnalyzer = analyzer;
      fields.put(fieldName, current);
      changed();
    } else if (current.queryAnalyzer == null) {
      boolean success = false;
      try {
        current.queryAnalyzer = analyzer;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.queryAnalyzer = null;
        }
      }
      changed();
    } else {
      illegalState(fieldName, "analyzer was already set");
    }
  }

  public synchronized Analyzer getQueryAnalyzer(String fieldName) {
    return getFieldType(fieldName).queryAnalyzer;
  }

  /** NOTE: similarity does not persist, so each time you create {@code FieldTypes} from
   *  {@linkIndexWriter} or {@link IndexReader} you must set all per-field similarities again. */
  public synchronized void setSimilarity(String fieldName, Similarity similarity) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.similarity = similarity;
      fields.put(fieldName, current);
      changed();
    } else {
      // nocommit should we not allow this...
      current.similarity = similarity;
      changed();
    }
  }

  public synchronized Similarity getSimilarity(String fieldName) {
    return getFieldType(fieldName).similarity;
  }

  /** Notes that this field may have more than one value per document. */
  public synchronized void setMultiValued(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.multiValued = Boolean.TRUE;
      fields.put(fieldName, current);
      changed();
    } else if (current.multiValued == null) {
      boolean success = false;
      try {
        current.multiValued = Boolean.TRUE;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.multiValued = null;
        }
      }
      changed();
    } else if (current.multiValued == Boolean.FALSE) {
      illegalState(fieldName, "multiValued was already set to False");
    }
  }

  /** Returns true if this field may have more than one value per document. */
  public synchronized boolean getMultiValued(String fieldName) {
    return getFieldType(fieldName).multiValued == Boolean.TRUE;
  }

  /** The gap that should be added to token positions between each multi-valued field. */
  public synchronized void setAnalyzerPositionGap(String fieldName, int gap) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.analyzerPositionGap = gap;
      fields.put(fieldName, current);
      changed();
    } else if (current.analyzerPositionGap == null) {
      Integer oldValue = current.analyzerPositionGap;
      boolean success = false;
      try {
        current.analyzerPositionGap = gap;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.analyzerPositionGap = oldValue;
        }
      }
      changed();
    } else if (current.analyzerPositionGap.intValue() != gap) {
      illegalState(fieldName, "analyzerPositionGap was already set to " + current.analyzerPositionGap + "; cannot change again to " + gap);
    }
  }

  /** The gap that should be added to token positions between each multi-valued field. */
  public synchronized void setAnalyzerOffsetGap(String fieldName, int gap) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.analyzerOffsetGap = gap;
      fields.put(fieldName, current);
      changed();
    } else if (current.analyzerOffsetGap == null) {
      Integer oldValue = current.analyzerOffsetGap;
      boolean success = false;
      try {
        current.analyzerOffsetGap = gap;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.analyzerOffsetGap = oldValue;
        }
      }
      changed();
    } else if (current.analyzerOffsetGap.intValue() != gap) {
      illegalState(fieldName, "analyzerOffsetGap was already set to " + current.analyzerOffsetGap + "; cannot change again to " + gap);
    }
  }

  /** Sets the minimum number of terms in each term block in the terms dictionary.  These can be changed at any time, but changes only take
   *  effect for newly written (flushed or merged) segments.  The default is 25; higher values make fewer, larger blocks, which require less
   *  heap in the IndexReader but slows down term lookups. */
  public synchronized void setTermsDictBlockSize(String fieldName, int minItemsPerBlock) {
    setTermsDictBlockSize(fieldName, minItemsPerBlock, 2*(minItemsPerBlock-1));
  }

  /** Sets the minimum and maximum number of terms in each term block in the terms dictionary.  These can be changed at any time, but changes only take
   *  effect for newly written (flushed or merged) segments.  The default is 25 and 48; higher values make fewer, larger blocks, which require less
   *  heap in the IndexReader but slows down term lookups. */
  public synchronized void setTermsDictBlockSize(String fieldName, int minItemsPerBlock, int maxItemsPerBlock) {
    ensureWritable();

    try {
      BlockTreeTermsWriter.validateSettings(minItemsPerBlock, maxItemsPerBlock);
    } catch (IllegalArgumentException iae) {
      illegalState(fieldName, iae.getMessage());
    }

    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.blockTreeMinItemsInBlock = minItemsPerBlock;
      current.blockTreeMaxItemsInBlock = maxItemsPerBlock;
      fields.put(fieldName, current);
      changed();
    } else if (current.blockTreeMinItemsInBlock == null) {
      boolean success = false;
      try {
        current.blockTreeMinItemsInBlock = minItemsPerBlock;
        current.blockTreeMaxItemsInBlock = maxItemsPerBlock;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.blockTreeMinItemsInBlock = null;
          current.blockTreeMaxItemsInBlock = null;
        }
      }
      changed();
    } else {
      current.blockTreeMinItemsInBlock = minItemsPerBlock;
      current.blockTreeMaxItemsInBlock = maxItemsPerBlock;
      assert current.validate();
    }
  }

  /** Enables sorting for this field, using doc values of the appropriate type. */
  public synchronized void enableSorting(String fieldName) {
    enableSorting(fieldName, false);
  }

  public synchronized void enableSorting(String fieldName, boolean reversed) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.sortable = Boolean.TRUE;
      current.sortReversed = reversed;
      fields.put(fieldName, current);
      changed();
    } else if (current.sortable == null) {
      assert current.sortReversed == null;
      boolean success = false;
      try {
        current.sortable = Boolean.TRUE;
        current.sortReversed = reversed;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.sortable = null;
          current.sortReversed = null;
        }
      }
      changed();
    } else if (current.sortable == Boolean.FALSE) {
      illegalState(fieldName, "sorting was already disabled");
    } else if (current.sortReversed != reversed) {
      current.sortReversed = reversed;
      changed();
    }
  }

  /** Disables sorting for this field. */
  public synchronized void disableSorting(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.sortable = Boolean.FALSE;
      fields.put(fieldName, current);
      changed();
    } else if (current.sortable != Boolean.FALSE) {
      // nocommit don't we need to ... turn off DocValues if they were only on because of sorting?
      // nocommit ok to allow this?
      // nocommit should we validate?
      current.sortable = Boolean.FALSE;
      current.sortReversed = null;
      changed();
    }
  }

  public synchronized boolean getSorted(String fieldName) {
    return getFieldType(fieldName).sortable == Boolean.TRUE;
  }

  /** Enables sorting for this field, using doc values of the appropriate type. */
  public synchronized void enableFastRanges(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.fastRanges = Boolean.TRUE;
      fields.put(fieldName, current);
      changed();
    } else if (current.fastRanges == null) {
      boolean success = false;
      try {
        current.fastRanges = Boolean.TRUE;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.fastRanges = null;
        }
      }
      changed();
    } else if (current.fastRanges == Boolean.FALSE) {
      illegalState(fieldName, "fastRanges was already disabled");
    }
  }

  /** Disables sorting for this field. */
  public synchronized void disableFastRanges(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.fastRanges = Boolean.FALSE;
      fields.put(fieldName, current);
      changed();
    } else if (current.fastRanges != Boolean.FALSE) {
      // nocommit don't we need to ... turn off DocValues if they were only on because of sorting?
      // nocommit ok to allow this?
      // nocommit should we validate?
      current.fastRanges = Boolean.FALSE;
      changed();
    }
  }

  public synchronized boolean getFastRanges(String fieldName) {
    return getFieldType(fieldName).fastRanges == Boolean.TRUE;
  }

  /** Enables highlighting for this field, using postings highlighter. */
  public synchronized void enableHighlighting(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.highlighted = Boolean.TRUE;
      fields.put(fieldName, current);
      changed();
    } else if (current.highlighted == null) {
      boolean success = false;
      try {
        current.highlighted = Boolean.TRUE;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.highlighted = null;
        }
      }
      changed();
    } else if (current.highlighted == Boolean.FALSE) {
      illegalState(fieldName, "cannot enable highlighting: it was already disabled");
    }
  }

  /** Disables highlighting for this field. */
  public synchronized void disableHighlighting(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.highlighted = Boolean.FALSE;
      fields.put(fieldName, current);
      changed();
    } else if (current.highlighted == null) {
      Boolean currentValue = current.highlighted;
      boolean success = false;
      try {
        current.highlighted = Boolean.FALSE;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.highlighted = currentValue;
        }
      }
      changed();
    }
  }

  public synchronized boolean getHighlighted(String fieldName) {
    return getFieldType(fieldName).highlighted == Boolean.TRUE;
  }

  /** Enables norms for this field.  This is only allowed if norms were not already disabled. */
  public synchronized void enableNorms(String fieldName) {
    // throws exc if norms were already disabled
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.indexNorms = Boolean.TRUE;
      fields.put(fieldName, current);
      changed();
    } else if (current.indexNorms == null) {
      boolean success = false;
      try {
        current.indexNorms = Boolean.TRUE;
        success = true;
      } finally {
        if (success == false) {
          current.indexNorms = null;
        }
      }
      changed();
    } else if (current.indexNorms == Boolean.FALSE) {
      illegalState(fieldName, "cannot enable norms that were already disable");
    }
  }

  /** Disable norms for this field. */
  public synchronized void disableNorms(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.indexNorms = Boolean.FALSE;
      fields.put(fieldName, current);
      changed();
    } else if (current.indexNorms != Boolean.FALSE) {
      current.indexNorms = Boolean.FALSE;
      changed();
    }
  }

  public synchronized boolean getNorms(String fieldName) {
    return getFieldType(fieldName).indexNorms == Boolean.TRUE;
  }

  /** Store values for this field.  This can be changed at any time. */
  public void enableStored(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.stored = Boolean.TRUE;
      fields.put(fieldName, current);
      changed();
    } else if (current.stored != Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.stored = Boolean.TRUE;
      changed();
    }
  }

  /** Do not store values for this field.  This can be changed at any time. */
  public synchronized void disableStored(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.stored = Boolean.FALSE;
      fields.put(fieldName, current);
      changed();
    } else if (current.stored == null || current.stored == Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.stored = Boolean.FALSE;
      changed();
    }
  }

  /** Whether this field's value is stored. */
  public synchronized boolean getStored(String fieldName) {
    return getFieldType(fieldName).stored == Boolean.TRUE;
  }

  // nocommit iterator over all fields / types?

  public synchronized void setNumericPrecisionStep(String fieldName, int precStep) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      fields.put(fieldName, current);
      current.numericPrecisionStep = precStep;
      changed();
    } else if (current.numericPrecisionStep == null) {
      current.numericPrecisionStep = precStep;
      changed();
    } else if (current.numericPrecisionStep.intValue() != precStep) {
      illegalState(fieldName, "cannot change numericPrecisionStep from " + current.numericPrecisionStep + " to " + precStep);
    }
  }

  /** @throws IllegalStateException if this field is unknown, or is not a numeric field. */
  public synchronized int getNumericPrecisionStep(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.numericPrecisionStep == null) { 
      illegalState(fieldName, "no numericPrecisionStep is set");
    }
    return current.numericPrecisionStep;
  }

  // nocommit should we make a single method to enable the different combinations...?
  public synchronized void enableTermVectors(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.storeTermVectors = Boolean.TRUE;
      fields.put(fieldName, current);
      changed();
    } else if (current.storeTermVectors != Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectors = Boolean.TRUE;
      changed();
    }
  }

  public synchronized void disableTermVectors(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.storeTermVectors = Boolean.FALSE;
      fields.put(fieldName, current);
      changed();
    } else if (current.storeTermVectors != Boolean.FALSE) {
      // nocommit should this change not be allowed...
      current.storeTermVectors = Boolean.FALSE;
      changed();
    }
  }

  public synchronized boolean getTermVectors(String fieldName) {
    return getFieldType(fieldName).storeTermVectors == Boolean.TRUE;
  }

  public void enableTermVectorOffsets(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectors != Boolean.TRUE) {
      // nocommit we could enable term vectors for you?
      illegalState(fieldName, "cannot enable termVectorOffsets when termVectors haven't been enabled");
    }
    if (current.storeTermVectorOffsets != Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorOffsets = Boolean.TRUE;
      changed();
    }
  }

  public void disableTermVectorOffsets(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectorOffsets == Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorOffsets = Boolean.FALSE;
      changed();
    }
  }

  public boolean getTermVectorOffsets(String fieldName) {
    return getFieldType(fieldName).storeTermVectorOffsets == Boolean.TRUE;
  }

  public void enableTermVectorPositions(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectors != Boolean.TRUE) {
      // nocommit we could enable term vectors for you?
      illegalState(fieldName, "cannot enable termVectorPositions when termVectors haven't been enabled");
    }
    if (current.storeTermVectorPositions != Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorPositions = Boolean.TRUE;
      changed();
    }
  }

  public void disableTermVectorPositions(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectorPositions == Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorPositions = Boolean.FALSE;
      changed();
    }
  }

  public boolean getTermVectorPositions(String fieldName) {
    return getFieldType(fieldName).storeTermVectorPositions == Boolean.TRUE;
  }

  public void enableTermVectorPayloads(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectors != Boolean.TRUE) {
      // nocommit we could enable term vectors / positions for you?
      illegalState(fieldName, "cannot enable termVectorPayloads when termVectors haven't been enabled");
    }
    if (current.storeTermVectorPayloads != Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorPayloads = Boolean.TRUE;
      changed();
    }
  }

  public void disableTermVectorPayloads(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectorPayloads == Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorPayloads = Boolean.FALSE;
      changed();
    }
  }

  public boolean getTermVectorPayloads(String fieldName) {
    return getFieldType(fieldName).storeTermVectorPayloads == Boolean.TRUE;
  }

  /** Changes index options for this field.  This can be set to any
   *  value if it's not already set for the provided field; otherwise
   *  it can only be downgraded as low as DOCS_ONLY but never unset
   *  entirely (once indexed, always indexed). */
  public void setIndexOptions(String fieldName, IndexOptions indexOptions) {
    ensureWritable();
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.indexOptions = indexOptions;
      current.indexOptionsSet = true;
      fields.put(fieldName, current);
      changed();
    } else if (current.indexOptionsSet == false) {
      assert current.indexOptions == null;
      boolean success = false;
      try {
        current.indexOptions = indexOptions;
        current.indexOptionsSet = true;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.indexOptions = null;
          current.indexOptionsSet = false;
        }
      }
      changed();
    } else if (current.indexOptions != null && current.indexOptions != indexOptions) {
      assert current.indexOptionsSet;
      // Only allow downgrading IndexOptions:
      if (current.indexOptions.compareTo(indexOptions) < 0) {
        illegalState(fieldName, "cannot upgrade indexOptions from " + current.indexOptions + " to " + indexOptions);
      }
      current.indexOptions = indexOptions;
      changed();
    }
  }

  public IndexOptions getIndexOptions(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      return null;
    } else {
      return current.indexOptions;
    }
  }

  public synchronized void setDocValuesType(String fieldName, DocValuesType dvType) {
    ensureWritable();
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.docValuesType = dvType;
      current.docValuesTypeSet = true;
      fields.put(fieldName, current);
      changed();
    } else if (current.docValuesTypeSet == false) {
      boolean success = false;
      assert current.docValuesType == null;
      current.docValuesTypeSet = true;
      current.docValuesType = dvType;
      try {
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.docValuesType = null;
          current.docValuesTypeSet = false;
        }
      }
      changed();
    } else if (current.docValuesType != dvType) {
      illegalState(fieldName, "cannot change from docValuesType " + current.docValuesType + " to docValutesType " + dvType);
    }
  }

  public synchronized DocValuesType getDocValuesType(String fieldName, DocValuesType dvType) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      return null;
    } else {
      return current.docValuesType;
    }
  }

  synchronized void recordValueType(String fieldName, ValueType valueType) {
    ensureWritable();
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.valueType = valueType;
      fields.put(fieldName, current);
      setDefaults(current);
      changed();
    } else if (current.valueType == null) {
      // This can happen if e.g. the app first calls FieldTypes.setStored(...)
      boolean success = false;
      try {
        current.valueType = valueType;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.valueType = null;
        }
      }
      setDefaults(current);
      changed();
    } else if (current.valueType != valueType) {
      illegalState(fieldName, "cannot change from value type " + current.valueType + " to " + valueType);
    }
  }

  synchronized void recordLargeTextType(String fieldName, boolean allowStored, boolean indexed) {
    ensureWritable();
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = new FieldType(fieldName);
      current.valueType = ValueType.TEXT;
      fields.put(fieldName, current);
      setDefaults(current);
      if (allowStored == false) {
        current.stored = Boolean.FALSE;
      }
      if (indexed == false) {
        current.indexOptions = null;
      }
      changed();
    } else if (current.valueType == null) {
      // This can happen if e.g. the app first calls FieldTypes.setStored(...)
      boolean success = false;
      try {
        current.valueType = ValueType.TEXT;
        current.validate();
        if (allowStored == false && current.stored == Boolean.TRUE) {
          illegalState(fieldName, "can only store String large text fields");
        }
        success = true;
      } finally {
        if (success == false) {
          current.valueType = null;
        }
      }
      setDefaults(current);
      changed();
    } else if (current.valueType != ValueType.TEXT) {
      illegalState(fieldName, "cannot change from value type " + current.valueType + " to " + ValueType.TEXT);
    }
  }

  private void setDefaults(FieldType field) {
    switch (field.valueType) {

    case INT:
    case FLOAT:
    case LONG:
    case DOUBLE:
      if (field.highlighted == null) {
        field.highlighted = Boolean.FALSE;
      }
      if (field.fastRanges == null) {
        field.fastRanges = Boolean.TRUE;
      }
      if (field.sortable == null) {
        if (field.docValuesTypeSet == false || field.docValuesType == DocValuesType.NUMERIC || field.docValuesType == DocValuesType.SORTED_NUMERIC) {
          field.sortable = Boolean.TRUE;
        } else {
          field.sortable = Boolean.FALSE;
        }
      }
      if (field.multiValued == null) {
        field.multiValued = Boolean.FALSE;
      }
      if (field.stored == null) {
        field.stored = Boolean.TRUE;
      }
      if (field.sortable == Boolean.TRUE && field.docValuesTypeSet == false) {
        if (field.multiValued == Boolean.TRUE) {
          field.docValuesType = DocValuesType.SORTED_NUMERIC;
        } else {
          field.docValuesType = DocValuesType.NUMERIC;
        }
        field.docValuesTypeSet = true;
      }
      if (field.indexOptionsSet == false) {
        field.indexOptions = IndexOptions.DOCS_ONLY;
        field.indexOptionsSet = true;
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      if (field.numericPrecisionStep == null) {
        if (field.fastRanges == false) {
          field.numericPrecisionStep = Integer.MAX_VALUE;
        } else if (field.valueType == ValueType.INT || field.valueType == ValueType.FLOAT) {
          field.numericPrecisionStep = 8;
        } else {
          field.numericPrecisionStep = 16;
        }
      }
      break;

    case SHORT_TEXT:
      if (field.highlighted == null) {
        field.highlighted = Boolean.TRUE;
      }
      if (field.fastRanges == null) {
        field.fastRanges = Boolean.FALSE;
      }
      if (field.sortable == null) {
        if (field.docValuesTypeSet == false || field.docValuesType == DocValuesType.SORTED || field.docValuesType == DocValuesType.SORTED_SET) {
          field.sortable = Boolean.TRUE;
        } else {
          field.sortable = Boolean.FALSE;
        }
      }
      if (field.multiValued == null) {
        field.multiValued = Boolean.FALSE;
      }
      if (field.stored == null) {
        field.stored = Boolean.TRUE;
      }
      if (field.docValuesTypeSet == false) {
        if (field.sortable == Boolean.TRUE) {
          if (field.multiValued == Boolean.TRUE) {
            field.docValuesType = DocValuesType.SORTED_SET;
          } else {
            field.docValuesType = DocValuesType.SORTED;
          }
        }
        field.docValuesTypeSet = true;
      }
      if (field.indexOptionsSet == false) {
        if (field.highlighted) {
          field.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        } else {
          field.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        }
        field.indexOptionsSet = true;
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      break;

    case ATOM:
      if (field.highlighted == null) {
        field.highlighted = Boolean.FALSE;
      }
      if (field.fastRanges == null) {
        field.fastRanges = Boolean.FALSE;
      }
      if (field.sortable == null) {
        if (field.docValuesTypeSet == false || field.docValuesType == DocValuesType.SORTED || field.docValuesType == DocValuesType.SORTED_SET) {
          field.sortable = Boolean.TRUE;
        } else {
          field.sortable = Boolean.FALSE;
        }
      }
      if (field.multiValued == null) {
        field.multiValued = Boolean.FALSE;
      }
      if (field.stored == null) {
        field.stored = Boolean.TRUE;
      }
      if (field.indexOptionsSet == false) {
        field.indexOptions = IndexOptions.DOCS_ONLY;
        field.indexOptionsSet = true;
      }
      if (field.docValuesTypeSet == false) {
        if (field.sortable == Boolean.TRUE) {
          if (field.multiValued == Boolean.TRUE) {
            field.docValuesType = DocValuesType.SORTED_SET;
          } else {
            field.docValuesType = DocValuesType.SORTED;
          }
        }
        field.docValuesTypeSet = true;
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      break;

    case BINARY:
      if (field.highlighted == null) {
        field.highlighted = Boolean.FALSE;
      }
      if (field.fastRanges == null) {
        field.fastRanges = Boolean.FALSE;
      }
      if (field.sortable == null) {
        if (field.docValuesTypeSet == false || field.docValuesType == DocValuesType.SORTED || field.docValuesType == DocValuesType.SORTED_SET) {
          field.sortable = Boolean.TRUE;
        } else {
          field.sortable = Boolean.FALSE;
        }
      }
      if (field.multiValued == null) {
        field.multiValued = Boolean.FALSE;
      }
      if (field.stored == null) {
        field.stored = Boolean.TRUE;
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      if (field.indexOptionsSet == false) {
        assert field.indexOptions == null;
        field.indexOptionsSet = true;
      }
      if (field.docValuesTypeSet == false) {
        if (field.sortable == Boolean.TRUE) {
          if (field.multiValued == Boolean.TRUE) {
            field.docValuesType = DocValuesType.SORTED_SET;
          } else {
            field.docValuesType = DocValuesType.SORTED;
          }
        }
        field.docValuesTypeSet = true;
      }
      break;

    case TEXT:
      if (field.highlighted == null) {
        field.highlighted = Boolean.TRUE;
      }
      if (field.fastRanges == null) {
        field.fastRanges = Boolean.FALSE;
      }
      if (field.sortable == null) {
        field.sortable = Boolean.FALSE;
      }
      if (field.multiValued == null) {
        field.multiValued = Boolean.FALSE;
      }
      if (field.stored == null) {
        field.stored = Boolean.TRUE;
      }
      if (field.indexOptionsSet == false) {
        if (field.highlighted) {
          field.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        } else {
          field.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        }
        field.indexOptionsSet = true;
      }

      // nocommit is this always a bug if not?  validate should prevent it...
      assert field.docValuesType == null;
      field.docValuesTypeSet = true;

      if (field.indexNorms == null) {
        field.indexNorms = Boolean.TRUE;
      }
      break;

    default:
      throw new AssertionError("missing value type in switch");
    }

    assert field.highlighted != null;
    assert field.fastRanges != null;
    assert field.sortable != null;
    assert field.multiValued != null;
    assert field.stored != null;
    assert field.indexOptionsSet;
    assert field.docValuesTypeSet;
    assert field.indexOptions == null || field.indexNorms != null;
    assert field.validate();
  } 

  /** Returns a query matching all documents that have this int term. */
  public Query newTermQuery(String fieldName, int token) {
    // nocommit should we take Number?

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == null) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    BytesRefBuilder bytesBuilder = new BytesRefBuilder();

    switch (fieldType.valueType) {
    case INT:
      NumericUtils.intToPrefixCodedBytes(token, 0, bytesBuilder);
      break;
    case LONG:
      NumericUtils.longToPrefixCodedBytes(token, 0, bytesBuilder);
      break;
    default:
      illegalState(fieldName, "cannot create int term query when valueType=" + fieldType.valueType);
    }
    return new TermQuery(new Term(fieldName, bytesBuilder.get()));
  }

  /** Returns a query matching all documents that have this long term. */
  public Query newTermQuery(String fieldName, long token) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == null) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    BytesRefBuilder bytesBuilder = new BytesRefBuilder();

    switch (fieldType.valueType) {
    case LONG:
      NumericUtils.longToPrefixCodedBytes(token, 0, bytesBuilder);
      break;
    default:
      illegalState(fieldName, "cannot create long term query when valueType=" + fieldType.valueType);
    }

    return new TermQuery(new Term(fieldName, bytesBuilder.get()));
  }

  /** Returns a query matching all documents that have this binary token. */
  public Query newTermQuery(String fieldName, byte[] token) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == null) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    // Field must be binary:
    if (fieldType.valueType != ValueType.BINARY && fieldType.valueType != ValueType.ATOM) {
      illegalState(fieldName, "binary term query must have valueType BINARY or ATOM; got " + fieldType.valueType);
    }

    return new TermQuery(new Term(fieldName, new BytesRef(token)));
  }

  public Query newTermQuery(String fieldName, String token) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == null) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    // Field must be text:
    if (fieldType.valueType != ValueType.TEXT && fieldType.valueType != ValueType.SHORT_TEXT && fieldType.valueType != ValueType.ATOM) {
      illegalState(fieldName, "text term query have valueType TEXT, SHORT_TEXT or ATOM; got " + fieldType.valueType);
    }

    return new TermQuery(new Term(fieldName, token));
  }

  public Query newRangeQuery(String fieldName, Number min, boolean minInclusive, Number max, boolean maxInclusive) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == null) {
      illegalState(fieldName, "cannot create range query: this field was not indexed");
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "this field was not indexed for fast ranges");
    }

    // nocommit should we really take Number here?  it's too weakly typed?  you could ask for float range on an int field?  should we
    // instead make separate methods for each atomic type?  or should we "type check" the incoming Number?  taking Number is more
    // conventient for query parsers...?

    switch (fieldType.valueType) {
    case INT:
      return NumericRangeQuery.newIntRange(fieldName,
                                           fieldType.numericPrecisionStep.intValue(),
                                           min == null ? null : min.intValue(),
                                           max == null ? null : max.intValue(),
                                           minInclusive, maxInclusive);
    case FLOAT:
      return NumericRangeQuery.newFloatRange(fieldName,
                                             fieldType.numericPrecisionStep.intValue(),
                                             min == null ? null : min.floatValue(),
                                             max == null ? null : max.floatValue(),
                                             minInclusive, maxInclusive);
    case LONG:
      return NumericRangeQuery.newLongRange(fieldName,
                                            fieldType.numericPrecisionStep.intValue(),
                                            min == null ? null : min.longValue(),
                                            max == null ? null : max.longValue(),
                                            minInclusive, maxInclusive);
    case DOUBLE:
      return NumericRangeQuery.newDoubleRange(fieldName,
                                              fieldType.numericPrecisionStep.intValue(),
                                              min == null ? null : min.doubleValue(),
                                              max == null ? null : max.doubleValue(),
                                              minInclusive, maxInclusive);
      // nocommit termRangeQuery?  but we should add enableRangeQueries and check against that?
    default:
      illegalState(fieldName, "cannot create numeric range query on non-numeric field; got valueType=" + fieldType.valueType);

      // Dead code but javac disagrees:
      return null;
    }
  }

  // nocommit newPhraseQuery?

  /** Builds a sort from arbitrary list of fieldName, reversed pairs. */
  public Sort newSort(Object... fields) {
    if (fields.length == 0) {
      throw new IllegalArgumentException("must sort by at least one field; got nothing");
    }

    int upto = 0;
    List<SortField> sortFields = new ArrayList<>();

    while (upto < fields.length) {
      if ((fields[upto] instanceof String) == false) {
        throw new IllegalArgumentException("arguments must (String [Boolean])+; expected String but got: " + fields[upto].getClass());
      }
      String fieldName = (String) fields[upto++];
      Boolean reversed;
      if (upto == fields.length || (fields[upto] instanceof Boolean) == false) {
        reversed = null;
      } else {
        reversed = (Boolean) fields[upto+1];
        upto++;
      }
      sortFields.add(newSortField(fieldName, reversed));
    }

    return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
  }

  /** Returns the SortField for this field. */
  public SortField newSortField(String fieldName) {
    return newSortField(fieldName, false);
  }

  /** Returns the SortField for this field, optionally reversed.  If reverse is null, we use the default for the field. */
  public SortField newSortField(String fieldName, Boolean reverse) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);
    if (fieldType.sortable != Boolean.TRUE) {
      illegalState(fieldName, "this field was not indexed for sorting");
    }
    if (reverse == null) {
      reverse = fieldType.sortReversed;
    }
    if (reverse == null) {
      reverse = Boolean.FALSE;
    }
    switch (fieldType.valueType) {
    case INT:
      if (fieldType.multiValued == Boolean.TRUE) {
        return new SortedNumericSortField(fieldName, SortField.Type.INT, reverse);
      } else {
        return new SortField(fieldName, SortField.Type.INT, reverse);
      }
    case FLOAT:
      if (fieldType.multiValued == Boolean.TRUE) {
        // nocommit need to be able to set selector...
        return new SortedNumericSortField(fieldName, SortField.Type.FLOAT, reverse);
      } else {
        return new SortField(fieldName, SortField.Type.FLOAT, reverse);
      }
    case LONG:
      if (fieldType.multiValued == Boolean.TRUE) {
        // nocommit need to be able to set selector...
        return new SortedNumericSortField(fieldName, SortField.Type.LONG, reverse);
      } else {
        return new SortField(fieldName, SortField.Type.LONG, reverse);
      }
    case DOUBLE:
      if (fieldType.multiValued == Boolean.TRUE) {
        // nocommit need to be able to set selector...
        return new SortedNumericSortField(fieldName, SortField.Type.DOUBLE, reverse);
      } else {
        return new SortField(fieldName, SortField.Type.DOUBLE, reverse);
      }
    case SHORT_TEXT:
    case ATOM:
    case BINARY:
      if (fieldType.multiValued == Boolean.TRUE) {
        // nocommit need to be able to set selector...
        return new SortedSetSortField(fieldName, reverse);
      } else {
        return new SortField(fieldName, SortField.Type.STRING, reverse);
      }
    default:
      // BUG
      illegalState(fieldName, "unhandled sort case, valueType=" + fieldType.valueType);

      // Dead code but javac disagrees:
      return null;
    }
  }     

  private synchronized void changed() {
    ensureWritable();
    // Push to IW's commit data
  }

  private synchronized void ensureWritable() {
    if (readOnly) {
      throw new IllegalStateException("cannot make changes to a read-only FieldTypes (it was opened from an IndexReader, not an IndexWriter)");
    }
  }

  static void illegalState(String fieldName, String message) {
    throw new IllegalStateException("field \"" + fieldName + "\": " + message);
  }

  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;
  public static String CODEC_NAME = "FieldTypes";

  // nocommit should this be under codec control...
  public synchronized String writeToString() throws IOException {
    RAMFile file = new RAMFile();
    RAMOutputStream out = new RAMOutputStream(file, true);
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    
    out.writeInt(indexCreatedVersion.major);
    out.writeInt(indexCreatedVersion.minor);
    out.writeInt(indexCreatedVersion.bugfix);

    out.writeVInt(fields.size());
    for(FieldType fieldType : fields.values()) {
      fieldType.write(out);
    }

    CodecUtil.writeFooter(out);

    out.close();
    byte[] bytes = new byte[(int) out.getFilePointer()];
    RAMInputStream in = new RAMInputStream("FieldTypes", file);
    in.readBytes(bytes, 0, bytes.length);
    return bytesToString(bytes);
  }

  /** Reads FieldTypes from previously saved. */
  private synchronized Version readFromString(String stringIn) throws IOException {
    byte[] bytesIn = stringToBytes(stringIn);
    RAMFile file = new RAMFile();
    RAMOutputStream out = new RAMOutputStream(file, false);
    out.writeBytes(bytesIn, 0, bytesIn.length);
    out.close();
    RAMInputStream ris = new RAMInputStream("FieldTypes", file);
    ChecksumIndexInput in = new BufferedChecksumIndexInput(ris);

    CodecUtil.checkHeader(in, CODEC_NAME, VERSION_START, VERSION_START);

    Version indexCreatedVersion = Version.fromBits(in.readInt(), in.readInt(), in.readInt());

    int count = in.readVInt();
    for(int i=0;i<count;i++) {
      FieldType fieldType = new FieldType(in);
      fields.put(fieldType.name, fieldType);
    }

    CodecUtil.checkFooter(in);

    return indexCreatedVersion;
  }

  public Version getIndexCreatedVersion() {
    return indexCreatedVersion;
  }

  // nocommit somehow move this to IndexReader?
  public static FieldTypes getFieldTypes(Directory dir, Analyzer defaultQueryAnalyzer) throws IOException {
    SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    return getFieldTypes(infos.getUserData(), defaultQueryAnalyzer, IndexSearcher.getDefaultSimilarity());
  }

  // nocommit somehow move this to IndexReader?
  public static FieldTypes getFieldTypes(IndexCommit commit, Analyzer defaultQueryAnalyzer) throws IOException {
    return getFieldTypes(commit, defaultQueryAnalyzer, IndexSearcher.getDefaultSimilarity());
  }

  // nocommit somehow move this to IndexReader?
  public static FieldTypes getFieldTypes(IndexCommit commit, Analyzer defaultQueryAnalyzer, Similarity defaultSimilarity) throws IOException {
    return getFieldTypes(commit.getUserData(), defaultQueryAnalyzer, defaultSimilarity);
  }

  // nocommit somehow move this to IndexReader?
  public static FieldTypes getFieldTypes(Map<String,String> commitUserData, Analyzer defaultQueryAnalyzer, Similarity defaultSimilarity) throws IOException {
    return new FieldTypes(commitUserData, defaultQueryAnalyzer, defaultSimilarity);
  }

  public Iterable<String> getFieldNames() {
    return Collections.unmodifiableSet(fields.keySet());
  }

  // nocommit on exception (mismatched schema), this should ensure no changes were actually made:
  public void addAll(FieldTypes in) {
    for (FieldType fieldType : in.fields.values()) {
      FieldType curFieldType = fields.get(fieldType.name);
      if (curFieldType == null) {
        // nocommit must clone?:
        fields.put(fieldType.name, fieldType);
      } else {
        // nocommit must merge / check here!!  and fail if inconsistent ... and needs tests showing this:
      }
    }
  }
}
