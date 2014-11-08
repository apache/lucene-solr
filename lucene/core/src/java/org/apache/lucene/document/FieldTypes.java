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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.codecs.lucene50.Lucene50Codec;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TermRangeQuery;
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
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

// TODO
//   - explore what it'd be like to add other higher level types?
//     - BigInt, BigDecimal, IPV6
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
//     - creating queries, catching invalid field names, no positions indexed, etc.
//     - prox queries can verify field was indexed w/ positions
//   - move analyzer out of IW/IWC into Field/FieldType/s only?
//   - why does STS fill offset...

// Lucene's secret schemas
//   StoredFieldsVisitor
//   FieldInfos/GlobalFieldNumbers
//   SortField.type
//   DocValuesType
//   subclassing QueryParsers
//   PerFieldPF/DVF
//   PerFieldSimilarityWrapper
//   PerFieldAnalyzerWrapper
//   oal.document

// nocommit byte, short?

// nocommit allow adding array of atom values?  fieldnamesfield would use it?

// nocommit optimize field exists filter to MatchAllBits when all docs in the seg have the field; same opto as range query when min < terms.min & max > terms.max

// nocommit use better pf when field is unique

// nocommit filter caching?  parent docs filter?

// nocommit do we allow mixing of binary and non-binary atom?

// nocommit index field names the doc has?

// nocommit fix simple qp to optionally take this?

// nocommit optimize term range query when it's really "matches all docs"?

// nocommit but how can we randomized IWC for tests?

// nocommit maybe we should filter out our key from commit user data?  scary if apps can ... mess with it accidentally?  or just store it
// directly in SIS or someplace else

// nocommit move to oal.index?

// nocommit per-field norms format?  then we can commit these "tradeoffs"

// nocommit default value?

// nocommit can we have test infra that randomly reopens writer?

// nocommit getTermFilter?

// nocommit facets?

// nocommit live values?

// nocommit expr fields?

// nocommit default qp operator

// nocommit copy field?

// nocommit sort proxy field?

// nocommit controlling compression of stored fields, norms

// nocommit can we somehow detect at search time if the field types you are using doesn't match the searcher you are now searching against?

// nocommit back compat: how to handle pre-schema indices

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

// nocommit can we require use of analyzer factories?

// nocommit what schema options does solr/ES offer

// nocommit accent removal and lowercasing for wildcards should just work

// separate analyzer for phrase queries in suggesters

// nocommit Index class?  enforcing unique id, xlog?

// nocommit how to randomize IWC?  RIW?

// nocommit unique/primary key ?

// nocommit fix all change methods to call validate / rollback

// nocommit float16?

// nocommit can we move multi-field-ness out of IW?  so IW only gets a single instance of each field

/** Records how each field is indexed, stored, etc.  This class persists
 *  its state using {@link IndexWriter#setCommitData}, using the
 *  {@link FieldTypes#FIELD_PROPERTIES_KEY} key. */

public class FieldTypes {

  enum ValueType {
    NONE,
    TEXT,
    SHORT_TEXT,
    ATOM,  // nocommit binary sort of overlaps w/ this?
    UNIQUE_ATOM,  // nocommit binary sort of overlaps w/ this?
    INT,
    FLOAT,
    LONG,
    DOUBLE,
    BINARY, // nocommit rename to bytes?
    BOOLEAN,
    DATE,
    INET_ADDRESS,
    // nocommit primary_key?
  }

  private final boolean readOnly;

  public static final String FIELD_NAMES_FIELD = "$fieldnames";

  /** So exists filters are fast */
  boolean enableExistsFilters = true;
  private boolean indexedDocs;

  private final Version indexCreatedVersion;

  final Map<String,FieldType> fields = new HashMap<>();

  // Null when we are readOnly:
  private final Analyzer defaultIndexAnalyzer;

  // Null when we are not readOnly:
  private final Analyzer defaultQueryAnalyzer;

  private final Similarity defaultSimilarity;

  /** Used only in memory to record when something changed. */
  private long changeCount;

  // nocommit nested docs?

  // nocommit required?  not null?

  /** Just like current oal.document.FieldType, except for each setting it can also record "not-yet-set". */
  static class FieldType implements IndexableFieldType {
    private final String name;

    // Lucene version when we were created:
    private final Version createdVersion;

    public FieldType(String name) {
      this(name, Version.LATEST);
    }

    public FieldType(String name, Version version) {
      this.name = name;
      this.createdVersion = version;
    }

    // nocommit don't use null here:
    volatile ValueType valueType = ValueType.NONE;
    volatile DocValuesType docValuesType = DocValuesType.NONE;
    private volatile boolean docValuesTypeSet;

    // Expert: settings we pass to BlockTree to control how many terms are allowed in each block and auto-prefix term
    volatile Integer blockTreeMinItemsInBlock;
    volatile Integer blockTreeMaxItemsInBlock;
    volatile Integer blockTreeMinItemsInAutoPrefix;
    volatile Integer blockTreeMaxItemsInAutoPrefix;

    // Gaps to add between multiple values of the same field; if these are not set, we fallback to the Analyzer for that field.
    volatile Integer analyzerPositionGap;
    volatile Integer analyzerOffsetGap;

    // nocommit should we default max token length to ... 256?

    // Min/max token length, or null if there are no limits:
    volatile Integer minTokenLength;
    volatile Integer maxTokenLength;

    // Limit on number of tokens to index for this field
    volatile Integer maxTokenCount;
    volatile Boolean consumeAllTokens;

    // Whether this field's values are stored, or null if it's not yet set:
    private volatile Boolean stored;

    // Whether this field's values should be indexed for sorting (using doc values):
    private volatile Boolean sortable;
    private volatile Boolean sortReversed;
    private volatile Boolean sortMissingLast = Boolean.TRUE;

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
    private volatile IndexOptions indexOptions = IndexOptions.NONE;
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

    private volatile Analyzer wrappedIndexAnalyzer;
    private volatile Analyzer wrappedQueryAnalyzer;

    boolean validate() {
      switch (valueType) {
      case NONE:
        break;
      case INT:
      case FLOAT:
      case LONG:
      case DOUBLE:
      case DATE:
        if (highlighted == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot highlight");
        }
        if (indexAnalyzer != null) {
          illegalState(name, "type " + valueType + " cannot have an indexAnalyzer");
        }
        if (queryAnalyzer != null) {
          illegalState(name, "type " + valueType + " cannot have a queryAnalyzer");
        }
        if (docValuesType != DocValuesType.NONE && (docValuesType != DocValuesType.NUMERIC && docValuesType != DocValuesType.SORTED_NUMERIC)) {
          illegalState(name, "type " + valueType + " must use NUMERIC docValuesType (got: " + docValuesType + ")");
        }
        if (indexOptions != IndexOptions.NONE && indexOptions.compareTo(IndexOptions.DOCS) > 0) {
          illegalState(name, "type " + valueType + " cannot use indexOptions > DOCS (got indexOptions " + indexOptions + ")");
        }
        if (indexNorms == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot index norms");
        }
        if (minTokenLength != null) {
          illegalState(name, "type " + valueType + " cannot set min/max token length");
        }
        if (maxTokenCount != null) {
          illegalState(name, "type " + valueType + " cannot set max token count");
        }
        break;
      case TEXT:
        if (sortable == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot sort");
        }
        if (fastRanges == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot optimize for range queries");
        }
        if (docValuesType != DocValuesType.NONE) {
          illegalState(name, "type " + valueType + " cannot use docValuesType " + docValuesType);
        }
        break;
      case SHORT_TEXT:
        if (docValuesType != DocValuesType.NONE && docValuesType != DocValuesType.BINARY && docValuesType != DocValuesType.SORTED && docValuesType != DocValuesType.SORTED_SET) {
          illegalState(name, "type " + valueType + " cannot use docValuesType " + docValuesType);
        }
        if (fastRanges == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot optimize for range queries");
        }
        break;
      case BINARY:
      case INET_ADDRESS:
        if (highlighted == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot highlight");
        }
        if (indexAnalyzer != null) {
          illegalState(name, "type " + valueType + " cannot have an indexAnalyzer");
        }
        if (queryAnalyzer != null) {
          illegalState(name, "type " + valueType + " cannot have a queryAnalyzer");
        }
        if (docValuesType != DocValuesType.NONE && docValuesType != DocValuesType.BINARY && docValuesType != DocValuesType.SORTED && docValuesType != DocValuesType.SORTED_SET) {
          illegalState(name, "type " + valueType + " must use BINARY, SORTED or SORTED_SET docValuesType (got: " + docValuesType + ")");
        }
        if (indexOptions != IndexOptions.NONE && indexOptions.compareTo(IndexOptions.DOCS) > 0) {
          // nocommit too anal?
          illegalState(name, "type " + valueType + " can only be indexed as DOCS_ONLY; got " + indexOptions);
        }
        if (minTokenLength != null) {
          illegalState(name, "type " + valueType + " cannot set min/max token length");
        }
        if (maxTokenCount != null) {
          illegalState(name, "type " + valueType + " cannot set max token count");
        }
        break;
      case ATOM:
      case UNIQUE_ATOM:
        if (highlighted == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot highlight");
        }
        if (indexAnalyzer != null) {
          illegalState(name, "type " + valueType + " cannot have an indexAnalyzer");
        }
        if (queryAnalyzer != null) {
          illegalState(name, "type " + valueType + " cannot have a queryAnalyzer");
        }
        if (indexNorms == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot index norms");
        }
        if (indexOptions != IndexOptions.NONE && indexOptions.compareTo(IndexOptions.DOCS) > 0) {
          illegalState(name, "type " + valueType + " can only be indexed as DOCS; got " + indexOptions);
        }
        if (valueType == ValueType.UNIQUE_ATOM) {
          if (indexOptions != IndexOptions.DOCS) {
            illegalState(name, "type " + valueType + " must be indexed as DOCS; got " + indexOptions);
          }
          if (multiValued == Boolean.TRUE) {
            illegalState(name, "type " + valueType + " cannot be multivalued");
          }
        }
        if (maxTokenCount != null) {
          illegalState(name, "type " + valueType + " cannot set max token count");
        }
        break;
      case BOOLEAN:
        if (highlighted == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot highlight");
        }
        if (indexNorms == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot index norms");
        }
        if (docValuesType != DocValuesType.NONE && docValuesType != DocValuesType.NUMERIC && docValuesType != DocValuesType.SORTED_NUMERIC) {
          illegalState(name, "type " + valueType + " must use NUMERIC or SORTED_NUMERIC docValuesType (got: " + docValuesType + ")");
        }
        if (minTokenLength != null) {
          illegalState(name, "type " + valueType + " cannot set min/max token length");
        }
        if (maxTokenCount != null) {
          illegalState(name, "type " + valueType + " cannot set max token count");
        }
        break;
      default:
        throw new AssertionError("missing value type in switch");
      }

      // nocommit more checks

      if (multiValued == Boolean.TRUE &&
          (docValuesType == DocValuesType.NUMERIC ||
           docValuesType == DocValuesType.SORTED ||
           docValuesType == DocValuesType.BINARY)) {
        illegalState(name, "DocValuesType=" + docValuesType + " cannot be multi-valued");
      }

      if (sortable == Boolean.TRUE && (docValuesTypeSet && (docValuesType == DocValuesType.NONE || docValuesType == DocValuesType.BINARY))) {
        illegalState(name, "cannot sort when DocValuesType=" + docValuesType);
      }

      if (indexOptions == IndexOptions.NONE) {
        if (blockTreeMinItemsInBlock != null) {
          illegalState(name, "can only setTermsDictBlockSize if the field is indexed");
        }
        if (blockTreeMinItemsInAutoPrefix != null) {
          illegalState(name, "can only setTermsDictAutoPrefixSize if the field is indexed");
        }
        if (indexAnalyzer != null) {
          illegalState(name, "can only setIndexAnalyzer if the field is indexed");
        }
        if (queryAnalyzer != null) {
          illegalState(name, "can only setQueryAnalyzer if the field is indexed");
        }
        if (fastRanges == Boolean.TRUE) {
          illegalState(name, "can only enableFastRanges if the field is indexed");
        }
      } else {
        if (valueType != ValueType.TEXT && valueType != ValueType.SHORT_TEXT && indexAnalyzer != null) {
          illegalState(name, "can only setIndexAnalyzer for short text and large text fields; got valueType=" + valueType);
        }
        if (valueType != ValueType.TEXT && valueType != ValueType.SHORT_TEXT && queryAnalyzer != null) {
          illegalState(name, "can only setQueryAnalyzer for short text and large text fields; got valueType=" + valueType);
        }
      }

      // nocommit must check that if fastRanges is on, you have a PF that supports it

      if (analyzerPositionGap != null) {
        if (indexOptions == IndexOptions.NONE) {
          illegalState(name, "can only setAnalyzerPositionGap if the field is indexed");
        }
        if (multiValued != Boolean.TRUE) {
          illegalState(name, "can only setAnalyzerPositionGap if the field is multi-valued");
        }
      }
      if (analyzerOffsetGap != null) {
        if (indexOptions == IndexOptions.NONE) {
          illegalState(name, "can only setAnalyzerOffsetGap if the field is indexed");
        }
        if (multiValued != Boolean.TRUE) {
          illegalState(name, "can only setAnalyzerOffsetGap if the field is multi-valued");
        }
      }

      if (postingsFormat != null && blockTreeMinItemsInBlock != null) {
        illegalState(name, "cannot use both setTermsDictBlockSize and setPostingsFormat");
      }

      if (postingsFormat != null && blockTreeMinItemsInAutoPrefix != null) {
        illegalState(name, "cannot use both setTermsDictAutoPrefixSize and setPostingsFormat");
      }

      if (highlighted == Boolean.TRUE) {
        if (valueType != ValueType.TEXT && valueType != ValueType.SHORT_TEXT) {
          illegalState(name, "can only enable highlighting for TEXT or SHORT_TEXT fields; got valueType=" + valueType);
        }
        if (indexOptions != IndexOptions.NONE && indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
          illegalState(name, "must index with IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS when highlighting is enabled");
        }
      }

      return true;
    }

    private boolean needsWrapping() {
      return minTokenLength != null || maxTokenCount != null;
    }

    void reWrapAnalyzers(Analyzer defaultIndexAnalyzer, Analyzer defaultQueryAnalyzer) {
      // nocommit need test to verify wrapping for ATOM fields works correctly
      if (needsWrapping()) {
        if (indexAnalyzer != null) {
          wrappedIndexAnalyzer = wrapAnalyzer(indexAnalyzer);
        } else if (defaultIndexAnalyzer != null) {
          wrappedIndexAnalyzer = wrapAnalyzer(defaultIndexAnalyzer);
        } else {
          wrappedIndexAnalyzer = null;
        }
        if (queryAnalyzer != null) {
          wrappedQueryAnalyzer = wrapAnalyzer(queryAnalyzer);
        } else if (defaultQueryAnalyzer != null) {
          wrappedQueryAnalyzer = wrapAnalyzer(defaultQueryAnalyzer);
        } else {
          wrappedQueryAnalyzer = null;
        }
      } else {
        wrappedIndexAnalyzer = null;
        wrappedQueryAnalyzer = null;
      }
    }

    private Analyzer wrapAnalyzer(final Analyzer in) {
      return new AnalyzerWrapper(in.getReuseStrategy()) {
        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
          return in;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
          TokenStream end = components.getTokenStream();
          if (minTokenLength != null) {
            end = new LengthFilter(end,
                                   minTokenLength.intValue(),
                                   maxTokenLength.intValue());
          }
          if (maxTokenCount != null) {
            end = new LimitTokenCountFilter(end, maxTokenCount.intValue(), consumeAllTokens.booleanValue());
          }

          return new TokenStreamComponents(components.getTokenizer(), end);
        }
      };
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("field \"");
      b.append(name);
      b.append("\":\n");
      b.append("  valueType: ");
      b.append(valueType);
      b.append('\n');

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
        if (sortMissingLast == Boolean.TRUE) {
          b.append(" (missing: last)");
        } else if (sortMissingLast == Boolean.FALSE) {
          b.append(" (missing: first)");
        }
      } else {
        b.append("unset");
      }
      b.append('\n');

      b.append("  fastRanges: ");
      if (fastRanges != null) {
        b.append(fastRanges);
        if (fastRanges == Boolean.TRUE) {
          if (blockTreeMinItemsInAutoPrefix != null) {
            b.append(" (auto-prefix blocks: ");
            b.append(blockTreeMinItemsInAutoPrefix);
            b.append(" - ");
            b.append(blockTreeMaxItemsInAutoPrefix);
            b.append(")");
          }
        }
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
      } else if (docValuesType == DocValuesType.NONE) {
        b.append("disabled");
      } else {
        b.append(docValuesType);
      }
      b.append('\n');

      b.append("  indexOptions: ");
      if (indexOptionsSet == false) {
        b.append("unset");
      } else if (indexOptions == IndexOptions.NONE) {
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
        if (minTokenLength != null) {
          b.append("\n  token length limit min=");
          b.append(minTokenLength);
          b.append(" max");
          b.append(maxTokenLength);
        }
        if (maxTokenCount != null) {
          b.append("\n  token count limit=");
          b.append(maxTokenCount);
          b.append(" consumeAllTokens=");
          b.append(consumeAllTokens);
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

      out.writeVInt(createdVersion.major);
      out.writeVInt(createdVersion.minor);
      out.writeVInt(createdVersion.bugfix);

      switch (valueType) {
      case NONE:
        out.writeByte((byte) 0);
        break;
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
      case BOOLEAN:
        out.writeByte((byte) 9);
        break;
      case DATE:
        out.writeByte((byte) 10);
        break;
      case INET_ADDRESS:
        out.writeByte((byte) 11);
        break;
      case UNIQUE_ATOM:
        out.writeByte((byte) 12);
        break;
      default:
        throw new AssertionError("missing ValueType in switch");
      }

      if (docValuesTypeSet == false) {
        assert docValuesType == DocValuesType.NONE;
        out.writeByte((byte) 0);
      } else {
        switch (docValuesType) {
        case NONE:
          out.writeByte((byte) 1);
          break;
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
      writeNullableInteger(out, blockTreeMinItemsInAutoPrefix);
      writeNullableInteger(out, blockTreeMaxItemsInAutoPrefix);
      writeNullableInteger(out, analyzerPositionGap);
      writeNullableInteger(out, analyzerOffsetGap);
      writeNullableInteger(out, minTokenLength);
      writeNullableInteger(out, maxTokenLength);
      writeNullableInteger(out, maxTokenCount);
      writeNullableBoolean(out, consumeAllTokens);
      writeNullableBoolean(out, stored);
      writeNullableBoolean(out, sortable);
      writeNullableBoolean(out, sortReversed);
      writeNullableBoolean(out, sortMissingLast);
      writeNullableBoolean(out, multiValued);
      writeNullableBoolean(out, indexNorms);
      writeNullableBoolean(out, fastRanges);
      writeNullableBoolean(out, storeTermVectors);
      writeNullableBoolean(out, storeTermVectorPositions);
      writeNullableBoolean(out, storeTermVectorOffsets);
      writeNullableBoolean(out, storeTermVectorPayloads);

      if (indexOptionsSet == false) {
        assert indexOptions == IndexOptions.NONE;
        out.writeByte((byte) 0);
      } else {
        switch(indexOptions) {
        case NONE:
          out.writeByte((byte) 1);
          break;
        case DOCS:
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
      createdVersion = Version.fromBits(in.readVInt(), in.readVInt(), in.readVInt());

      byte b = in.readByte();
      switch (b) {
      case 0:
        valueType = ValueType.NONE;
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
      case 9:
        valueType = ValueType.BOOLEAN;
        break;
      case 10:
        valueType = ValueType.DATE;
        break;
      case 11:
        valueType = ValueType.INET_ADDRESS;
        break;
      case 12:
        valueType = ValueType.UNIQUE_ATOM;
        break;
      default:
        throw new CorruptIndexException("invalid byte for ValueType: " + b, in);
      }

      b = in.readByte();
      switch (b) {
      case 0:
        docValuesTypeSet = false;
        docValuesType = DocValuesType.NONE;
        break;
      case 1:
        docValuesTypeSet = true;
        docValuesType = DocValuesType.NONE;
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
      blockTreeMinItemsInAutoPrefix = readNullableInteger(in);
      blockTreeMaxItemsInAutoPrefix = readNullableInteger(in);
      analyzerPositionGap = readNullableInteger(in);
      analyzerOffsetGap = readNullableInteger(in);
      minTokenLength = readNullableInteger(in);
      maxTokenLength = readNullableInteger(in);
      maxTokenCount = readNullableInteger(in);
      consumeAllTokens = readNullableBoolean(in);
      stored = readNullableBoolean(in);
      sortable = readNullableBoolean(in);
      sortReversed = readNullableBoolean(in);
      sortMissingLast = readNullableBoolean(in);
      multiValued = readNullableBoolean(in);
      indexNorms = readNullableBoolean(in);
      fastRanges = readNullableBoolean(in);
      storeTermVectors = readNullableBoolean(in);
      storeTermVectorPositions = readNullableBoolean(in);
      storeTermVectorOffsets = readNullableBoolean(in);
      storeTermVectorPayloads = readNullableBoolean(in);

      b = in.readByte();
      switch (b) {
      case 0:
        indexOptionsSet = false;
        indexOptions = IndexOptions.NONE;
        break;
      case 1:
        indexOptionsSet = true;
        indexOptions = IndexOptions.NONE;
        break;
      case 2:
        indexOptionsSet = true;
        indexOptions = IndexOptions.DOCS;
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
  /** Key used to store the field types inside {@link IndexWriter#setCommitData}. */
  public static final String FIELD_TYPES_KEY = "FieldTypes";
  
  private Version loadFields(Map<String,String> commitUserData, boolean isNewIndex) throws IOException {
    // nocommit must deserialize current fields from commit data
    String currentFieldTypes = commitUserData.get(FIELD_TYPES_KEY);
    if (currentFieldTypes != null) {
      return readFromString(currentFieldTypes);
    } else if (isNewIndex == false) {
      // nocommit must handle back compat here
      // throw new CorruptIndexException("FieldTypes is missing from this index", "CommitUserData");
      enableExistsFilters = false;
      return Version.LATEST;
    } else {
      FieldType fieldType = new FieldType(FIELD_NAMES_FIELD);
      fields.put(FIELD_NAMES_FIELD, fieldType);
      fieldType.multiValued = Boolean.TRUE;
      fieldType.valueType = ValueType.ATOM;
      fieldType.sortable = Boolean.TRUE;
      fieldType.stored = Boolean.FALSE;
      setDefaults(fieldType);
      return Version.LATEST;
    }
  }

  private FieldType newFieldType(String fieldName) {
    if (fieldName.equals(FIELD_NAMES_FIELD)) {
      throw new IllegalArgumentException("field name \"" + fieldName + "\" is reserved");
    }

    return new FieldType(fieldName);
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
      current = newFieldType(fieldName);
      current.postingsFormat = postingsFormat;
      fields.put(fieldName, current);
      changed();
    } else {
      current.postingsFormat = postingsFormat;
      changed();
    }
  }

  public String getFieldTypeString(String fieldName) {
    return getFieldType(fieldName).toString();
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
      current = newFieldType(fieldName);
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
        } else if (field.blockTreeMinItemsInBlock != null || field.blockTreeMinItemsInAutoPrefix != null) {
          int minItemsInBlock, maxItemsInBlock;
          int minItemsInAutoPrefix, maxItemsInAutoPrefix;
          if (field.blockTreeMinItemsInBlock != null) {
            assert field.blockTreeMaxItemsInBlock != null;
            minItemsInBlock = field.blockTreeMinItemsInBlock.intValue();
            maxItemsInBlock = field.blockTreeMaxItemsInBlock.intValue();
          } else {
            minItemsInBlock = BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE;
            maxItemsInBlock = BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE;
          }
          if (field.blockTreeMinItemsInAutoPrefix != null) {
            assert field.blockTreeMaxItemsInAutoPrefix != null;
            minItemsInAutoPrefix = field.blockTreeMinItemsInAutoPrefix.intValue();
            maxItemsInAutoPrefix = field.blockTreeMaxItemsInAutoPrefix.intValue();
          } else {
            minItemsInAutoPrefix = BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE;
            maxItemsInAutoPrefix = BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE;
          }

          // nocommit do we now have cleaner API for this?  Ie "get me default PF, changing these settings"...
          return new Lucene50PostingsFormat(minItemsInBlock, maxItemsInBlock,
                                            minItemsInAutoPrefix, maxItemsInAutoPrefix);
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
        if (defaultIndexAnalyzer == null) {
          // nocommit sheisty
          return 0;
        } else {
          return defaultIndexAnalyzer.getPositionIncrementGap(fieldName);
        }
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
        if (defaultIndexAnalyzer == null) {
          // nocommit sheisty
          return 1;
        } else {
          return defaultIndexAnalyzer.getOffsetGap(fieldName);
        }
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
          // Must be lenient in case app is using low-schema API during indexing:
          return defaultIndexAnalyzer;
        }
        if (field.wrappedIndexAnalyzer != null) {
          return field.wrappedIndexAnalyzer;
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
          // Must be lenient in case app used low-schema API during indexing:
          return defaultQueryAnalyzer;
        }
        if (field.wrappedQueryAnalyzer != null) {
          return field.wrappedQueryAnalyzer;
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
      current = newFieldType(fieldName);
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
      current.reWrapAnalyzers(defaultIndexAnalyzer, defaultQueryAnalyzer);
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
      current = newFieldType(fieldName);
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
      current.reWrapAnalyzers(defaultIndexAnalyzer, defaultQueryAnalyzer);
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
      current = newFieldType(fieldName);
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
      current = newFieldType(fieldName);
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
  
  /** Require that all tokens indexed for this field fall between the min and max
   *  length, inclusive.  Any too-short or too-long tokens are silently discarded. */
  public synchronized void setMinMaxTokenLength(String fieldName, int minTokenLength, int maxTokenLength) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.minTokenLength = minTokenLength;
      current.maxTokenLength = maxTokenLength;
      fields.put(fieldName, current);
      current.reWrapAnalyzers(defaultIndexAnalyzer, defaultQueryAnalyzer);
      changed();
    } else if (current.minTokenLength == null ||
               current.minTokenLength.intValue() != minTokenLength ||
               current.maxTokenLength.intValue() != maxTokenLength) {
      Integer oldMin = current.minTokenLength;
      Integer oldMax = current.maxTokenLength;
      boolean success = false;
      try {
        current.minTokenLength = minTokenLength;
        current.maxTokenLength = maxTokenLength;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.minTokenLength = oldMin;
          current.maxTokenLength = oldMax;
        }
      }
      current.reWrapAnalyzers(defaultIndexAnalyzer, defaultQueryAnalyzer);
      changed();
    }
  }

  // nocommit clearMinMaxTokenLength

  public synchronized Integer getMinTokenLength(String fieldName) {
    return getFieldType(fieldName).minTokenLength;
  }

  public synchronized Integer getMaxTokenLength(String fieldName) {
    return getFieldType(fieldName).maxTokenLength;
  }

  public synchronized void setMaxTokenCount(String fieldName, int maxTokenCount) {
    setMaxTokenCount(fieldName, maxTokenCount, false);
  }

  // nocommit clearMaxTokenCount

  /** Only index up to maxTokenCount tokens for this field. */
  public synchronized void setMaxTokenCount(String fieldName, int maxTokenCount, boolean consumeAllTokens) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.maxTokenCount = maxTokenCount;
      current.consumeAllTokens = consumeAllTokens;
      fields.put(fieldName, current);
      current.reWrapAnalyzers(defaultIndexAnalyzer, defaultQueryAnalyzer);
      changed();
    } else if (current.maxTokenCount == null ||
               current.maxTokenCount.intValue() != maxTokenCount ||
               current.consumeAllTokens.booleanValue() != consumeAllTokens) {
      Integer oldMax = current.maxTokenCount;
      Boolean oldConsume = current.consumeAllTokens;
      boolean success = false;
      try {
        current.maxTokenCount = maxTokenCount;
        current.consumeAllTokens = consumeAllTokens;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.maxTokenCount = maxTokenCount;
          current.consumeAllTokens = consumeAllTokens;
        }
      }
      current.reWrapAnalyzers(defaultIndexAnalyzer, defaultQueryAnalyzer);
      changed();
    }
  }

  public synchronized Integer getMaxTokenCount(String fieldName) {
    return getFieldType(fieldName).maxTokenCount;
  }

  public synchronized Boolean getMaxTokenCountConsumeAllTokens(String fieldName) {
    return getFieldType(fieldName).consumeAllTokens;
  }

  /** The gap that should be added to token positions between each multi-valued field. */
  public synchronized void setAnalyzerPositionGap(String fieldName, int gap) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
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
      current = newFieldType(fieldName);
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
  public synchronized void setTermsDictBlockSize(String fieldName, int minItemsInBlock) {
    setTermsDictBlockSize(fieldName, minItemsInBlock, 2*(minItemsInBlock-1));
  }

  /** Sets the minimum and maximum number of terms in each term block in the terms dictionary.  These can be changed at any time, but changes only take
   *  effect for newly written (flushed or merged) segments.  The default is 25 and 48; higher values make fewer, larger blocks, which require less
   *  heap in the IndexReader but slows down term lookups. */
  public synchronized void setTermsDictBlockSize(String fieldName, int minItemsInBlock, int maxItemsInBlock) {
    ensureWritable();

    // nocommit must check that field is in fact using block tree?

    try {
      BlockTreeTermsWriter.validateSettings(minItemsInBlock, maxItemsInBlock);
    } catch (IllegalArgumentException iae) {
      illegalState(fieldName, iae.getMessage());
    }

    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.blockTreeMinItemsInBlock = minItemsInBlock;
      current.blockTreeMaxItemsInBlock = maxItemsInBlock;
      fields.put(fieldName, current);
      changed();
    } else if (current.blockTreeMinItemsInBlock == null) {
      boolean success = false;
      try {
        current.blockTreeMinItemsInBlock = minItemsInBlock;
        current.blockTreeMaxItemsInBlock = maxItemsInBlock;
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
      current.blockTreeMinItemsInBlock = minItemsInBlock;
      current.blockTreeMaxItemsInBlock = maxItemsInBlock;
      assert current.validate();
    }
  }
  /** Sets the minimum number of terms in each term block in the terms dictionary.  These can be changed at any time, but changes only take
   *  effect for newly written (flushed or merged) segments.  The default is 25; higher values make fewer, larger blocks, which require less
   *  heap in the IndexReader but slows down term lookups. */
  public synchronized void setTermsDictAutoPrefixSize(String fieldName, int minItemsInAutoPrefix) {
    setTermsDictAutoPrefixSize(fieldName, minItemsInAutoPrefix, 2*(minItemsInAutoPrefix-1));
  }

  /** Sets the minimum and maximum number of terms in each term block in the terms dictionary.  These can be changed at any time, but changes only take
   *  effect for newly written (flushed or merged) segments.  The default is 25 and 48; higher values make fewer, larger blocks, which require less
   *  heap in the IndexReader but slows down term lookups. */
  public synchronized void setTermsDictAutoPrefixSize(String fieldName, int minItemsInAutoPrefix, int maxItemsInAutoPrefix) {
    ensureWritable();

    // nocommit must check that field is in fact using block tree?

    try {
      BlockTreeTermsWriter.validateAutoPrefixSettings(minItemsInAutoPrefix, maxItemsInAutoPrefix);
    } catch (IllegalArgumentException iae) {
      illegalState(fieldName, iae.getMessage());
    }

    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.blockTreeMinItemsInAutoPrefix = minItemsInAutoPrefix;
      current.blockTreeMaxItemsInAutoPrefix = maxItemsInAutoPrefix;
      fields.put(fieldName, current);
      changed();
    } else if (current.blockTreeMinItemsInAutoPrefix == null) {
      boolean success = false;
      try {
        current.blockTreeMinItemsInAutoPrefix = minItemsInAutoPrefix;
        current.blockTreeMaxItemsInAutoPrefix = maxItemsInAutoPrefix;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.blockTreeMinItemsInAutoPrefix = null;
          current.blockTreeMaxItemsInAutoPrefix = null;
        }
      }
      changed();
    } else {
      current.blockTreeMinItemsInAutoPrefix = minItemsInAutoPrefix;
      current.blockTreeMaxItemsInAutoPrefix = maxItemsInAutoPrefix;
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
      current = newFieldType(fieldName);
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
    } else if (current.sortReversed == null || current.sortReversed.booleanValue() != reversed) {
      current.sortReversed = reversed;
      changed();
    }
  }

  /** Disables sorting for this field. */
  public synchronized void disableSorting(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
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

  public synchronized void setSortMissingFirst(String fieldName) {
    // Field must exist
    FieldType current = getFieldType(fieldName);

    if (current.sortable != Boolean.TRUE) {
      illegalState(fieldName, "cannot setSortMissingFirst: field is not enabled for sorting");
    }

    Boolean currentValue = current.sortMissingLast;
    if (currentValue != Boolean.FALSE) {
      current.sortMissingLast = Boolean.FALSE;
      boolean success = false;
      try {
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.sortMissingLast = currentValue;
        }
      }
      changed();
    }
  }

  public synchronized void setSortMissingLast(String fieldName) {
    // Field must exist
    FieldType current = getFieldType(fieldName);

    if (current.sortable != Boolean.TRUE) {
      illegalState(fieldName, "cannot setSortMissingLast: field is not enabled for sorting");
    }

    Boolean currentValue = current.sortMissingLast;
    if (currentValue != Boolean.TRUE) {
      current.sortMissingLast = Boolean.TRUE;
      boolean success = false;
      try {
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.sortMissingLast = currentValue;
        }
      }
      changed();
    }
  }

  /** Enables fast range filters for this field, using auto-prefix terms. */
  public synchronized void enableFastRanges(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
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

  /** Disables fast range filters for this field. */
  public synchronized void disableFastRanges(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.fastRanges = Boolean.FALSE;
      fields.put(fieldName, current);
      changed();
    } else if (current.fastRanges != Boolean.FALSE) {
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
      current = newFieldType(fieldName);
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
      current = newFieldType(fieldName);
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
      current = newFieldType(fieldName);
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
      current = newFieldType(fieldName);
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
  public synchronized void enableStored(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
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
      current = newFieldType(fieldName);
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

  // nocommit should we make a single method to enable the different combinations...?
  public synchronized void enableTermVectors(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
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
      current = newFieldType(fieldName);
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

  public synchronized void enableTermVectorOffsets(String fieldName) {
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

  public synchronized void disableTermVectorOffsets(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectorOffsets == Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorOffsets = Boolean.FALSE;
      changed();
    }
  }

  public synchronized boolean getTermVectorOffsets(String fieldName) {
    return getFieldType(fieldName).storeTermVectorOffsets == Boolean.TRUE;
  }

  public synchronized void enableTermVectorPositions(String fieldName) {
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

  public synchronized void disableTermVectorPositions(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectorPositions == Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorPositions = Boolean.FALSE;
      changed();
    }
  }

  public synchronized boolean getTermVectorPositions(String fieldName) {
    return getFieldType(fieldName).storeTermVectorPositions == Boolean.TRUE;
  }

  public synchronized void enableTermVectorPayloads(String fieldName) {
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

  public synchronized void disableTermVectorPayloads(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectorPayloads == Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorPayloads = Boolean.FALSE;
      changed();
    }
  }

  public synchronized boolean getTermVectorPayloads(String fieldName) {
    return getFieldType(fieldName).storeTermVectorPayloads == Boolean.TRUE;
  }

  /** Changes index options for this field.  This can be set to any
   *  value if it's not already set for the provided field; otherwise
   *  it can only be downgraded as low as DOCS but never unset
   *  entirely (once indexed, always indexed). */
  public synchronized void setIndexOptions(String fieldName, IndexOptions indexOptions) {
    ensureWritable();
    if (indexOptions == null) {
      throw new NullPointerException("IndexOptions must not be null (field: \"" + fieldName + "\")");
    }
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.indexOptions = indexOptions;
      current.indexOptionsSet = true;
      fields.put(fieldName, current);
      changed();
    } else if (current.indexOptionsSet == false) {
      assert current.indexOptions == IndexOptions.NONE;
      boolean success = false;
      try {
        current.indexOptions = indexOptions;
        current.indexOptionsSet = true;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.indexOptions = IndexOptions.NONE;
          current.indexOptionsSet = false;
        }
      }
      changed();
    } else if (current.indexOptions != IndexOptions.NONE && current.indexOptions != indexOptions) {
      assert current.indexOptionsSet;
      // Only allow downgrading IndexOptions:
      if (current.indexOptions.compareTo(indexOptions) < 0) {
        illegalState(fieldName, "cannot upgrade indexOptions from " + current.indexOptions + " to " + indexOptions);
      }
      current.indexOptions = indexOptions;
      changed();
    }
  }

  public synchronized IndexOptions getIndexOptions(String fieldName) {
    // nocommit throw exc if field doesn't exist?
    FieldType current = fields.get(fieldName);
    if (current == null) {
      // nocommit IO.NONE?
      return null;
    } else {
      return current.indexOptions;
    }
  }

  public synchronized void setDocValuesType(String fieldName, DocValuesType dvType) {
    ensureWritable();
    if (dvType == null) {
      throw new NullPointerException("docValueType cannot be null (field: \"" + fieldName + "\")");
    }
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.docValuesType = dvType;
      current.docValuesTypeSet = true;
      fields.put(fieldName, current);
      changed();
    } else if (current.docValuesTypeSet == false) {
      boolean success = false;
      assert current.docValuesType == DocValuesType.NONE;
      current.docValuesTypeSet = true;
      current.docValuesType = dvType;
      try {
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.docValuesType = DocValuesType.NONE;
          current.docValuesTypeSet = false;
        }
      }
      changed();
    } else if (current.docValuesType != dvType) {
      illegalState(fieldName, "cannot change docValuesType from " + current.docValuesType + " to " + dvType);
    }
  }

  public synchronized DocValuesType getDocValuesType(String fieldName, DocValuesType dvType) {
    // nocommit should we insist field exists?
    FieldType current = fields.get(fieldName);
    if (current == null) {
      // nocommit dvt.NONE?
      return null;
    } else {
      return current.docValuesType;
    }
  }

  synchronized void recordValueType(String fieldName, ValueType valueType) {
    ensureWritable();
    indexedDocs = true;
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.valueType = valueType;
      fields.put(fieldName, current);
      setDefaults(current);
      changed();
    } else if (current.valueType == ValueType.NONE) {
      // This can happen if e.g. the app first calls FieldTypes.setStored(...)
      boolean success = false;
      try {
        current.valueType = valueType;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.valueType = ValueType.NONE;
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
    indexedDocs = true;
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.valueType = ValueType.TEXT;
      fields.put(fieldName, current);
      setDefaults(current);
      if (allowStored == false) {
        current.stored = Boolean.FALSE;
      }
      if (indexed == false) {
        current.indexOptions = IndexOptions.NONE;
      }
      changed();
    } else if (current.valueType == ValueType.NONE) {
      // This can happen if e.g. the app first calls FieldTypes.setStored(...)
      Boolean oldStored = current.stored;
      boolean success = false;
      try {
        current.valueType = ValueType.TEXT;
        if (allowStored == false) {
          if (current.stored == Boolean.TRUE) {
            illegalState(fieldName, "can only store String large text fields");
          } else if (current.stored == null) {
            current.stored = Boolean.FALSE;
          }
        }
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.valueType = ValueType.NONE;
          current.stored = oldStored;
        }
      }
      setDefaults(current);
      changed();
    } else if (current.valueType != ValueType.TEXT) {
      illegalState(fieldName, "cannot change from value type " + current.valueType + " to " + ValueType.TEXT);
    }
  }

  // ncommit move this method inside FildType:
  private void setDefaults(FieldType field) {
    switch (field.valueType) {
    case NONE:
      // bug
      throw new AssertionError("valueType should not be NONE");
    case INT:
    case FLOAT:
    case LONG:
    case DOUBLE:
    case DATE:
      if (field.highlighted == null) {
        field.highlighted = Boolean.FALSE;
      }
      if (field.storeTermVectors == null) {
        field.storeTermVectors = Boolean.FALSE;
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
      if (field.indexOptionsSet == false) {
        field.indexOptions = IndexOptions.DOCS;
        field.indexOptionsSet = true;
      }
      if (field.docValuesTypeSet == false) {
        if (field.sortable == Boolean.TRUE) {
          if (field.multiValued == Boolean.TRUE) {
            field.docValuesType = DocValuesType.SORTED_NUMERIC;
          } else {
            field.docValuesType = DocValuesType.NUMERIC;
          }
        }
        field.docValuesTypeSet = true;
      }
      if (field.fastRanges == null) {
        if (field.indexOptions != IndexOptions.NONE) {
          field.fastRanges = Boolean.TRUE;
        } else {
          field.fastRanges = Boolean.FALSE;
        }
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      break;

    case SHORT_TEXT:
      if (field.highlighted == null) {
        field.highlighted = Boolean.TRUE;
      }
      if (field.storeTermVectors == null) {
        field.storeTermVectors = Boolean.FALSE;
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
      if (field.fastRanges == null) {
        field.fastRanges = Boolean.FALSE;
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      break;

    case ATOM:
    case UNIQUE_ATOM:
    case INET_ADDRESS:
      if (field.highlighted == null) {
        field.highlighted = Boolean.FALSE;
      }
      if (field.storeTermVectors == null) {
        field.storeTermVectors = Boolean.FALSE;
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
        field.indexOptions = IndexOptions.DOCS;
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
      if (field.fastRanges == null) {
        if (field.indexOptions != IndexOptions.NONE) {
          field.fastRanges = Boolean.TRUE;
        } else {
          field.fastRanges = Boolean.FALSE;
        }
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      break;

    case BINARY:
      if (field.highlighted == null) {
        field.highlighted = Boolean.FALSE;
      }
      if (field.storeTermVectors == null) {
        field.storeTermVectors = Boolean.FALSE;
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
        assert field.indexOptions == IndexOptions.NONE;
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
      if (field.fastRanges == null) {
        field.fastRanges = Boolean.FALSE;
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      break;

    case TEXT:
      if (field.highlighted == null) {
        field.highlighted = Boolean.TRUE;
      }
      if (field.storeTermVectors == null) {
        field.storeTermVectors = Boolean.FALSE;
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
      assert field.docValuesType == DocValuesType.NONE;
      field.docValuesTypeSet = true;

      if (field.fastRanges == null) {
        field.fastRanges = Boolean.FALSE;
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.TRUE;
      }
      break;
    
    case BOOLEAN:
      if (field.highlighted == null) {
        field.highlighted = Boolean.FALSE;
      }
      if (field.storeTermVectors == null) {
        field.storeTermVectors = Boolean.FALSE;
      }
      if (field.sortable == null) {
        field.sortable = Boolean.TRUE;
      }
      if (field.multiValued == null) {
        field.multiValued = Boolean.FALSE;
      }
      if (field.stored == null) {
        field.stored = Boolean.TRUE;
      }
      if (field.indexOptionsSet == false) {
        // validate enforces this:
        assert field.highlighted == false;
        field.indexOptions = IndexOptions.DOCS;
        field.indexOptionsSet = true;
      }
      if (field.docValuesTypeSet == false) {
        if (field.sortable == Boolean.TRUE) {
          if (field.multiValued == Boolean.TRUE) {
            field.docValuesType = DocValuesType.SORTED_NUMERIC;
          } else {
            field.docValuesType = DocValuesType.NUMERIC;
          }
        }
        field.docValuesTypeSet = true;
      }
      if (field.fastRanges == null) {
        field.fastRanges = Boolean.FALSE;
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      break;

    default:
      throw new AssertionError("missing value type in switch");
    }

    if (field.fastRanges == Boolean.TRUE) {
      if (field.blockTreeMinItemsInAutoPrefix == null) {
        field.blockTreeMinItemsInAutoPrefix = BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE;
        field.blockTreeMaxItemsInAutoPrefix = BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE;
      }
    }

    // nocommit assert all other settings are not null
    assert field.highlighted != null;
    assert field.storeTermVectors != null;
    assert field.fastRanges != null;
    assert field.sortable != null;
    assert field.multiValued != null;
    assert field.stored != null;
    assert field.indexOptionsSet;
    assert field.indexOptions != null;
    assert field.docValuesTypeSet;
    assert field.docValuesType != null;
    assert field.indexOptions == IndexOptions.NONE || field.indexNorms != null;

    // nocommit not an assert?  our setDefaults should never create an invalid setting!
    assert field.validate();
  } 

  /** Returns a query matching all documents that have this int term. */
  public Query newIntTermQuery(String fieldName, int token) {
    // nocommit should we take Number?

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    BytesRef bytes;

    switch (fieldType.valueType) {
    case INT:
      bytes = Document2.intToBytes(token);
      break;
    default:
      illegalState(fieldName, "cannot create int term query when valueType=" + fieldType.valueType);
      // Dead code but javac disagrees:
      bytes = null;
    }

    return new TermQuery(new Term(fieldName, bytes));
  }

  /** Returns a query matching all documents that have this long term. */
  public Query newLongTermQuery(String fieldName, long token) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    BytesRef bytes;

    switch (fieldType.valueType) {
    case LONG:
      bytes = Document2.longToBytes(token);
      break;
    default:
      illegalState(fieldName, "cannot create long term query when valueType=" + fieldType.valueType);
      // Dead code but javac disagrees:
      bytes = null;
    }

    return new TermQuery(new Term(fieldName, bytes));
  }

  /** Returns a query matching all documents that have this binary token. */
  public Query newBinaryTermQuery(String fieldName, byte[] token) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    // Field must be binary:
    if (fieldType.valueType != ValueType.BINARY && fieldType.valueType != ValueType.ATOM) {
      illegalState(fieldName, "binary term query must have valueType BINARY or ATOM; got " + fieldType.valueType);
    }

    return new TermQuery(new Term(fieldName, new BytesRef(token)));
  }

  public Query newStringTermQuery(String fieldName, String token) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    // Field must be text:
    if (fieldType.valueType != ValueType.TEXT && fieldType.valueType != ValueType.SHORT_TEXT && fieldType.valueType != ValueType.ATOM) {
      illegalState(fieldName, "string term query must have valueType TEXT, SHORT_TEXT or ATOM; got " + fieldType.valueType);
    }

    return new TermQuery(new Term(fieldName, token));
  }

  public Query newBooleanTermQuery(String fieldName, boolean token) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    // Field must be boolean:
    if (fieldType.valueType != ValueType.BOOLEAN) {
      illegalState(fieldName, "boolean term query must have valueType BOOLEAN; got " + fieldType.valueType);
    }

    byte[] value = new byte[1];
    if (token) {
      value[0] = 1;
    }

    return new TermQuery(new Term(fieldName, new BytesRef(value)));
  }

  public Query newInetAddressTermQuery(String fieldName, InetAddress token) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    // Field must be InetAddress:
    if (fieldType.valueType != ValueType.INET_ADDRESS) {
      illegalState(fieldName, "inet address term query must have valueType INET_ADDRESS; got " + fieldType.valueType);
    }

    return new TermQuery(new Term(fieldName, new BytesRef(token.getAddress())));
  }

  public Filter newRangeFilter(String fieldName, Number min, boolean minInclusive, Number max, boolean maxInclusive) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range query: this field was not indexed");
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "this field was not indexed for fast ranges");
    }

    // nocommit should we really take Number here?  it's too weakly typed?  you could ask for float range on an int field?  should we
    // instead make separate methods for each atomic type?  or should we "type check" the incoming Number?  taking Number is more
    // conventient for query parsers...?

    BytesRef minTerm;
    BytesRef maxTerm;
    
    switch (fieldType.valueType) {
    case INT:
      minTerm = min == null ? null : Document2.intToBytes(min.intValue());
      maxTerm = max == null ? null : Document2.intToBytes(max.intValue());
      break;

    case FLOAT:
      minTerm = min == null ? null : Document2.intToBytes(Float.floatToIntBits(min.floatValue()));
      maxTerm = max == null ? null : Document2.intToBytes(Float.floatToIntBits(max.floatValue()));
      break;

    case LONG:
      minTerm = min == null ? null : Document2.longToBytes(min.longValue());
      maxTerm = max == null ? null : Document2.longToBytes(max.longValue());
      break;

    case DOUBLE:
      minTerm = min == null ? null : Document2.longToBytes(Double.doubleToLongBits(min.doubleValue()));
      maxTerm = max == null ? null : Document2.longToBytes(Double.doubleToLongBits(max.doubleValue()));
      break;

    default:
      illegalState(fieldName, "cannot create numeric range query on non-numeric field; got valueType=" + fieldType.valueType);

      // Dead code but javac disagrees:
      return null;
    }

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive);
  }

  public Filter newRangeFilter(String fieldName, byte[] minTerm, boolean minInclusive, byte[] maxTerm, boolean maxInclusive) {
    return newRangeFilter(fieldName, new BytesRef(minTerm), minInclusive, new BytesRef(maxTerm), maxInclusive);
  }

  public Filter newRangeFilter(String fieldName, BytesRef minTerm, boolean minInclusive, BytesRef maxTerm, boolean maxInclusive) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range query: this field was not indexed");
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "this field was not indexed for fast ranges");
    }

    // nocommit verify type is BINARY or ATOM?

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive);
  }

  // nocommit Date sugar for a range query matching a specific hour/day/month/year/etc.?  need locale/timezone... should we use DateTools?

  public Filter newRangeFilter(String fieldName, Date min, boolean minInclusive, Date max, boolean maxInclusive) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range query: this field was not indexed");
    }

    if (fieldType.valueType != ValueType.DATE) {
      illegalState(fieldName, "cannot create range query: expected valueType=DATE but got: " + fieldType.valueType);
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "this field was not indexed for fast ranges");
    }

    BytesRef minTerm = min == null ? null : Document2.longToBytes(min.getTime());
    BytesRef maxTerm = max == null ? null : Document2.longToBytes(max.getTime());

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive);
  }

  // nocommit also add "range filter using net mask" sugar version
  public Filter newRangeFilter(String fieldName, InetAddress min, boolean minInclusive, InetAddress max, boolean maxInclusive) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range query: this field was not indexed");
    }

    if (fieldType.valueType != ValueType.INET_ADDRESS) {
      illegalState(fieldName, "cannot create range query: expected valueType=INET_ADDRESS but got: " + fieldType.valueType);
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "this field was not indexed for fast ranges");
    }

    BytesRef minTerm = min == null ? null : new BytesRef(min.getAddress());
    BytesRef maxTerm = max == null ? null : new BytesRef(max.getAddress());

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive);
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
        reversed = (Boolean) fields[upto++];
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
      {
        SortField sortField;
        if (fieldType.multiValued == Boolean.TRUE) {
          // nocommit need to be able to set selector...
          sortField = new SortedNumericSortField(fieldName, SortField.Type.INT, reverse);
        } else {
          sortField = new SortField(fieldName, SortField.Type.INT, reverse);
        }
        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Integer.MIN_VALUE);
          } else {
            sortField.setMissingValue(Integer.MAX_VALUE);
          }
        } else if (fieldType.sortMissingLast == Boolean.FALSE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Integer.MAX_VALUE);
          } else {
            sortField.setMissingValue(Integer.MIN_VALUE);
          }
        }
        return sortField;
      }

    case FLOAT:
      {
        SortField sortField;
        if (fieldType.multiValued == Boolean.TRUE) {
          // nocommit need to be able to set selector...
          sortField = new SortedNumericSortField(fieldName, SortField.Type.FLOAT, reverse);
        } else {
          sortField = new SortField(fieldName, SortField.Type.FLOAT, reverse);
        }
        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Float.NEGATIVE_INFINITY);
          } else {
            sortField.setMissingValue(Float.POSITIVE_INFINITY);
          }
        } else if (fieldType.sortMissingLast == Boolean.FALSE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Float.POSITIVE_INFINITY);
          } else {
            sortField.setMissingValue(Float.NEGATIVE_INFINITY);
          }
        }
        return sortField;
      }

    case LONG:
    case DATE:
      {
        SortField sortField;
        if (fieldType.multiValued == Boolean.TRUE) {
          // nocommit need to be able to set selector...
          sortField = new SortedNumericSortField(fieldName, SortField.Type.LONG, reverse);
        } else {
          sortField = new SortField(fieldName, SortField.Type.LONG, reverse);
        }
        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Long.MIN_VALUE);
          } else {
            sortField.setMissingValue(Long.MAX_VALUE);
          }
        } else if (fieldType.sortMissingLast == Boolean.FALSE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Long.MAX_VALUE);
          } else {
            sortField.setMissingValue(Long.MIN_VALUE);
          }
        }
        return sortField;
      }

    case DOUBLE:
      {
        SortField sortField;
        if (fieldType.multiValued == Boolean.TRUE) {
          // nocommit need to be able to set selector...
          sortField = new SortedNumericSortField(fieldName, SortField.Type.DOUBLE, reverse);
        } else {
          sortField = new SortField(fieldName, SortField.Type.DOUBLE, reverse);
        }
        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Double.NEGATIVE_INFINITY);
          } else {
            sortField.setMissingValue(Double.POSITIVE_INFINITY);
          }
        } else if (fieldType.sortMissingLast == Boolean.FALSE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Double.POSITIVE_INFINITY);
          } else {
            sortField.setMissingValue(Double.NEGATIVE_INFINITY);
          }
        }
        return sortField;
      }

    case SHORT_TEXT:
    case ATOM:
    case UNIQUE_ATOM:
    case BINARY:
    case BOOLEAN:
    case INET_ADDRESS:
      SortField sortField;
      {
        if (fieldType.multiValued == Boolean.TRUE) {
          // nocommit need to be able to set selector...
          sortField = new SortedSetSortField(fieldName, reverse);
        } else {
          sortField = new SortField(fieldName, SortField.Type.STRING, reverse);
        }

        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(SortField.STRING_FIRST);
          } else {
            sortField.setMissingValue(SortField.STRING_LAST);
          }
        } else if (fieldType.sortMissingLast == Boolean.FALSE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(SortField.STRING_LAST);
          } else {
            sortField.setMissingValue(SortField.STRING_FIRST);
          }
        }
        return sortField;
      }

    default:
      // BUG
      illegalState(fieldName, "unhandled sort case, valueType=" + fieldType.valueType);

      // Dead code but javac disagrees:
      return null;
    }
  }

  /** Returns a {@link Filter} accepting documents that have this field. */
  public Filter newFieldExistsFilter(String fieldName) {
    if (enableExistsFilters == false) {
      throw new IllegalStateException("field exists filter was disabled");
    }

    // nocommit TermFilter?
    // nocommit optimize this filter to MatchAllDocs when Terms.getDocCount() == maxDoc
    return new QueryWrapperFilter(new TermQuery(new Term(FIELD_NAMES_FIELD, fieldName)));
  }

  private synchronized void changed() {
    ensureWritable();
    changeCount++;
  }

  public synchronized long getChangeCount() {
    return changeCount;
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
    
    out.writeVInt(indexCreatedVersion.major);
    out.writeVInt(indexCreatedVersion.minor);
    out.writeVInt(indexCreatedVersion.bugfix);

    writeBoolean(out, enableExistsFilters);
    writeBoolean(out, indexedDocs);

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

    Version indexCreatedVersion = Version.fromBits(in.readVInt(), in.readVInt(), in.readVInt());

    enableExistsFilters = readBoolean(in);
    indexedDocs = readBoolean(in);

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

  public boolean isUniqueAtom(String fieldName) {
    return getFieldType(fieldName).valueType == ValueType.UNIQUE_ATOM;
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

  /** Returns true if values in this field must be unique across all documents in the index. */
  public synchronized boolean isUnique(String fieldName) {   
    FieldType current = fields.get(fieldName);
    return current != null && current.valueType == ValueType.UNIQUE_ATOM;
  }

  /** Defines a dynamic field, computed by a Javascript expression referring
   *  to other field values, to be used for sorting. */
  public void addIntExpressionField(String fieldName, String expression) {
    // nocommit how to do this?  must we make a FieldTypes subclass in expressions module = pita?
  }

  // nocommit also long, float, double

  public synchronized void enableExistsFilters() {
    if (enableExistsFilters == false && indexedDocs) {
      throw new IllegalStateException("cannot enable exists filters after documents were already indexed");
    }
    enableExistsFilters = true;
  }

  public synchronized void disableExistsFilters() {
    if (enableExistsFilters && indexedDocs) {
      throw new IllegalStateException("cannot disable exists filters after documents were already indexed");
    }
    enableExistsFilters = false;
  }

  private static void writeBoolean(DataOutput out, boolean value) throws IOException {
    if (value) {
      out.writeByte((byte) 1);
    } else {
      out.writeByte((byte) 0);
    }
  }

  private static boolean readBoolean(DataInput in) throws IOException {
    byte b = in.readByte();
    if (b == 1) {
      return true;
    } else if (b == 0) {
      return false;
    } else {
      throw new CorruptIndexException("invalid byte for boolean: " + b, in);
    }
  }
}
