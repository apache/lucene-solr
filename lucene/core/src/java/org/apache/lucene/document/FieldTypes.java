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
import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesRangeFilter;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.HalfFloatComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
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
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;

// TODO
//   - nocommit: back compat to old numeric fields
//   - write/read of field types should be up to codec?
//   - how should Codec integrate
//   - analyzers are not serializable
//   - what about sparse fields... anything for us to do...
//   - payloads just stay write once in their own way?
//   - CheckIndex could optionally do more validation
//     - or that terms in an ipv6 field always have 16 bytes
//   - add query time integration
//     - query parsers
//       - exc if invalid field name asked for
//       - numeric range queries "just work"
//       - default search field

// docsWithField and fieldExistsFilter are redundant if field is dv'd and indexed


// TODO: how to allow extending this?
//   - geo
//   - expressions
//   - index sorting
//   - suggesters
//   - icu
//   - facets (FacetsConfig, hierarchical fields)
//   - doc blocks (nested) / joins

// nocommit how will future back-compat work?  segment must store field types as of when it was written?

// nocommit run all monster tests

// nocommit move to oal.index?

// NO
//   - filter caching?  parent blockdocs filter?
//   - required
//   - not null
//   - "all" field
//   - "copy" field
//   - dynamic fields
//   - can we somehow always store a "source"?  can be handled above
//   - default value (value used if the field is null/missing): this seems silly wasteful, and layer above can handle it
//   - sort proxy field ("when I sort by X you should actually sort by Y"): can be handled above
//   - can/should we validate field names here?

// LATER
//   - time intervals
//   - fold in compressing stored fields format params...how
//   - add array of atom values?  fieldnamesfield would use it?
//   - sugar for more/less ranges (where one end is null)
//   - separate analyzer for phrase queries in suggesters
//   - more sugar for common analyzers, e.g. accent folding (to ascii), case folding, ngrams for partial matching
//   - can we somehow detect at search time if the field types you are using doesn't match the searcher you are now searching against?
//   - add "partial match" option, that turns on ngrams?
//   - can we move multi-field-ness out of IW?  so IW only gets a single instance of each field
//   - use better pf when field is unique
//   - nested/parent/child docs?
//   - highlight proxy field (LUCENE-6061)
//   - sugar API to retrieve values from DVs or stored fields or whatever?
//   - we could track here which fields are actually searched/filtered on ... and e.g. make use of this during warmers ...
//   - BigDecimal?
//   - index-time sorting should be here too
//   - make LiveFieldValues easier to use?
//   - can we have test infra that randomly reopens writer?
//   - newTermFilter?
//   - PH should take this and validate highlighting was enabled?  (it already checks for OFFSETS in postings)

// nocommit -- can we sort on a field that ran through analysis (and made a single token), e.g. case / accent folding

// nocommit sort order, sort options (e.g. for collation)
//   case sensitive, punc sensitive, accent sensitive

// nocommit accent removal and lowercasing for wildcards should just work

// nocommit fix all change methods to call validate / rollback

// nocommit controlling compression of stored fields, norms

/** Records how each field is indexed, stored, etc.
 *
 * @lucene.experimental */

public class FieldTypes {

  public static final int DEFAULT_POSITION_GAP = 0;

  public static final int DEFAULT_OFFSET_GAP = 1;

  /** Key used to store the field types inside {@link IndexWriter#setCommitData}. */
  public static final String FIELD_TYPES_KEY = "FieldTypes";
  
  public static final String FIELD_NAMES_FIELD = "$fieldnames";

  public static final int VERSION_START = 0;

  public static final int VERSION_CURRENT = VERSION_START;

  public static final String CODEC_NAME = "FieldTypes";

  public enum ValueType {
    NONE,
    TEXT,
    SHORT_TEXT,
    ATOM,
    INT,
    HALF_FLOAT,
    FLOAT,
    LONG,
    DOUBLE,
    BIG_INT,
    BINARY,
    BOOLEAN,
    DATE,
    INET_ADDRESS,
  }

  // nocommit should we have a "resolution" for Date field?

  private final boolean readOnly;

  /** So exists filters are fast */
  boolean enableExistsFilters = true;
  private boolean indexedDocs;

  private final Version indexCreatedVersion;

  final Map<String,FieldType> fields = new HashMap<>();

  // Null when we are readOnly:
  private final Analyzer defaultIndexAnalyzer;

  // Null when we are not readOnly:
  private Analyzer defaultQueryAnalyzer;

  private Similarity defaultSimilarity;

  /** Used only in memory to record when something changed. */
  private long changeCount;

  private volatile boolean closed;

  /** Just like current oal.document.FieldType, except for each setting it can also record "not-yet-set". */
  class FieldType implements IndexableFieldType, Cloneable {
    private final String name;

    // Lucene version when we were created:
    private final Version createdVersion;

    volatile ValueType valueType = ValueType.NONE;
    volatile DocValuesType docValuesType = DocValuesType.NONE;
    private volatile boolean docValuesTypeSet;

    // True if the term is unique across all documents (e.g. a primary key field):
    volatile Boolean isUnique;

    // True when Document.addStoredXXX was used:
    volatile Boolean storedOnly;

    // Only used for ATOM:
    volatile Boolean isBinary;

    // Expert: settings we pass to BlockTree to control how many terms are allowed in each block and auto-prefix term
    volatile Integer blockTreeMinItemsInBlock;
    volatile Integer blockTreeMaxItemsInBlock;
    volatile Integer blockTreeMinItemsInAutoPrefix;
    volatile Integer blockTreeMaxItemsInAutoPrefix;

    // Gaps to add between multiple values of the same field; if these are not set, we fallback to the Analyzer for that field.
    volatile Integer analyzerPositionGap;
    volatile Integer analyzerOffsetGap;

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
    private volatile SortedNumericSelector.Type numericSelector = SortedNumericSelector.Type.MIN;
    private volatile SortedSetSelector.Type sortedSetSelector = SortedSetSelector.Type.MIN;

    // Whether this field's values should be indexed for fast ranges (using numeric field for now):
    private volatile Boolean fastRanges;

    // Whether this field may appear more than once per document:
    volatile Boolean multiValued;

    // Whether this field's norms are indexed:
    private volatile Boolean indexNorms;

    // Bit width for a big int field:
    volatile Integer bigIntByteWidth;

    private volatile Boolean storeTermVectors;
    private volatile Boolean storeTermVectorPositions;
    private volatile Boolean storeTermVectorOffsets;
    private volatile Boolean storeTermVectorPayloads;

    // Field is indexed if this != null:
    private volatile IndexOptions indexOptions = IndexOptions.NONE;
    private volatile boolean indexOptionsSet;

    // TODO: not great that we can't also set other formats:
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

    volatile Boolean reversedTerms;

    Locale sortLocale;
    Collator sortCollator;
    SortKey sortKey;

    public FieldType(String name) {
      this(name, Version.LATEST);
    }

    public FieldType(String name, Version version) {
      this.name = name;
      this.createdVersion = version;
    }

    /** Copy constructor. */
    FieldType(FieldType other) {
      this.name = other.name;
      this.createdVersion = other.createdVersion;
      this.valueType = other.valueType;
      this.docValuesType = other.docValuesType;
      this.docValuesTypeSet = other.docValuesTypeSet;
      this.isUnique = other.isUnique;
      this.storedOnly = other.storedOnly;
      this.isBinary = other.isBinary;
      this.blockTreeMinItemsInBlock = other.blockTreeMinItemsInBlock;
      this.blockTreeMaxItemsInBlock = other.blockTreeMaxItemsInBlock;
      this.blockTreeMinItemsInAutoPrefix = other.blockTreeMinItemsInAutoPrefix;
      this.blockTreeMaxItemsInAutoPrefix = other.blockTreeMaxItemsInAutoPrefix;
      this.analyzerPositionGap = other.analyzerPositionGap;
      this.analyzerOffsetGap = other.analyzerOffsetGap;
      this.minTokenLength = other.minTokenLength;
      this.maxTokenLength = other.maxTokenLength;
      this.maxTokenCount = other.maxTokenCount;
      this.consumeAllTokens = other.consumeAllTokens;
      this.stored = other.stored;
      this.sortable = other.sortable;
      this.sortReversed = other.sortReversed;
      this.sortMissingLast = other.sortMissingLast;
      this.numericSelector = other.numericSelector;
      this.sortedSetSelector = other.sortedSetSelector;
      this.fastRanges = other.fastRanges;
      this.multiValued = other.multiValued;
      this.indexNorms = other.indexNorms;
      this.bigIntByteWidth = other.bigIntByteWidth;
      this.storeTermVectors = other.storeTermVectors;
      this.storeTermVectorPositions = other.storeTermVectorPositions;
      this.storeTermVectorOffsets = other.storeTermVectorOffsets;
      this.storeTermVectorPayloads = other.storeTermVectorPayloads;
      this.indexOptions = other.indexOptions;
      this.indexOptionsSet = other.indexOptionsSet;
      this.postingsFormat = other.postingsFormat;
      this.docValuesFormat = other.docValuesFormat;
      this.highlighted = other.highlighted;
      this.queryAnalyzer = other.queryAnalyzer;
      this.indexAnalyzer = other.indexAnalyzer;
      this.similarity = other.similarity;
      this.wrappedIndexAnalyzer = other.wrappedIndexAnalyzer;
      this.wrappedQueryAnalyzer = other.wrappedQueryAnalyzer;
      this.reversedTerms = other.reversedTerms;
      this.sortLocale = other.sortLocale;
      this.sortCollator = other.sortCollator;
      this.sortKey = other.sortKey;
    }

    public synchronized void merge(FieldType other) {
      assert name.equals(other.name);

      // nocommit more need test coverage here
      if (other.valueType != ValueType.NONE) {
        if (valueType == ValueType.NONE) {
          valueType = other.valueType;
        } else if (other.valueType != valueType) {
          illegalState(name, "cannot change value type from " + valueType + " to " + other.valueType);
        }
      }
      if (other.docValuesTypeSet) {
        if (docValuesTypeSet == false) {
          docValuesType = other.docValuesType;  
          docValuesTypeSet = true;
        } else if (other.docValuesType != docValuesType) {
          illegalState(name, "cannot change docValuesType from " + docValuesType + " to " + other.docValuesType);
        }
      }
      if (other.isUnique != null) {
        if (isUnique == null) {
          isUnique = other.isUnique;
        } else if (other.isUnique != isUnique) {
          illegalState(name, "cannot change isUnique from " + isUnique + " to " + other.isUnique);
        }
      }
      if (other.storedOnly != null) {
        if (storedOnly == null) {
          storedOnly = other.storedOnly;
        } else if (other.storedOnly != storedOnly) {
          illegalState(name, "cannot change storedOnly from " + storedOnly + " to " + other.storedOnly);
        }
      }
      if (other.isBinary != null) {
        if (isBinary == null) {
          isBinary = other.isBinary;
        } else if (other.isBinary != isBinary) {
          illegalState(name, "cannot change isBinary from " + isBinary + " to " + other.isBinary);
        }
      }
      if (other.blockTreeMinItemsInBlock != null) {
        blockTreeMinItemsInBlock = other.blockTreeMinItemsInBlock;
      }
      if (other.blockTreeMaxItemsInBlock != null) {
        blockTreeMaxItemsInBlock = other.blockTreeMaxItemsInBlock;
      }
      if (other.blockTreeMinItemsInAutoPrefix != null) {
        blockTreeMinItemsInAutoPrefix = other.blockTreeMinItemsInAutoPrefix;
      }
      if (other.blockTreeMaxItemsInAutoPrefix != null) {
        blockTreeMaxItemsInAutoPrefix = other.blockTreeMaxItemsInAutoPrefix;
      }

      if (other.analyzerPositionGap != null) {
        if (analyzerPositionGap == null) {
          analyzerPositionGap = other.analyzerPositionGap;
        } else if (other.analyzerPositionGap.equals(analyzerPositionGap) == false) {
          illegalState(name, "cannot change analyzerPositionGap from " + analyzerPositionGap + " to " + other.analyzerPositionGap);
        }
      }
      if (other.analyzerOffsetGap != null) {
        if (analyzerOffsetGap == null) {
          analyzerOffsetGap = other.analyzerOffsetGap;
        } else if (other.analyzerOffsetGap.equals(analyzerOffsetGap) == false) {
          illegalState(name, "cannot change analyzerOffsetGap from " + analyzerOffsetGap + " to " + other.analyzerOffsetGap);
        }
      }
      if (other.minTokenLength != null) {
        minTokenLength = other.minTokenLength;
      }
      if (other.maxTokenLength != null) {
        maxTokenLength = other.maxTokenLength;
      }
      if (other.maxTokenCount != null) {
        maxTokenCount = other.maxTokenCount;
      }
      if (other.consumeAllTokens != null) {
        consumeAllTokens = other.consumeAllTokens;
      }
      if (other.stored != null) {
        stored = other.stored;
      }
      if (other.sortable != null) {
        if (sortable == null) {
          sortable = other.sortable;
        } else if (other.sortable == Boolean.FALSE) {
          sortable = other.sortable;
        } else if (sortable == Boolean.FALSE) {
          illegalState(name, "sorting was already disabled");
        }
        if (other.sortReversed != null) {
          sortReversed = other.sortReversed;
        }
        if (other.sortMissingLast != null) {
          sortMissingLast = other.sortMissingLast;
        }
      }

      if (other.numericSelector != null) {
        numericSelector = other.numericSelector;
      }
      if (other.sortedSetSelector != null) {
        sortedSetSelector = other.sortedSetSelector;
      }
      if (other.fastRanges != null) {
        if (fastRanges == null) {
          fastRanges = other.fastRanges;
        } else if (other.fastRanges == Boolean.FALSE) {
          fastRanges = Boolean.FALSE;
        } else if (fastRanges == Boolean.FALSE) {
          illegalState(name, "fastRanges was already disabled");
        }
      }
      if (other.multiValued != null) {
        if (multiValued == null) {
          multiValued = other.multiValued;
        } else if (other.multiValued != multiValued) {
          illegalState(name, "cannot change multiValued from " + multiValued + " to " + other.multiValued);
        }
      }

      if (other.indexNorms != null) {
        if (indexNorms == null) {
          indexNorms = other.indexNorms;
        } else if (other.indexNorms == Boolean.FALSE) {
          indexNorms = Boolean.FALSE;
        } else if (indexNorms == Boolean.FALSE) {
          illegalState(name, "norms were already disabled");
        }
      }

      if (other.bigIntByteWidth != null) {
        if (bigIntByteWidth == null) {
          bigIntByteWidth = other.bigIntByteWidth;
        } else if (bigIntByteWidth.equals(other.bigIntByteWidth) == false) {
          illegalState(name, "cannot change bigIntByteWidth from " + bigIntByteWidth + " to " + other.bigIntByteWidth);
        }
      }

      if (other.storeTermVectors != null) {
        storeTermVectors = other.storeTermVectors;

        if (other.storeTermVectorPositions != null) {
          storeTermVectorPositions = other.storeTermVectorPositions;
        }
        if (other.storeTermVectorOffsets != null) {
          storeTermVectorOffsets = other.storeTermVectorOffsets;
        }
        if (other.storeTermVectorPayloads != null) {
          storeTermVectorPayloads = other.storeTermVectorPayloads;
        }
      }

      if (other.indexOptionsSet) {
        if (indexOptionsSet == false) {
          indexOptions = other.indexOptions;
          indexOptionsSet = true;
        } else if (indexOptions.compareTo(other.indexOptions) >= 0) {
          indexOptions = other.indexOptions;
        } else {
          illegalState(name, "cannot upgrade indexOptions from " + indexOptions + " to " + other.indexOptions);
        }
      }

      if (other.postingsFormat != null) {
        postingsFormat = other.postingsFormat;
      }

      if (other.docValuesFormat != null) {
        docValuesFormat = other.docValuesFormat;
      }

      if (other.highlighted != null) {
        if (highlighted == null) {
          highlighted = other.highlighted;
        } else if (other.highlighted == Boolean.FALSE) {
          highlighted = Boolean.FALSE;
        } else if (highlighted == Boolean.FALSE) {
          illegalState(name, "highlighting was already disabled");
        }
      }

      if (other.queryAnalyzer != null) {
        if (queryAnalyzer == null) {
          queryAnalyzer = other.queryAnalyzer;
        } else if (queryAnalyzer != other.queryAnalyzer) {
          illegalState(name, "queryAnalyzer was already set");
        }
      }

      if (other.indexAnalyzer != null) {
        if (indexAnalyzer == null) {
          indexAnalyzer = other.indexAnalyzer;
        } else if (indexAnalyzer != other.indexAnalyzer) {
          illegalState(name, "indexAnalyzer was already set");
        }
      }

      if (other.similarity != null) {
        similarity = other.similarity;
      }

      if (other.wrappedIndexAnalyzer != null) {
        wrappedIndexAnalyzer = other.wrappedIndexAnalyzer;
      }
      if (other.wrappedQueryAnalyzer != null) {
        wrappedQueryAnalyzer = other.wrappedQueryAnalyzer;
      }
      if (other.reversedTerms != null) {
        if (reversedTerms == null) {
          reversedTerms = other.reversedTerms;
        } else if (other.reversedTerms != reversedTerms) {
          illegalState(name, "can only setReversedTerms before the field is indexed");
        }
      }
      if (other.sortLocale != null) {
        if (sortLocale == null) {
          sortLocale = other.sortLocale;
          sortCollator = other.sortCollator;
        } else if (sortLocale.equals(other.sortLocale) == false) {
          if (valueType == null) {
            sortLocale = other.sortLocale;
            sortCollator = other.sortCollator;
          } else {
            illegalState(name, "sortLocale can only be set before indexing");
          }
        }
      }

      if (other.sortKey != null) {
        sortKey = other.sortKey;
      }

      changed(false);
    }

    boolean validate() {
      switch (valueType) {
      case NONE:
        break;
      case INT:
      case HALF_FLOAT:
      case FLOAT:
      case LONG:
      case DOUBLE:
      case BIG_INT:
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
        if (valueType == ValueType.BIG_INT) {
          if (docValuesType != DocValuesType.NONE && (docValuesType != DocValuesType.SORTED && docValuesType != DocValuesType.SORTED_SET)) {
            illegalState(name, "type " + valueType + " must use SORTED or SORTED_SET docValuesType; got: " + docValuesType);
          }
        } else {
          if (docValuesType != DocValuesType.NONE && (docValuesType != DocValuesType.NUMERIC && docValuesType != DocValuesType.SORTED_NUMERIC)) {
            illegalState(name, "type " + valueType + " must use NUMERIC or SORTED_NUMERIC docValuesType; got: " + docValuesType);
          }
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
          illegalState(name, "type " + valueType + " must use BINARY, SORTED or SORTED_SET docValuesType; got: " + docValuesType);
        }
        if (indexOptions != IndexOptions.NONE && indexOptions.compareTo(IndexOptions.DOCS) > 0) {
          illegalState(name, "type " + valueType + " can only be indexed as DOCS; got " + indexOptions);
        }
        if (minTokenLength != null) {
          illegalState(name, "type " + valueType + " cannot set min/max token length");
        }
        if (maxTokenCount != null) {
          illegalState(name, "type " + valueType + " cannot set max token count");
        }
        break;
      case ATOM:
        if (indexAnalyzer != null) {
          illegalState(name, "type " + valueType + " cannot have an indexAnalyzer");
        }
        if (queryAnalyzer != null) {
          illegalState(name, "type " + valueType + " cannot have a queryAnalyzer");
        }
        if (indexNorms == Boolean.TRUE) {
          illegalState(name, "type " + valueType + " cannot index norms");
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
          illegalState(name, "type " + valueType + " must use NUMERIC or SORTED_NUMERIC docValuesType; got: " + docValuesType);
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

      if (sortKey != null && valueType != ValueType.ATOM) {
        illegalState(name, "sortKey can only be set for ATOM fields; got value type=" + valueType);
      }

      if (multiValued == Boolean.TRUE &&
          (docValuesType == DocValuesType.NUMERIC ||
           docValuesType == DocValuesType.SORTED ||
           docValuesType == DocValuesType.BINARY)) {
        illegalState(name, "DocValuesType=" + docValuesType + " cannot be multi-valued");
      }

      if (storeTermVectors == Boolean.TRUE) {
        if (indexOptionsSet && indexOptions == IndexOptions.NONE) {
          illegalState(name, "cannot enable term vectors when indexOptions is NONE");
        }
      } else {
        if (storeTermVectorOffsets == Boolean.TRUE) {
          illegalState(name, "cannot enable term vector offsets when term vectors are not enabled");
        }
        if (storeTermVectorPositions == Boolean.TRUE) {
          illegalState(name, "cannot enable term vector positions when term vectors are not enabled");
        }
      }

      if (sortable == Boolean.TRUE && (docValuesTypeSet && docValuesType == DocValuesType.NONE)) {
        illegalState(name, "cannot sort when DocValuesType=" + docValuesType);
      }

      if (sortable == Boolean.FALSE && sortLocale != null) {
        illegalState(name, "cannot set sortLocale when field is not enabled for sorting");
      }

      if (indexOptionsSet) {
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
          if (isUnique == Boolean.TRUE) {
            illegalState(name, "can only setIsUnique if the field is indexed");
          }
          if (storeTermVectors == Boolean.TRUE) {
            illegalState(name, "can only store term vectors if the field is indexed");
          }
        } else {
          if (valueType != ValueType.TEXT && valueType != ValueType.SHORT_TEXT && indexAnalyzer != null) {
            illegalState(name, "can only setIndexAnalyzer for short text and large text fields; got value type=" + valueType);
          }
          if (valueType != ValueType.TEXT && valueType != ValueType.SHORT_TEXT && queryAnalyzer != null) {
            illegalState(name, "can only setQueryAnalyzer for short text and large text fields; got value type=" + valueType);
          }
          if (isUnique == Boolean.TRUE && indexOptions != IndexOptions.DOCS) {
            illegalState(name, "unique fields should be indexed with IndexOptions.DOCS; got indexOptions=" + indexOptions);
          }
        }
      }

      if (reversedTerms == Boolean.TRUE) {
        if (indexOptions == IndexOptions.NONE) {
          illegalState(name, "can only reverse terms if the field is indexed");
        }
        if (valueType != ValueType.SHORT_TEXT && valueType != ValueType.TEXT && valueType != ValueType.ATOM) {
          illegalState(name, "can only reverse terms for text and short_text value type; got value type=" + valueType);
        }
      }

      // nocommit must check that if fastRanges is on, you have a PF that supports it
      
      if (fastRanges == Boolean.TRUE && indexOptions != IndexOptions.DOCS) {
        illegalState(name, "fastRanges is only possible when indexOptions=DOCS; got: " + indexOptions);
      }

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
        if (valueType != ValueType.TEXT && valueType != ValueType.SHORT_TEXT && valueType != ValueType.ATOM) {
          illegalState(name, "can only enable highlighting for TEXT or SHORT_TEXT fields; got value type=" + valueType);
        }
        if (indexOptions != IndexOptions.NONE && indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
          illegalState(name, "must index with IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS when highlighting is enabled");
        }
      }

      return true;
    }

    private boolean needsWrapping() {
      return minTokenLength != null || maxTokenCount != null || reversedTerms == Boolean.TRUE;
    }

    void reWrapAnalyzers(Analyzer defaultIndexAnalyzer, Analyzer defaultQueryAnalyzer) {
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
        wrappedIndexAnalyzer = indexAnalyzer;
        wrappedQueryAnalyzer = queryAnalyzer;
      }
    }

    void setDefaults() {
      switch (valueType) {
      case NONE:
        // bug
        throw new AssertionError("valueType should not be NONE");
      case INT:
      case HALF_FLOAT:
      case FLOAT:
      case LONG:
      case DOUBLE:
      case BIG_INT:
      case DATE:
        if (highlighted == null) {
          highlighted = Boolean.FALSE;
        }
        if (storeTermVectors == null) {
          storeTermVectors = Boolean.FALSE;
        }
        if (sortable == null) {
          if (valueType == ValueType.BIG_INT) {
            if (docValuesTypeSet == false || docValuesType == DocValuesType.SORTED || docValuesType == DocValuesType.SORTED_SET) {
              sortable = Boolean.TRUE;
            } else {
              sortable = Boolean.FALSE;
            }
          } else {
            if (docValuesTypeSet == false || docValuesType == DocValuesType.NUMERIC || docValuesType == DocValuesType.SORTED_NUMERIC) {
              sortable = Boolean.TRUE;
            } else {
              sortable = Boolean.FALSE;
            }
          }
        }
        if (multiValued == null) {
          multiValued = Boolean.FALSE;
        }
        if (stored == null) {
          stored = Boolean.TRUE;
        }
        if (indexOptionsSet == false) {
          indexOptions = IndexOptions.DOCS;
          indexOptionsSet = true;
        }
        if (docValuesTypeSet == false) {
          if (sortable == Boolean.TRUE) {
            if (valueType == ValueType.BIG_INT) {
              if (multiValued == Boolean.TRUE) {
                docValuesType = DocValuesType.SORTED_SET;
              } else {
                docValuesType = DocValuesType.SORTED;
              }
            } else {
              if (multiValued == Boolean.TRUE) {
                docValuesType = DocValuesType.SORTED_NUMERIC;
              } else {
                docValuesType = DocValuesType.NUMERIC;
              }
            }
          }
          docValuesTypeSet = true;
        }
        if (fastRanges == null) {
          if (indexOptions != IndexOptions.NONE) {
            fastRanges = Boolean.TRUE;
          } else {
            fastRanges = Boolean.FALSE;
          }
        }
        if (indexNorms == null) {
          indexNorms = Boolean.FALSE;
        }
        if (isUnique == null) {
          isUnique = Boolean.FALSE;
        }
        break;

      case SHORT_TEXT:
        if (highlighted == null) {
          highlighted = Boolean.TRUE;
        }
        if (storeTermVectors == null) {
          storeTermVectors = Boolean.FALSE;
        }
        if (sortable == null) {
          if (docValuesTypeSet == false || docValuesType == DocValuesType.SORTED || docValuesType == DocValuesType.SORTED_SET) {
            sortable = Boolean.TRUE;
          } else {
            sortable = Boolean.FALSE;
          }
        }
        if (multiValued == null) {
          multiValued = Boolean.FALSE;
        }
        if (stored == null) {
          stored = Boolean.TRUE;
        }
        if (docValuesTypeSet == false) {
          if (sortable == Boolean.TRUE) {
            if (multiValued == Boolean.TRUE) {
              docValuesType = DocValuesType.SORTED_SET;
            } else {
              docValuesType = DocValuesType.SORTED;
            }
          }
          docValuesTypeSet = true;
        }
        if (indexOptionsSet == false) {
          if (highlighted) {
            indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
          } else {
            indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
          }
          indexOptionsSet = true;
        }
        if (fastRanges == null) {
          fastRanges = Boolean.FALSE;
        }
        if (indexNorms == null) {
          indexNorms = Boolean.FALSE;
        }
        if (isUnique == null) {
          isUnique = Boolean.FALSE;
        }
        break;

      case ATOM:
      case INET_ADDRESS:
        if (highlighted == null) {
          highlighted = Boolean.FALSE;
        }
        if (storeTermVectors == null) {
          storeTermVectors = Boolean.FALSE;
        }
        if (sortable == null) {
          if (docValuesTypeSet == false || docValuesType == DocValuesType.SORTED || docValuesType == DocValuesType.SORTED_SET) {
            sortable = Boolean.TRUE;
          } else {
            sortable = Boolean.FALSE;
          }
        }
        if (multiValued == null) {
          multiValued = Boolean.FALSE;
        }
        if (stored == null) {
          stored = Boolean.TRUE;
        }
        if (indexOptionsSet == false) { 
          if (highlighted) {
            indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
          } else {
            indexOptions = IndexOptions.DOCS;
          }
          indexOptionsSet = true;
        }
        if (docValuesTypeSet == false) {
          if (sortable == Boolean.TRUE) {
            if (multiValued == Boolean.TRUE) {
              docValuesType = DocValuesType.SORTED_SET;
            } else {
              docValuesType = DocValuesType.SORTED;
            }
          }
          docValuesTypeSet = true;
        }
        if (fastRanges == null) {
          if (indexOptions != IndexOptions.NONE) {
            fastRanges = Boolean.TRUE;
          } else {
            fastRanges = Boolean.FALSE;
          }
        }
        if (indexNorms == null) {
          indexNorms = Boolean.FALSE;
        }
        if (isUnique == null) {
          isUnique = Boolean.FALSE;
        }
        break;

      case BINARY:
        if (highlighted == null) {
          highlighted = Boolean.FALSE;
        }
        if (storeTermVectors == null) {
          storeTermVectors = Boolean.FALSE;
        }
        if (sortable == null) {
          if (docValuesTypeSet == false || docValuesType == DocValuesType.SORTED || docValuesType == DocValuesType.SORTED_SET) {
            sortable = Boolean.TRUE;
          } else {
            sortable = Boolean.FALSE;
          }
        }
        if (multiValued == null) {
          multiValued = Boolean.FALSE;
        }
        if (stored == null) {
          stored = Boolean.TRUE;
        }
        if (indexOptionsSet == false) {
          assert indexOptions == IndexOptions.NONE;
          indexOptionsSet = true;
        }
        if (docValuesTypeSet == false) {
          if (sortable == Boolean.TRUE) {
            if (multiValued == Boolean.TRUE) {
              docValuesType = DocValuesType.SORTED_SET;
            } else {
              docValuesType = DocValuesType.SORTED;
            }
          } else {
            docValuesType = DocValuesType.BINARY;
          }
          docValuesTypeSet = true;
        }
        if (fastRanges == null) {
          fastRanges = Boolean.FALSE;
        }
        if (indexNorms == null) {
          indexNorms = Boolean.FALSE;
        }
        if (isUnique == null) {
          isUnique = Boolean.FALSE;
        }
        break;

      case TEXT:
        if (highlighted == null) {
          highlighted = Boolean.TRUE;
        }
        if (storeTermVectors == null) {
          storeTermVectors = Boolean.FALSE;
        }
        if (sortable == null) {
          sortable = Boolean.FALSE;
        }
        if (multiValued == null) {
          multiValued = Boolean.FALSE;
        }
        if (stored == null) {
          stored = Boolean.TRUE;
        }
        if (indexOptionsSet == false) {
          if (highlighted) {
            indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
          } else {
            indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
          }
          indexOptionsSet = true;
        }

        assert docValuesType == DocValuesType.NONE;
        docValuesTypeSet = true;

        if (fastRanges == null) {
          fastRanges = Boolean.FALSE;
        }
        if (indexNorms == null) {
          indexNorms = Boolean.TRUE;
        }
        if (isUnique == null) {
          isUnique = Boolean.FALSE;
        }
        break;
    
      case BOOLEAN:
        if (highlighted == null) {
          highlighted = Boolean.FALSE;
        }
        if (storeTermVectors == null) {
          storeTermVectors = Boolean.FALSE;
        }
        if (sortable == null) {
          sortable = Boolean.TRUE;
        }
        if (multiValued == null) {
          multiValued = Boolean.FALSE;
        }
        if (stored == null) {
          stored = Boolean.TRUE;
        }
        if (indexOptionsSet == false) {
          // validate enforces this:
          assert highlighted == false;
          indexOptions = IndexOptions.DOCS;
          indexOptionsSet = true;
        }
        if (docValuesTypeSet == false) {
          if (sortable == Boolean.TRUE) {
            if (multiValued == Boolean.TRUE) {
              docValuesType = DocValuesType.SORTED_NUMERIC;
            } else {
              docValuesType = DocValuesType.NUMERIC;
            }
          }
          docValuesTypeSet = true;
        }
        if (fastRanges == null) {
          fastRanges = Boolean.FALSE;
        }
        if (indexNorms == null) {
          indexNorms = Boolean.FALSE;
        }
        if (isUnique == null) {
          isUnique = Boolean.FALSE;
        }
        break;

      default:
        throw new AssertionError("missing value type in switch");
      }

      if (fastRanges == Boolean.TRUE) {
        if (blockTreeMinItemsInAutoPrefix == null) {
          blockTreeMinItemsInAutoPrefix = BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE;
          blockTreeMaxItemsInAutoPrefix = BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE;
        }
      }

      assert name != null;
      assert createdVersion != null;
      assert valueType != null;
      assert docValuesTypeSet;
      assert docValuesType != null;
      assert isUnique != null;
      assert storedOnly != null;
      assert valueType != ValueType.ATOM || isBinary != null;
      assert indexOptionsSet;
      assert indexOptions != null;
      assert stored != null;
      assert sortable != null;
      assert fastRanges != null;
      assert multiValued != null;
      assert indexOptions == IndexOptions.NONE || indexNorms != null;
      assert highlighted != null;
      assert storeTermVectors != null;

      // setDefaults() should never create an invalid state:
      validate();
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

          if (reversedTerms == Boolean.TRUE) {
            try {
              Class c = Class.forName("org.apache.lucene.analysis.reverse.ReverseStringFilter");
              Constructor init = c.getConstructor(new Class[] {TokenStream.class});
              end = (TokenStream) init.newInstance(end);
            } catch (ReflectiveOperationException roe) {
              throw new IllegalStateException("could not locate ReverseStringFilter; ensure Lucene's analysis module is on your CLASSPATH", roe);
            }
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
      b.append("  value type: ");
      b.append(valueType);
      if (valueType == ValueType.ATOM && isBinary == Boolean.TRUE) {
        b.append(" (binary)");
      }
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
        b.append('\n');
      }

      if (analyzerOffsetGap != null) {
        b.append("  multi-valued offset gap: ");
        b.append(analyzerOffsetGap);
        b.append('\n');
      }

      if (multiValued == Boolean.TRUE) {
        b.append("  multiValued: true");
        b.append('\n');
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
        if (sortable == Boolean.TRUE) {
          if (sortReversed != null) {
            b.append(" reversed=");
            b.append(sortReversed);
          }
          if (sortMissingLast == Boolean.TRUE) {
            b.append(" (missing: last)");
          } else if (sortMissingLast == Boolean.FALSE) {
            b.append(" (missing: first)");
          }
        }

        if (multiValued == Boolean.TRUE) {
          if (isNumericType(valueType)) {
            if (numericSelector != null) {
              b.append(" (numericSelector: " + numericSelector + ")");
            }
          } else if (sortedSetSelector != null) {
            b.append(" (sortedSetSelector: " + sortedSetSelector + ")");
          }
        }
      } else {
        b.append("unset");
      }
      b.append('\n');

      b.append("  fastRanges: ");
      if (fastRanges != null) {
        b.append(fastRanges);
        if (fastRanges == Boolean.TRUE) {
          b.append(" (auto-prefix blocks: ");
          if (blockTreeMinItemsInAutoPrefix != null) {
            b.append(blockTreeMinItemsInAutoPrefix);
            b.append(" - ");
            b.append(blockTreeMaxItemsInAutoPrefix);
          } else {
            b.append(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE);
            b.append(" - ");
            b.append(BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
          }
          b.append(")");
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
        if (isUnique != null) {
          b.append("\n  unique: " + isUnique);
        }
        if (storeTermVectors == Boolean.TRUE) {
          b.append("\n  termVectors: yes");
          if (storeTermVectorPositions == Boolean.TRUE) {
            b.append(" positions");
            if (storeTermVectorPayloads == Boolean.TRUE) {
              b.append(" payloads");
            }
          }
          if (storeTermVectorOffsets == Boolean.TRUE) {
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
    public DocValuesType docValuesType() {
      return docValuesType;
    }

    void write(DataOutput out) throws IOException {
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
      case HALF_FLOAT:
        out.writeByte((byte) 5);
        break;
      case FLOAT:
        out.writeByte((byte) 6);
        break;
      case LONG:
        out.writeByte((byte) 7);
        break;
      case DOUBLE:
        out.writeByte((byte) 8);
        break;
      case BIG_INT:
        out.writeByte((byte) 9);
        break;
      case BINARY:
        out.writeByte((byte) 10);
        break;
      case BOOLEAN:
        out.writeByte((byte) 11);
        break;
      case DATE:
        out.writeByte((byte) 12);
        break;
      case INET_ADDRESS:
        out.writeByte((byte) 13);
        break;
      default:
        throw new AssertionError("missing value type in switch");
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
      out.writeString(numericSelector.toString());
      out.writeString(sortedSetSelector.toString());
      writeNullableBoolean(out, multiValued);
      writeNullableBoolean(out, indexNorms);
      writeNullableBoolean(out, reversedTerms);
      writeNullableInteger(out, bigIntByteWidth);
      writeNullableBoolean(out, fastRanges);
      writeNullableBoolean(out, storeTermVectors);
      writeNullableBoolean(out, storeTermVectorPositions);
      writeNullableBoolean(out, storeTermVectorOffsets);
      writeNullableBoolean(out, storeTermVectorPayloads);
      writeNullableBoolean(out, isUnique);
      writeNullableBoolean(out, storedOnly);
      writeNullableBoolean(out, isBinary);

      if (sortLocale != null) {
        out.writeByte((byte) 1);
        // nocommit this is not sufficient right?  need to use the builder?
        writeNullableString(out, sortLocale.getLanguage());
        writeNullableString(out, sortLocale.getCountry());
        writeNullableString(out, sortLocale.getVariant());
      } else {
        out.writeByte((byte) 0);
      }

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

    public FieldType(DataInput in) throws IOException {
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
        valueType = ValueType.HALF_FLOAT;
        break;
      case 6:
        valueType = ValueType.FLOAT;
        break;
      case 7:
        valueType = ValueType.LONG;
        break;
      case 8:
        valueType = ValueType.DOUBLE;
        break;
      case 9:
        valueType = ValueType.BIG_INT;
        break;
      case 10:
        valueType = ValueType.BINARY;
        break;
      case 11:
        valueType = ValueType.BOOLEAN;
        break;
      case 12:
        valueType = ValueType.DATE;
        break;
      case 13:
        valueType = ValueType.INET_ADDRESS;
        break;
      default:
        throw new CorruptIndexException("invalid byte for value type: " + b, in);
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
      numericSelector = SortedNumericSelector.Type.valueOf(in.readString());
      sortedSetSelector = SortedSetSelector.Type.valueOf(in.readString());
      multiValued = readNullableBoolean(in);
      indexNorms = readNullableBoolean(in);
      reversedTerms = readNullableBoolean(in);
      bigIntByteWidth = readNullableInteger(in);
      fastRanges = readNullableBoolean(in);
      storeTermVectors = readNullableBoolean(in);
      storeTermVectorPositions = readNullableBoolean(in);
      storeTermVectorOffsets = readNullableBoolean(in);
      storeTermVectorPayloads = readNullableBoolean(in);
      isUnique = readNullableBoolean(in);
      storedOnly = readNullableBoolean(in);
      isBinary = readNullableBoolean(in);
      b = in.readByte();
      if (b == 1) {
        String language = readNullableString(in);
        String country = readNullableString(in);
        String variant = readNullableString(in);
        // nocommit this is not sufficient right?  need to use the builder?
        sortLocale = new Locale(language, country, variant);
        sortCollator = Collator.getInstance(sortLocale);
      } else if (b != 0) {
        throw new CorruptIndexException("invalid byte for sortLocale: " + b, in);        
      }
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

    @Override
    public int getPositionGap() {
      if (analyzerPositionGap != null) {
        return analyzerPositionGap;
      } else if (indexAnalyzer != null) {
        return indexAnalyzer.getPositionIncrementGap(name);
      } else if (defaultIndexAnalyzer != null) {
        return defaultIndexAnalyzer.getPositionIncrementGap(name);
      } else {
        return DEFAULT_POSITION_GAP;
      }
    }

    @Override
    public int getOffsetGap() {
      if (analyzerOffsetGap != null) {
        return analyzerOffsetGap;
      } else if (indexAnalyzer != null) {
        return indexAnalyzer.getOffsetGap(name);
      } else if (defaultIndexAnalyzer != null) {
        return defaultIndexAnalyzer.getOffsetGap(name);
      } else {
        return DEFAULT_OFFSET_GAP;
      }
    }
  }

  /** Only invoked by IndexWriter directly.
   *
   * @lucene.internal */
  // nocommit lock this down so only IW can create?
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

  public FieldTypes(FieldTypes other) {
    readOnly = true;
    this.defaultIndexAnalyzer = null;
    if (other != null) {
      indexCreatedVersion = other.indexCreatedVersion;
      this.defaultQueryAnalyzer = other.defaultQueryAnalyzer;
      this.defaultSimilarity = other.defaultSimilarity;
      addAll(other);
    } else {
      indexCreatedVersion = Version.LATEST;
      this.defaultQueryAnalyzer = null;
      this.defaultSimilarity = null;
    }
  }

  public FieldTypes(FieldTypes other, Iterable<String> fieldsToKeep) {
    readOnly = true;
    this.defaultIndexAnalyzer = null;
    if (other != null) {
      this.defaultQueryAnalyzer = other.defaultQueryAnalyzer;
      this.defaultSimilarity = other.defaultSimilarity;
      indexCreatedVersion = other.indexCreatedVersion;
      for(String field : fieldsToKeep) {
        FieldType fieldType = other.fields.get(field);
        if (fieldType != null) {
          fields.put(field, new FieldType(fieldType));
        }
      }
      FieldType fieldType = other.fields.get(FIELD_NAMES_FIELD);
      if (fieldType != null) {
        fields.put(FIELD_NAMES_FIELD, new FieldType(fieldType));
      }
      
    } else {
      addFieldNamesField();
      this.defaultQueryAnalyzer = null;
      this.defaultSimilarity = null;
      indexCreatedVersion = Version.LATEST;
    }
  }

  private synchronized Version loadFields(Map<String,String> commitUserData, boolean isNewIndex) throws IOException {
    String currentFieldTypes = commitUserData.get(FIELD_TYPES_KEY);
    if (currentFieldTypes != null) {
      return readFromString(currentFieldTypes);
    } else if (isNewIndex == false) {
      // Index already exists, but no FieldTypes
      // nocommit must handle back compat here
      // throw new CorruptIndexException("FieldTypes is missing from this index", "CommitUserData");
      enableExistsFilters = false;
      return Version.LATEST;
    } else {
      addFieldNamesField();
      return Version.LATEST;
    }
  }

  private void addFieldNamesField() {
    assert fields.containsKey(FIELD_NAMES_FIELD) == false;

    FieldType fieldType = new FieldType(FIELD_NAMES_FIELD);
    fields.put(FIELD_NAMES_FIELD, fieldType);
    fieldType.valueType = ValueType.ATOM;
    fieldType.multiValued = Boolean.TRUE;
    fieldType.sortable = Boolean.FALSE;
    fieldType.stored = Boolean.FALSE;
    fieldType.storedOnly = Boolean.FALSE;
    fieldType.fastRanges = Boolean.FALSE;
    fieldType.isBinary = Boolean.FALSE;
    fieldType.setDefaults();
  }

  private synchronized FieldType newFieldType(String fieldName) {
    if (fieldName.equals(FIELD_NAMES_FIELD)) {
      throw new IllegalArgumentException("field name \"" + fieldName + "\" is reserved");
    }

    return new FieldType(fieldName);
  }

  /** Decodes String previously created by bytesToString. */
  private static byte[] stringToBytes(String s) {
    byte[] bytesIn = s.getBytes(StandardCharsets.UTF_8);
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

  // nocommit: let codec control this format?  should we write directly into segments_N?  or into a new separately gen'd file?

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

    return new String(bytesOut, StandardCharsets.UTF_8);
  }

  public synchronized void setPostingsFormat(String fieldName, String postingsFormat) {
    // nocommit can we prevent this, if our codec isn't used by IW?
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

  public IndexableFieldType getIndexableFieldType(String fieldName) {
    return getFieldType(fieldName);
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
    // nocommit can we prevent this, if our codec isn't used by IW?
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

  private Similarity similarity = new PerFieldSimilarityWrapper() {
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
      // TODO: too bad we can't just set every format here?  what if we fix this schema to record which format per field, and then
      // remove PerFieldXXXFormat...?
      @Override
      public PostingsFormat getPostingsFormatForField(String fieldName) {
        FieldType field = fields.get(fieldName);
        if (field == null) {
          return super.getPostingsFormatForField(fieldName);
        }

        if (field.postingsFormat != null) {
          // Field has a custom PF:
          return PostingsFormat.forName(field.postingsFormat);
        } else if (field.blockTreeMinItemsInBlock != null || field.fastRanges == Boolean.TRUE) {
          // Field has the default PF, but we customize BlockTree params:
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
          } else if (field.fastRanges == Boolean.TRUE) {
            minItemsInAutoPrefix = BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE;
            maxItemsInAutoPrefix = BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE;
          } else {
            minItemsInAutoPrefix = 0;
            maxItemsInAutoPrefix = 0;
          }

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

  private static final Analyzer SINGLE_TOKEN_ANALYZER = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(final String fieldName) {
        return new TokenStreamComponents(new SingleTokenTokenizer());
      }
    };

  private abstract class FieldTypeAnalyzer extends DelegatingAnalyzerWrapper {
    public FieldTypeAnalyzer() {
      super(Analyzer.PER_FIELD_REUSE_STRATEGY);
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getOffsetGap(String fieldName) {
      throw new UnsupportedOperationException();
    }

    // nocommit: what about wrapReader?
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
          return SINGLE_TOKEN_ANALYZER;
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

  // TODO: we could note that the field had a specific analyzer set, and then throw exc if it didn't get set again after load?

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
      current.reWrapAnalyzers(defaultIndexAnalyzer, defaultQueryAnalyzer);
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
      illegalState(fieldName, "indexAnalyzer was already set");
    }
  }

  public synchronized void setDefaultSimilarity(Similarity sim) {
    this.defaultSimilarity = sim;
  }

  public synchronized Similarity getDefaultSimilarity() {
    return defaultSimilarity;
  }

  public synchronized void setDefaultQueryAnalyzer(Analyzer a) {
    this.defaultQueryAnalyzer = a;
  }

  public synchronized Analyzer getDefaultQueryAnalyzer() {
    return defaultQueryAnalyzer;
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
      illegalState(fieldName, "queryAnalyzer was already set");
    }
  }

  public synchronized Analyzer getQueryAnalyzer(String fieldName) {
    return getFieldType(fieldName).queryAnalyzer;
  }

  /** NOTE: similarity does not persist, so each time you create {@code FieldTypes} from
   *  {@linkIndexWriter} or {@link IndexReader} you must set all per-field similarities again.  This can be changed at any time. */
  public synchronized void setSimilarity(String fieldName, Similarity similarity) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.similarity = similarity;
      fields.put(fieldName, current);
      changed();
    } else {
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
      FieldType sav = new FieldType(current);
      boolean success = false;
      try {
        current.minTokenLength = minTokenLength;
        current.maxTokenLength = maxTokenLength;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          fields.put(fieldName, current);
        }
      }
      current.reWrapAnalyzers(defaultIndexAnalyzer, defaultQueryAnalyzer);
      changed();
    }
  }

  public synchronized Integer getMinTokenLength(String fieldName) {
    return getFieldType(fieldName).minTokenLength;
  }

  public synchronized Integer getMaxTokenLength(String fieldName) {
    return getFieldType(fieldName).maxTokenLength;
  }

  public synchronized void setMaxTokenCount(String fieldName, int maxTokenCount) {
    setMaxTokenCount(fieldName, maxTokenCount, false);
  }

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
    // nocommit can we prevent this, if our codec isn't used by IW?
    setTermsDictBlockSize(fieldName, minItemsInBlock, 2*(minItemsInBlock-1));
  }

  /** Sets the minimum and maximum number of terms in each term block in the terms dictionary.  These can be changed at any time, but changes only take
   *  effect for newly written (flushed or merged) segments.  The default is 25 and 48; higher values make fewer, larger blocks, which require less
   *  heap in the IndexReader but slows down term lookups. */
  public synchronized void setTermsDictBlockSize(String fieldName, int minItemsInBlock, int maxItemsInBlock) {
    // nocommit can we prevent this, if our codec isn't used by IW?
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
      changed();
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
      changed();
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
      current.sortable = Boolean.FALSE;
      current.sortReversed = null;
      changed();
    }
  }

  public synchronized boolean getSorted(String fieldName) {
    return getFieldType(fieldName).sortable == Boolean.TRUE;
  }

  private boolean isNumericType(ValueType type) {
    return type == ValueType.INT ||
      type == ValueType.HALF_FLOAT ||
      type == ValueType.FLOAT ||
      type == ValueType.LONG ||
      type == ValueType.DOUBLE ||
      type == ValueType.DATE;
  }

  /** For multi-valued numeric fields, sets which value should be selected for sorting.  This can be changed at any time. */
  public synchronized void setMultiValuedNumericSortSelector(String fieldName, SortedNumericSelector.Type selector) {
    // field must exist
    FieldType current = getFieldType(fieldName);
    if (current.multiValued != Boolean.TRUE) {
      illegalState(fieldName, "this field is not multi-valued");
    }
    if (isNumericType(current.valueType) == false) {
      illegalState(fieldName, "value type must be INT, HALF_FLOAT, FLOAT, LONG, DOUBLE or DATE; got value type=" + current.valueType);
    }
    if (current.sortable != Boolean.TRUE) {
      illegalState(fieldName, "field is not enabled for sorting");
    }
    if (current.numericSelector != selector) {
      current.numericSelector = selector;
      changed(false);
    }
  }

  public synchronized SortedNumericSelector.Type getMultiValuedNumericSortSelector(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.multiValued != Boolean.TRUE) {
      illegalState(fieldName, "this field is not multi-valued");
    }
    if (isNumericType(current.valueType) == false) {
      illegalState(fieldName, "value type must be INT, HALF_FLOAT, FLOAT, LONG, DOUBLE or DATE; got value type=" + current.valueType);
    }
    if (current.sortable != Boolean.TRUE) {
      illegalState(fieldName, "field is not enabled for sorting");
    }
    return current.numericSelector;
  }

  /** For multi-valued binary fields, sets which value should be selected for sorting.  This can be changed at any time. */
  public synchronized void setMultiValuedStringSortSelector(String fieldName, SortedSetSelector.Type selector) {
    // field must exist
    FieldType current = getFieldType(fieldName);
    if (current.multiValued != Boolean.TRUE) {
      illegalState(fieldName, "this field is not multi-valued");
    }
    if (current.valueType != ValueType.BIG_INT &&
        current.valueType != ValueType.ATOM) {
      illegalState(fieldName, "value type must be BIG_INT or ATOM; got value type=" + current.valueType);
    }
    if (current.sortable != Boolean.TRUE) {
      illegalState(fieldName, "field is not enabled for sorting");
    }
    if (current.sortedSetSelector != selector) {
      current.sortedSetSelector = selector;
      changed(false);
    }
  }

  public synchronized SortedSetSelector.Type getMultiValuedStringSortSelector(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.multiValued != Boolean.TRUE) {
      illegalState(fieldName, "this field is not multi-valued");
    }
    if (current.valueType != ValueType.BIG_INT &&
        current.valueType != ValueType.ATOM) {
      illegalState(fieldName, "value type must be BIG_INT or ATOM; got value type=" + current.valueType);
    }
    if (current.sortable != Boolean.TRUE) {
      illegalState(fieldName, "field is not enabled for sorting");
    }
    return current.sortedSetSelector;
  }

  public synchronized void setSortMissingFirst(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.sortMissingLast = Boolean.FALSE;
      fields.put(fieldName, current);
      changed();
    } else {
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
        changed(false);
      }
    }
  }

  public synchronized void setSortMissingLast(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.sortMissingLast = Boolean.TRUE;
      fields.put(fieldName, current);
      changed();
    } else {
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
        changed(false);
      }
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

  /** Disables fast range filters for this field.  You can do this at any time, but once it's disabled you cannot re-enable it. */
  public synchronized void disableFastRanges(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.fastRanges = Boolean.FALSE;
      fields.put(fieldName, current);
      changed();
    } else if (current.fastRanges != Boolean.FALSE) {
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
      illegalState(fieldName, "highlighting was already disabled");
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
    } else if (current.highlighted != Boolean.TRUE) {
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
      illegalState(fieldName, "norms were already disable");
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

  /** Sets the maximum precision for this big int field. */
  public void setBigIntByteWidth(String fieldName, int bytes) {
    if (bytes <= 0) {
      illegalState(fieldName, "bytes must be > 0; got: " + bytes);
    }

    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.storedOnly = Boolean.FALSE;
      current.valueType = ValueType.BIG_INT;
      current.bigIntByteWidth = bytes;
      fields.put(fieldName, current);
      current.setDefaults();
      changed();
    } else if (current.valueType == ValueType.NONE) {
      current.storedOnly = Boolean.FALSE;
      current.valueType = ValueType.BIG_INT;
      current.bigIntByteWidth = bytes;
      current.setDefaults();
      changed();
    } else if (current.valueType != ValueType.BIG_INT) {
      illegalState(fieldName, "can only setBigIntByteWidth on BIG_INT fields; got value type=" + current.valueType);
    } else if (current.bigIntByteWidth == null) {
      current.bigIntByteWidth = bytes;
      changed();
    } else if (current.bigIntByteWidth.intValue() != bytes) {
      illegalState(fieldName, "cannot change bigIntByteWidth from " + current.bigIntByteWidth + " to " + bytes);
    }
  }

  public int getBigIntByteWidth(String fieldName) {
    // field must exist
    FieldType current = getFieldType(fieldName);
    if (current.valueType != ValueType.BIG_INT) {
      illegalState(fieldName, "field is not BIG_INT; got value type=" + current.valueType);
    }
    return current.bigIntByteWidth;
  }

  /** All indexed terms are reversed before indexing, using {@code ReverseStringFilter}.  This requires that Lucene's analysis module is on
   *  the classpath. */
  public void setReversedTerms(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.reversedTerms = true;
      fields.put(fieldName, current);
      current.reWrapAnalyzers(defaultIndexAnalyzer, defaultQueryAnalyzer);
      changed();
    } else if (current.reversedTerms == null) {
      current.reversedTerms = true;
      boolean success = false;
      try {
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.reversedTerms = null;
        }
      }
      current.reWrapAnalyzers(defaultIndexAnalyzer, defaultQueryAnalyzer);
      changed();
    } else if (current.reversedTerms != Boolean.TRUE) {
      illegalState(fieldName, "can only setReversedTerms before the field is indexed");
    }
  }

  public Boolean getReversedTerms(String fieldName) {
    // field must exist
    FieldType current = getFieldType(fieldName);
    return current.reversedTerms; 
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
      current.stored = Boolean.FALSE;
      changed();
    }
  }

  /** Whether this field's value is stored. */
  public synchronized boolean getStored(String fieldName) {
    return getFieldType(fieldName).stored == Boolean.TRUE;
  }

  /** Enable term vectors for this field.  This can be changed at any time. */
  public synchronized void enableTermVectors(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.storeTermVectors = Boolean.TRUE;
      fields.put(fieldName, current);
      changed();
    } else if (current.storeTermVectors != Boolean.TRUE) {
      current.storeTermVectors = Boolean.TRUE;
      changed();
    }
  }

  /** Disable term vectors for this field.  This can be changed at any time. */
  public synchronized void disableTermVectors(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.storeTermVectors = Boolean.FALSE;
      fields.put(fieldName, current);
      changed();
    } else if (current.storeTermVectors != Boolean.FALSE) {
      current.storeTermVectors = Boolean.FALSE;
      changed();
    }
  }

  public synchronized boolean getTermVectors(String fieldName) {
    return getFieldType(fieldName).storeTermVectors == Boolean.TRUE;
  }

  /** Enable term vector offsets for this field.  This can be changed at any time. */
  public synchronized void enableTermVectorOffsets(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null || current.storeTermVectors != Boolean.TRUE) {
      illegalState(fieldName, "cannot enable termVectorOffsets when termVectors haven't been enabled");
    }
    if (current.storeTermVectorOffsets != Boolean.TRUE) {
      current.storeTermVectorOffsets = Boolean.TRUE;
      changed();
    }
  }

  /** Disable term vector offsets for this field.  This can be changed at any time. */
  public synchronized void disableTermVectorOffsets(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectorOffsets == Boolean.TRUE) {
      current.storeTermVectorOffsets = Boolean.FALSE;
      changed();
    }
  }

  public synchronized boolean getTermVectorOffsets(String fieldName) {
    return getFieldType(fieldName).storeTermVectorOffsets == Boolean.TRUE;
  }

  /** Enable term vector positions for this field.  This can be changed at any time. */
  public synchronized void enableTermVectorPositions(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null || current.storeTermVectors != Boolean.TRUE) {
      illegalState(fieldName, "cannot enable termVectorPositions when termVectors haven't been enabled");
    }
    if (current.storeTermVectorPositions != Boolean.TRUE) {
      current.storeTermVectorPositions = Boolean.TRUE;
      changed();
    }
  }

  /** Disable term vector positions for this field.  This can be changed at any time. */
  public synchronized void disableTermVectorPositions(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectorPositions == Boolean.TRUE) {
      current.storeTermVectorPositions = Boolean.FALSE;
      changed();
    }
  }

  public synchronized boolean getTermVectorPositions(String fieldName) {
    return getFieldType(fieldName).storeTermVectorPositions == Boolean.TRUE;
  }

  /** Enable term vector payloads for this field.  This can be changed at any time. */
  public synchronized void enableTermVectorPayloads(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null || current.storeTermVectors != Boolean.TRUE) {
      illegalState(fieldName, "cannot enable termVectorPayloads when termVectors haven't been enabled");
    }
    if (current.storeTermVectorPositions != Boolean.TRUE) {
      illegalState(fieldName, "cannot enable termVectorPayloads when termVectorPositions haven't been enabled");
    }
    if (current.storeTermVectorPayloads != Boolean.TRUE) {
      current.storeTermVectorPayloads = Boolean.TRUE;
      changed();
    }
  }

  /** Disable term vector payloads for this field.  This can be changed at any time. */
  public synchronized void disableTermVectorPayloads(String fieldName) {
    FieldType current = getFieldType(fieldName);
    if (current.storeTermVectorPayloads == Boolean.TRUE) {
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
    // Field must exist:
    return getFieldType(fieldName).indexOptions;
  }

  public synchronized void disableDocValues(String fieldName) {
    setDocValuesType(fieldName, DocValuesType.NONE);
  }

  public synchronized void setDocValuesType(String fieldName, DocValuesType dvType) {
    ensureWritable();
    if (dvType == null) {
      throw new NullPointerException("docValuesType cannot be null (field: \"" + fieldName + "\")");
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
    // Field must exist:
    return getFieldType(fieldName).docValuesType;
  }

  synchronized void recordValueType(String fieldName, ValueType valueType) {
    recordValueType(fieldName, valueType, false);
  }

  synchronized void recordValueType(String fieldName, ValueType valueType, boolean isUnique) {
    ensureWritable();
    indexedDocs = true;
    FieldType current = fields.get(fieldName);
    if (current == null) {
      if (valueType == ValueType.BIG_INT) {
        illegalState(fieldName, "you must first set the byte width for big_int fields");
      }
      current = newFieldType(fieldName);
      current.storedOnly = Boolean.FALSE;
      current.valueType = valueType;
      current.isUnique = isUnique;
      fields.put(fieldName, current);
      current.setDefaults();
      changed();
    } else if (current.valueType == ValueType.NONE) {
      if (valueType == ValueType.BIG_INT) {
        illegalState(fieldName, "you must first set the byte width for big_int fields");
      }

      if (current.isUnique != null && current.isUnique.booleanValue() != isUnique) {
        illegalState(fieldName, "cannot change to isUnique to " + isUnique + ": field was already with isUnique=" + current.isUnique);
      }

      FieldType sav = new FieldType(current);
      // This can happen if e.g. the app first calls FieldTypes.enableStored(...)
      boolean success = false;
      try {
        current.isUnique = isUnique;
        assert current.storedOnly == null;
        current.storedOnly = Boolean.FALSE;
        current.valueType = valueType;
        current.setDefaults();
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          fields.put(fieldName, sav);
        }
      }
      current.setDefaults();
      changed();
    } else if (current.valueType != valueType) {
      illegalState(fieldName, "cannot change from value type " + current.valueType + " to " + valueType);
    } else if (current.storedOnly == Boolean.TRUE) {
      illegalState(fieldName, "this field is only stored; use addStoredXXX instead");
    } else if (current.isUnique == null) {
      current.isUnique = isUnique;
      changed();
    } else if (current.isUnique != isUnique) {
      illegalState(fieldName, "cannot change isUnique from " + current.isUnique + " to " + isUnique);
    }
  }

  synchronized void recordStringAtomValueType(String fieldName, boolean isUnique) {
    ensureWritable();
    indexedDocs = true;
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.storedOnly = Boolean.FALSE;
      current.valueType = ValueType.ATOM;
      current.isBinary = Boolean.FALSE;
      current.isUnique = isUnique;
      fields.put(fieldName, current);
      current.setDefaults();
      changed();
    } else if (current.valueType == ValueType.NONE) {
      if (current.isUnique != null && current.isUnique.booleanValue() != isUnique) {
        illegalState(fieldName, "cannot change to isUnique to " + isUnique + ": field was already with isUnique=" + current.isUnique);
      }

      FieldType sav = new FieldType(current);
      // This can happen if e.g. the app first calls FieldTypes.enableStored(...)
      boolean success = false;
      try {
        current.isBinary = Boolean.FALSE;
        current.isUnique = isUnique;
        assert current.storedOnly == null;
        current.storedOnly = Boolean.FALSE;
        current.valueType = ValueType.ATOM;
        current.setDefaults();
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          fields.put(fieldName, sav);
        }
      }
      changed();
    } else if (current.valueType != ValueType.ATOM) {
      illegalState(fieldName, "cannot change from value type " + current.valueType + " to ATOM");
    } else if (current.isBinary != Boolean.FALSE) {
      illegalState(fieldName, "cannot change from binary to non-binary ATOM");
    } else if (current.isUnique != isUnique) {
      illegalState(fieldName, "cannot change isUnique from " + current.isUnique + " to " + isUnique);
    }
  }

  synchronized void recordBinaryAtomValueType(String fieldName, boolean isUnique) {
    ensureWritable();
    indexedDocs = true;
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.storedOnly = Boolean.FALSE;
      current.valueType = ValueType.ATOM;
      current.isBinary = Boolean.TRUE;
      current.isUnique = isUnique;
      fields.put(fieldName, current);
      current.setDefaults();
      changed();
    } else if (current.valueType == ValueType.NONE) {
      if (current.isUnique != null && current.isUnique.booleanValue() != isUnique) {
        illegalState(fieldName, "cannot change to isUnique to " + isUnique + ": field was already with isUnique=" + current.isUnique);
      }

      FieldType sav = new FieldType(current);
      // This can happen if e.g. the app first calls FieldTypes.enableStored(...)
      boolean success = false;
      try {
        current.isBinary = Boolean.TRUE;
        current.isUnique = isUnique;
        assert current.storedOnly == null;
        current.storedOnly = Boolean.FALSE;
        current.valueType = ValueType.ATOM;
        current.setDefaults();
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          fields.put(fieldName, sav);
        }
      }
      current.setDefaults();
      changed();
    } else if (current.valueType != ValueType.ATOM) {
      illegalState(fieldName, "cannot change from value type " + current.valueType + " to ATOM");
    } else if (current.isBinary != Boolean.TRUE) {
      illegalState(fieldName, "cannot change from string to binary ATOM");
    } else if (current.isUnique != isUnique) {
      illegalState(fieldName, "cannot change isUnique from " + current.isUnique + " to " + isUnique);
    }
  }

  synchronized void recordStoredValueType(String fieldName, ValueType valueType) {
    ensureWritable();
    indexedDocs = true;
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.storedOnly = Boolean.TRUE;
      current.valueType = valueType;
      current.isUnique = Boolean.FALSE;
      current.indexOptionsSet = true;
      current.indexOptions = IndexOptions.NONE;
      current.docValuesTypeSet = true;
      current.docValuesType = DocValuesType.NONE;
      fields.put(fieldName, current);
      current.setDefaults();
      changed();

    } else if (current.storedOnly == Boolean.FALSE) {
      illegalState(fieldName, "cannot addStored: field was already added non-stored");
    } else if (current.storedOnly == null) {

      if (current.indexOptionsSet && current.indexOptions != IndexOptions.NONE) {
        illegalState(fieldName, "cannot addStored: field is already indexed with indexOptions=" + current.indexOptions);
      }

      // nocommit why not?
      /*
      if (current.docValuesTypeSet && current.docValuesType != DocValuesType.NONE) {
        illegalState(fieldName, "cannot addStored: field already has docValuesType=" + current.docValuesType);
      }
      */

      // All methods that set valueType also set storedOnly to false:
      assert current.valueType == ValueType.NONE;

      FieldType sav = new FieldType(current);
      boolean success = false;
      try {
        current.storedOnly = Boolean.TRUE;
        current.valueType = valueType;
        current.indexOptions = IndexOptions.NONE;
        if (current.docValuesTypeSet == false) {
          current.docValuesType = DocValuesType.NONE;
          current.docValuesTypeSet = true;
        }
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          fields.put(fieldName, sav);
        }
      }
      current.setDefaults();
      changed();
    }
  }

  synchronized void recordLargeTextType(String fieldName, boolean allowStored, boolean allowIndexed) {
    ensureWritable();
    indexedDocs = true;
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.storedOnly = Boolean.FALSE;
      current.valueType = ValueType.TEXT;
      fields.put(fieldName, current);
      if (allowStored == false) {
        current.stored = Boolean.FALSE;
      }
      if (allowIndexed == false) {
        assert current.indexOptions == IndexOptions.NONE: "got " + current.indexOptions;
        current.indexOptionsSet = true;
      }
      current.setDefaults();
      changed();
    } else if (current.valueType == ValueType.NONE) {
      // This can happen if e.g. the app first calls FieldTypes.enableStored(...)
      FieldType sav = new FieldType(current);
      boolean success = false;
      try {
        assert current.storedOnly == null;
        current.storedOnly = Boolean.FALSE;
        current.valueType = ValueType.TEXT;
        if (allowStored == false) {
          if (current.stored == Boolean.TRUE) {
            illegalState(fieldName, "can only store String large text fields");
          } else if (current.stored == null) {
            current.stored = Boolean.FALSE;
          }
        }
        if (allowIndexed == false) {
          if (current.indexOptionsSet == false) {
            assert current.indexOptions == IndexOptions.NONE;
            current.indexOptionsSet = true;
          } else if (current.indexOptions != IndexOptions.NONE) {
            illegalState(fieldName, "this field is already indexed with indexOptions=" + current.indexOptions);
          }
        }
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          fields.put(fieldName, sav);
        }
      }
      current.setDefaults();
      changed();
    } else if (current.valueType != ValueType.TEXT) {
      illegalState(fieldName, "cannot change from value type " + current.valueType + " to " + ValueType.TEXT);
    } else if (allowIndexed == false && current.indexOptionsSet && current.indexOptions != IndexOptions.NONE) {
      illegalState(fieldName, "this field is already indexed with indexOptions=" + current.indexOptions);
    } else if (allowIndexed && current.indexOptionsSet && current.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "this field is already disabled for indexing");
    } else if (allowStored == false && current.stored == Boolean.TRUE) {
      illegalState(fieldName, "this field was already enabled for storing");
    }
  }

  public void setSortLocale(String fieldName, Locale locale) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.sortLocale = locale;
      current.sortCollator = Collator.getInstance(locale);
      fields.put(fieldName, current);
      changed();
    } else if (current.sortLocale == null || current.valueType == null) {
      current.sortLocale = locale;
      boolean success = false;
      try {
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.sortLocale = null;
        }
      }
      // TODO: can we make it easy to swap in icu?
      current.sortCollator = Collator.getInstance(locale);
      changed();
    } else if (locale.equals(current.sortLocale) == false) {
      illegalState(fieldName, "sortLocale can only be set before indexing");
    }
  }

  public static interface SortKey {
    Comparable getKey(Object o);
  }

  /** NOTE: does not persist; you must set this each time you open a new reader. */
  public void setSortKey(String fieldName, SortKey sortKey) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.sortKey = sortKey;
      fields.put(fieldName, current);
    } else if (current.valueType == ValueType.ATOM) {
      current.sortKey = sortKey;
    } else {
      illegalState(fieldName, "sortKey can only be set for ATOM fields; got value type=" + current.valueType);
    }
  }

  public Locale getSortLocale(String fieldName) {
    // Field must exist:
    return getFieldType(fieldName).sortLocale;
  }

  /** Each value in this field will be unique (never occur in more than one document).  IndexWriter validates this.  */
  public void setIsUnique(String fieldName) {
    FieldType current = fields.get(fieldName);
    if (current == null) {
      current = newFieldType(fieldName);
      current.isUnique = Boolean.TRUE;
      fields.put(fieldName, current);
      changed();
    } else if (current.isUnique == Boolean.FALSE) {
      illegalState(fieldName, "cannot change isUnique from FALSE to TRUE");
    }
  }

  /** Returns true if values in this field must be unique across all documents in the index. */
  public synchronized boolean getIsUnique(String fieldName) {   
    FieldType fieldType = fields.get(fieldName);
    return fieldType != null && fieldType.isUnique == Boolean.TRUE;
  }

  public Term newBigIntTerm(String fieldName, BigInteger token) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term: this field was not indexed");
    }

    BytesRef bytes;

    switch (fieldType.valueType) {
    case BIG_INT:
      bytes = NumericUtils.bigIntToBytes(token, fieldType.bigIntByteWidth);
      break;
    default:
      illegalState(fieldName, "cannot create big int term when value type=" + fieldType.valueType);
      // Dead code but javac disagrees:
      bytes = null;
    }

    return new Term(fieldName, bytes);
  }

  /** Returns a query matching all documents that have this int term. */
  public Query newBigIntTermQuery(String fieldName, BigInteger token) {
    return new TermQuery(newBigIntTerm(fieldName, token));
  }

  public Term newIntTerm(String fieldName, int token) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term: this field was not indexed");
    }

    BytesRef bytes;

    switch (fieldType.valueType) {
    case INT:
      bytes = NumericUtils.intToBytes(token);
      break;
    default:
      illegalState(fieldName, "cannot create int term when value type=" + fieldType.valueType);
      // Dead code but javac disagrees:
      bytes = null;
    }

    return new Term(fieldName, bytes);
  }

  /** Returns a query matching all documents that have this int term. */
  public Query newIntTermQuery(String fieldName, int token) {
    return new TermQuery(newIntTerm(fieldName, token));
  }

  public Term newFloatTerm(String fieldName, float token) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term: this field was not indexed");
    }

    BytesRef bytes;

    switch (fieldType.valueType) {
    case FLOAT:
      bytes = NumericUtils.floatToBytes(token);
      break;
    default:
      illegalState(fieldName, "cannot create float term when value type=" + fieldType.valueType);
      // Dead code but javac disagrees:
      bytes = null;
    }

    return new Term(fieldName, bytes);
  }

  /** Returns a query matching all documents that have this int term. */
  public Query newFloatTermQuery(String fieldName, float token) {
    return new TermQuery(newFloatTerm(fieldName, token));
  }

  public Term newLongTerm(String fieldName, long token) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term: this field was not indexed");
    }

    BytesRef bytes;

    switch (fieldType.valueType) {
    case LONG:
      bytes = NumericUtils.longToBytes(token);
      break;
    default:
      illegalState(fieldName, "cannot create long term when value type=" + fieldType.valueType);
      // Dead code but javac disagrees:
      bytes = null;
    }

    return new Term(fieldName, bytes);
  }

  /** Returns a query matching all documents that have this long term. */
  public Query newLongTermQuery(String fieldName, long token) {
    return new TermQuery(newLongTerm(fieldName, token));
  }

  public Term newDoubleTerm(String fieldName, double token) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term: this field was not indexed");
    }

    BytesRef bytes;

    switch (fieldType.valueType) {
    case DOUBLE:
      bytes = NumericUtils.doubleToBytes(token);
      break;
    default:
      illegalState(fieldName, "cannot create double term when value type=" + fieldType.valueType);
      // Dead code but javac disagrees:
      bytes = null;
    }

    return new Term(fieldName, bytes);
  }

  /** Returns a query matching all documents that have this int term. */
  public Query newDoubleTermQuery(String fieldName, double token) {
    return new TermQuery(newDoubleTerm(fieldName, token));
  }

  public Query newBinaryTermQuery(String fieldName, byte[] token) {
    return newBinaryTermQuery(fieldName, new BytesRef(token));
  }

  /** Returns a query matching all documents that have this binary token. */
  public Query newBinaryTermQuery(String fieldName, BytesRef token) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create term query: this field was not indexed");
    }

    // Field must be binary:
    if (fieldType.valueType != ValueType.BINARY && fieldType.valueType != ValueType.ATOM) {
      illegalState(fieldName, "binary term query must have value type BINARY or ATOM; got " + fieldType.valueType);
    }

    return new TermQuery(new Term(fieldName, token));
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
      illegalState(fieldName, "string term query must have value type TEXT, SHORT_TEXT or ATOM; got " + fieldType.valueType);
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
      illegalState(fieldName, "boolean term query must have value type BOOLEAN; got " + fieldType.valueType);
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
      illegalState(fieldName, "inet address term query must have value type INET_ADDRESS; got " + fieldType.valueType);
    }

    return new TermQuery(new Term(fieldName, new BytesRef(token.getAddress())));
  }

  private String getRangeFilterDesc(FieldType fieldType, Object min, boolean minInclusive, Object max, boolean maxInclusive) {
    StringBuilder sb = new StringBuilder();
    if (minInclusive) {
      sb.append('[');
    } else {
      sb.append('{');
    }
    if (min == null) {
      sb.append('*');
    } else {
      sb.append(min);
    }
    sb.append(" TO ");
    if (max == null) {
      sb.append('*');
    } else {
      sb.append(max);
    }      
    if (maxInclusive) {
      sb.append(']');
    } else {
      sb.append('}');
    }
    return sb.toString();
  }

  public Filter newDocValuesRangeFilter(String fieldName, Date min, boolean minInclusive, Date max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be DATE type:
    if (fieldType.valueType != ValueType.DATE) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed as value type DATE; got: " + fieldType.valueType);
    }

    // Field must have doc values:
    if (fieldType.docValuesType != DocValuesType.NUMERIC) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed with NUMERIC doc values; got: " + fieldType.docValuesType);
    }

    return DocValuesRangeFilter.newLongRange(fieldName,
                                             min == null ? null : min.getTime(),
                                             max == null ? null : max.getTime(),
                                             minInclusive, maxInclusive,
                                             getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newIntDocValuesRangeFilter(String fieldName, Integer min, boolean minInclusive, Integer max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must have doc values:
    if (fieldType.docValuesType != DocValuesType.NUMERIC) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed with NUMERIC doc values; got: " + fieldType.docValuesType);
    }

    if (fieldType.valueType != ValueType.INT) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type INT; got: " + fieldType.valueType);
    }

    return DocValuesRangeFilter.newIntRange(fieldName,
                                            min == null ? null : min.intValue(),
                                            max == null ? null : max.intValue(),
                                            minInclusive,
                                            maxInclusive,
                                            getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newLongDocValuesRangeFilter(String fieldName, Long min, boolean minInclusive, Long max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must have doc values:
    if (fieldType.docValuesType != DocValuesType.NUMERIC) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed with NUMERIC doc values; got: " + fieldType.docValuesType);
    }

    if (fieldType.valueType != ValueType.LONG) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type LONG; got: " + fieldType.valueType);
    }

    return DocValuesRangeFilter.newLongRange(fieldName,
                                             min == null ? null : min.longValue(),
                                             max == null ? null : max.longValue(),
                                             minInclusive,
                                             maxInclusive,
                                             getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newHalfFloatDocValuesRangeFilter(String fieldName, Float min, boolean minInclusive, Float max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must have doc values:
    if (fieldType.docValuesType != DocValuesType.NUMERIC) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed with NUMERIC doc values; got: " + fieldType.docValuesType);
    }

    if (fieldType.valueType != ValueType.HALF_FLOAT) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type HALF_FLOAT; got: " + fieldType.valueType);
    }

    return DocValuesRangeFilter.newHalfFloatRange(fieldName,
                                                  min == null ? null : min.floatValue(),
                                                  max == null ? null : max.floatValue(),
                                                  minInclusive,
                                                  maxInclusive,
                                                  getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newFloatDocValuesRangeFilter(String fieldName, Float min, boolean minInclusive, Float max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must have doc values:
    if (fieldType.docValuesType != DocValuesType.NUMERIC) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed with NUMERIC doc values; got: " + fieldType.docValuesType);
    }

    if (fieldType.valueType != ValueType.FLOAT) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type FLOAT; got: " + fieldType.valueType);
    }

    return DocValuesRangeFilter.newFloatRange(fieldName,
                                              min == null ? null : min.floatValue(),
                                              max == null ? null : max.floatValue(),
                                              minInclusive,
                                              maxInclusive,
                                              getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newDoubleDocValuesRangeFilter(String fieldName, Double min, boolean minInclusive, Double max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must have doc values:
    if (fieldType.docValuesType != DocValuesType.NUMERIC) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed with NUMERIC doc values; got: " + fieldType.docValuesType);
    }

    if (fieldType.valueType != ValueType.DOUBLE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type DOUBLE; got: " + fieldType.valueType);
    }

    return DocValuesRangeFilter.newDoubleRange(fieldName,
                                               min == null ? null : min.doubleValue(),
                                               max == null ? null : max.doubleValue(),
                                               minInclusive,
                                               maxInclusive,
                                               getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newStringDocValuesRangeFilter(String fieldName, String minTerm, boolean minInclusive, String maxTerm, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    BytesRef min;
    BytesRef max;
    // nocommit should we also index collation keys / allow ranges?
    if (fieldType.sortLocale != null) {
      synchronized (fieldType.sortCollator) {
        min = minTerm == null ? null : new BytesRef(fieldType.sortCollator.getCollationKey(minTerm).toByteArray());
        max = maxTerm == null ? null : new BytesRef(fieldType.sortCollator.getCollationKey(maxTerm).toByteArray());
      }
    } else {
      min = minTerm == null ? null : new BytesRef(minTerm);
      max = maxTerm == null ? null : new BytesRef(maxTerm);
    }
    
    return newBinaryDocValuesRangeFilter(fieldName, minTerm == null ? null : min, minInclusive, maxTerm == null ? null : max, maxInclusive);
  }

  public Filter newBinaryDocValuesRangeFilter(String fieldName, byte[] minTerm, boolean minInclusive, byte[] maxTerm, boolean maxInclusive) {
    return newBinaryDocValuesRangeFilter(fieldName, minTerm == null ? null : new BytesRef(minTerm), minInclusive, maxTerm == null ? null : new BytesRef(maxTerm), maxInclusive);
  }

  public Filter newBinaryDocValuesRangeFilter(String fieldName, BytesRef minTerm, boolean minInclusive, BytesRef maxTerm, boolean maxInclusive) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must have sorted doc values:
    if (fieldType.docValuesType != DocValuesType.SORTED && fieldType.docValuesType != DocValuesType.SORTED_SET) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed with SORTED or SORTED_SET doc values; got: " + fieldType.docValuesType);
    }

    if (fieldType.valueType != ValueType.ATOM && fieldType.valueType != ValueType.BINARY) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed as value type ATOM or BINARY; got: " + fieldType.valueType);
    }

    return DocValuesRangeFilter.newBytesRefRange(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                                                 getRangeFilterDesc(fieldType, minTerm, minInclusive, maxTerm, maxInclusive));
  }

  public Filter newDocValuesRangeFilter(String fieldName, InetAddress min, boolean minInclusive, InetAddress max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be InetAddress type:
    if (fieldType.valueType != ValueType.INET_ADDRESS) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed as value type INET_ADDRESS; got: " + fieldType.valueType);
    }

    // Field must have sorted doc values:
    if (fieldType.docValuesType != DocValuesType.SORTED && fieldType.docValuesType != DocValuesType.SORTED_SET) {
      illegalState(fieldName, "cannot create doc values range query: this field was not indexed with SORTED or SORTED_SET doc values; got: " + fieldType.docValuesType);
    }

    BytesRef minTerm = min == null ? null : new BytesRef(min.getAddress());
    BytesRef maxTerm = max == null ? null : new BytesRef(max.getAddress());

    return DocValuesRangeFilter.newBytesRefRange(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                                                 getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newIntRangeFilter(String fieldName, Integer min, boolean minInclusive, Integer max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed");
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed for fast ranges");
    }

    if (fieldType.valueType != ValueType.INT) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type INT; got: " + fieldType.valueType);
    }

    BytesRef minTerm = min == null ? null : NumericUtils.intToBytes(min.intValue());
    BytesRef maxTerm = max == null ? null : NumericUtils.intToBytes(max.intValue());

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                               getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newLongRangeFilter(String fieldName, Long min, boolean minInclusive, Long max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed");
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed for fast ranges");
    }

    if (fieldType.valueType != ValueType.LONG) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type LONG; got: " + fieldType.valueType);
    }

    BytesRef minTerm = min == null ? null : NumericUtils.longToBytes(min.longValue());
    BytesRef maxTerm = max == null ? null : NumericUtils.longToBytes(max.longValue());

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                               getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newBigIntRangeFilter(String fieldName, BigInteger min, boolean minInclusive, BigInteger max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed");
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed for fast ranges");
    }

    if (fieldType.valueType != ValueType.BIG_INT) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type BIG_INT; got: " + fieldType.valueType);
    }

    BytesRef minTerm = min == null ? null : NumericUtils.bigIntToBytes(min, fieldType.bigIntByteWidth.intValue());
    BytesRef maxTerm = max == null ? null : NumericUtils.bigIntToBytes(max, fieldType.bigIntByteWidth.intValue());

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                               getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newHalfFloatRangeFilter(String fieldName, Float min, boolean minInclusive, Float max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed");
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed for fast ranges");
    }

    if (fieldType.valueType != ValueType.HALF_FLOAT) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type HALF_FLOAT; got: " + fieldType.valueType);
    }

    BytesRef minTerm = min == null ? null : NumericUtils.halfFloatToBytes(min.floatValue());
    BytesRef maxTerm = max == null ? null : NumericUtils.halfFloatToBytes(max.floatValue());

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                               getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newFloatRangeFilter(String fieldName, Float min, boolean minInclusive, Float max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed");
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed for fast ranges");
    }

    if (fieldType.valueType != ValueType.FLOAT) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type FLOAT; got: " + fieldType.valueType);
    }

    BytesRef minTerm = min == null ? null : NumericUtils.floatToBytes(min.floatValue());
    BytesRef maxTerm = max == null ? null : NumericUtils.floatToBytes(max.floatValue());

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                               getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newDoubleRangeFilter(String fieldName, Double min, boolean minInclusive, Double max, boolean maxInclusive) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed");
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed for fast ranges");
    }

    if (fieldType.valueType != ValueType.DOUBLE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type DOUBLE; got: " + fieldType.valueType);
    }

    BytesRef minTerm = min == null ? null : NumericUtils.doubleToBytes(min.doubleValue());
    BytesRef maxTerm = max == null ? null : NumericUtils.doubleToBytes(max.doubleValue());

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                               getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  public Filter newStringRangeFilter(String fieldName, String minTerm, boolean minInclusive, String maxTerm, boolean maxInclusive) {
    return newBinaryRangeFilter(fieldName, minTerm == null ? null : new BytesRef(minTerm), minInclusive, maxTerm == null ? null : new BytesRef(maxTerm), maxInclusive);
  }

  public Filter newBinaryRangeFilter(String fieldName, byte[] minTerm, boolean minInclusive, byte[] maxTerm, boolean maxInclusive) {
    return newBinaryRangeFilter(fieldName, minTerm == null ? null : new BytesRef(minTerm), minInclusive, maxTerm == null ? null : new BytesRef(maxTerm), maxInclusive);
  }

  public Filter newBinaryRangeFilter(String fieldName, BytesRef minTerm, boolean minInclusive, BytesRef maxTerm, boolean maxInclusive) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed");
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed for fast ranges");
    }

    if (fieldType.valueType != ValueType.ATOM && fieldType.valueType != ValueType.BINARY) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed as value type ATOM or BINARY; got: " + fieldType.valueType);
    }

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                               getRangeFilterDesc(fieldType, minTerm, minInclusive, maxTerm, maxInclusive));
  }

  // TODO: Date sugar for a range filter matching a specific hour/day/month/year/etc.?  need locale/timezone... should we use DateTools?

  public Filter newRangeFilter(String fieldName, Date min, boolean minInclusive, Date max, boolean maxInclusive) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed");
    }

    if (fieldType.valueType != ValueType.DATE) {
      illegalState(fieldName, "cannot create range filter: expected value type=DATE but got: " + fieldType.valueType);
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed for fast ranges");
    }

    BytesRef minTerm = min == null ? null : NumericUtils.longToBytes(min.getTime());
    BytesRef maxTerm = max == null ? null : NumericUtils.longToBytes(max.getTime());

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                               getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

  // TODO: also add "range filter using net mask" sugar version
  public Filter newRangeFilter(String fieldName, InetAddress min, boolean minInclusive, InetAddress max, boolean maxInclusive) {

    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    // Field must be indexed:
    if (fieldType.indexOptions == IndexOptions.NONE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed");
    }

    if (fieldType.valueType != ValueType.INET_ADDRESS) {
      illegalState(fieldName, "cannot create range filter: expected value type=INET_ADDRESS but got: " + fieldType.valueType);
    }

    if (fieldType.fastRanges != Boolean.TRUE) {
      illegalState(fieldName, "cannot create range filter: this field was not indexed for fast ranges");
    }

    BytesRef minTerm = min == null ? null : new BytesRef(min.getAddress());
    BytesRef maxTerm = max == null ? null : new BytesRef(max.getAddress());

    return new TermRangeFilter(fieldName, minTerm, maxTerm, minInclusive, maxInclusive,
                               getRangeFilterDesc(fieldType, min, minInclusive, max, maxInclusive));
  }

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
          sortField = new SortedNumericSortField(fieldName, SortField.Type.INT, reverse, fieldType.numericSelector);
        } else {
          sortField = new SortField(fieldName, SortField.Type.INT, reverse);
        }
        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Integer.MIN_VALUE);
          } else {
            sortField.setMissingValue(Integer.MAX_VALUE);
          }
        } else {
          assert fieldType.sortMissingLast == Boolean.FALSE;
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Integer.MAX_VALUE);
          } else {
            sortField.setMissingValue(Integer.MIN_VALUE);
          }
        }
        return sortField;
      }

    case HALF_FLOAT:
      {
        final float missingValue;

        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            missingValue = Float.NEGATIVE_INFINITY;
          } else {
            missingValue = Float.POSITIVE_INFINITY;
          }
        } else {
          assert fieldType.sortMissingLast == Boolean.FALSE;
          if (reverse.booleanValue()) {
            missingValue = Float.POSITIVE_INFINITY;
          } else {
            missingValue = Float.NEGATIVE_INFINITY;
          }
        }

        SortField sortField;
        FieldComparatorSource compSource;
        if (fieldType.multiValued == Boolean.TRUE) {
          compSource = new FieldComparatorSource() {
              @Override
              public FieldComparator<Float> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
                return new HalfFloatComparator(numHits, fieldName, missingValue) {
                  @Override
                  protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                    assert field.equals(fieldName);
                    SortedNumericDocValues dvs = context.reader().getSortedNumericDocValues(fieldName);
                    assert dvs != null;
                    return SortedNumericSelector.wrap(dvs, fieldType.numericSelector);
                  }
                };
              }
            };
        } else {
          compSource = new FieldComparatorSource() {
              @Override
              public FieldComparator<Float> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
                return new HalfFloatComparator(numHits, fieldName, missingValue);
              }
            };
        }

        sortField = new SortField(fieldName, compSource, reverse) {
            @Override
            public String toString() {
              return "<halffloat" + ": \"" + fieldName + "\" missingLast=" + fieldType.sortMissingLast + ">";
            }
          };

        return sortField;
      }

    case FLOAT:
      {
        SortField sortField;
        if (fieldType.multiValued == Boolean.TRUE) {
          sortField = new SortedNumericSortField(fieldName, SortField.Type.FLOAT, reverse, fieldType.numericSelector);
        } else {
          sortField = new SortField(fieldName, SortField.Type.FLOAT, reverse);
        }
        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Float.NEGATIVE_INFINITY);
          } else {
            sortField.setMissingValue(Float.POSITIVE_INFINITY);
          }
        } else {
          assert fieldType.sortMissingLast == Boolean.FALSE;
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
          sortField = new SortedNumericSortField(fieldName, SortField.Type.LONG, reverse, fieldType.numericSelector);
        } else {
          sortField = new SortField(fieldName, SortField.Type.LONG, reverse);
        }
        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Long.MIN_VALUE);
          } else {
            sortField.setMissingValue(Long.MAX_VALUE);
          }
        } else {
          assert fieldType.sortMissingLast == Boolean.FALSE;
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
          sortField = new SortedNumericSortField(fieldName, SortField.Type.DOUBLE, reverse, fieldType.numericSelector);
        } else {
          sortField = new SortField(fieldName, SortField.Type.DOUBLE, reverse);
        }
        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Double.NEGATIVE_INFINITY);
          } else {
            sortField.setMissingValue(Double.POSITIVE_INFINITY);
          }
        } else {
          assert fieldType.sortMissingLast == Boolean.FALSE;
          if (reverse.booleanValue()) {
            sortField.setMissingValue(Double.POSITIVE_INFINITY);
          } else {
            sortField.setMissingValue(Double.NEGATIVE_INFINITY);
          }
        }
        return sortField;
      }

    case BIG_INT:
      {
        SortField sortField;
        FieldComparatorSource compSource;
        if (fieldType.multiValued == Boolean.TRUE) {
          compSource = new FieldComparatorSource() {
              @Override
              public FieldComparator<BigInteger> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
                return new BigIntComparator(numHits, fieldName, fieldType.bigIntByteWidth, fieldType.sortMissingLast != Boolean.FALSE) {
                  @Override
                  protected SortedDocValues getDocValues(LeafReaderContext context) throws IOException {
                    SortedSetDocValues dvs = context.reader().getSortedSetDocValues(fieldName);
                    assert dvs != null;
                    return SortedSetSelector.wrap(dvs, fieldType.sortedSetSelector);
                  }
                };
              }
            };
        } else {
          compSource = new FieldComparatorSource() {
              @Override
              public FieldComparator<BigInteger> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
                return new BigIntComparator(numHits, fieldName, fieldType.bigIntByteWidth, fieldType.sortMissingLast != Boolean.FALSE);
              }
            };
        }

        return new SortField(fieldName, compSource, reverse) {
            @Override
            public String toString() {
              return "<bigint" + ": \"" + fieldName + "\" missingLast=" + fieldType.sortMissingLast + ">";
            }
          };
      }

    case SHORT_TEXT:
    case ATOM:
    case BINARY:
    case BOOLEAN:
    case INET_ADDRESS:
      SortField sortField;
      {
        if (fieldType.multiValued == Boolean.TRUE) {
          sortField = new SortedSetSortField(fieldName, reverse, fieldType.sortedSetSelector);
        } else if (fieldType.sortKey != null) {
          FieldComparatorSource compSource = new FieldComparatorSource() {
              @Override
              public FieldComparator<BytesRef> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
                return new SortKeyComparator(numHits, fieldName, fieldType.sortMissingLast == Boolean.TRUE, fieldType.sortKey);
              }
            };

          sortField = new SortField(fieldName, compSource, reverse) {
              @Override
              public String toString() {
                return "<custom-sort-key" + ": \"" + fieldName + "\" missingLast=" + fieldType.sortMissingLast + ">";
              }
            };

        } else if (fieldType.docValuesType == DocValuesType.BINARY) {
          sortField = new SortField(fieldName, SortField.Type.STRING_VAL, reverse);
        } else {
          sortField = new SortField(fieldName, SortField.Type.STRING, reverse);
        }

        if (fieldType.sortMissingLast == Boolean.TRUE) {
          if (reverse.booleanValue()) {
            sortField.setMissingValue(SortField.STRING_FIRST);
          } else {
            sortField.setMissingValue(SortField.STRING_LAST);
          }
        } else {
          assert fieldType.sortMissingLast == Boolean.FALSE;
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
      illegalState(fieldName, "unhandled sort case, value type=" + fieldType.valueType);

      // Dead code but javac disagrees:
      return null;
    }
  }

  /** Returns a {@link Filter} accepting documents that have this field. */
  public Filter newFieldExistsFilter(String fieldName) {
    if (enableExistsFilters == false) {
      throw new IllegalStateException("field exists filter was disabled");
    }
    // nocommit just use FieldValueFilter if field is DV'd? and then don't index such fields into FIELD_NAMES_FIELD?

    return new FieldExistsFilter(fieldName);
  }

  private synchronized void changed() {
    changed(true);
  }

  private synchronized void changed(boolean requireWritable) {
    if (requireWritable) {
      ensureWritable();
    }
    changeCount++;
  }

  public synchronized long getChangeCount() {
    return changeCount;
  }

  private synchronized void ensureWritable() {
    if (readOnly) {
      throw new IllegalStateException("cannot make changes to a read-only FieldTypes (it was opened from an IndexReader, not an IndexWriter)");
    }
    if (closed) {
      throw new AlreadyClosedException("this FieldTypes has been closed");
    }
  }

  // nocommit make this private, ie only IW can invoke it
  public void close() {
    closed = true;
  }
  
  static void illegalState(String fieldName, String message) {
    throw new IllegalStateException("field \"" + fieldName + "\": " + message);
  }

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

    int count = fields.size();
    out.writeVInt(count);
    int count2 = 0;
    for(FieldType fieldType : fields.values()) {
      fieldType.write(out);
      count2++;
    }
    assert count == count2;

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

  public static FieldTypes getFieldTypes(Directory dir, Analyzer defaultQueryAnalyzer) throws IOException {
    SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    return getFieldTypes(infos.getUserData(), defaultQueryAnalyzer, IndexSearcher.getDefaultSimilarity());
  }

  public static FieldTypes getFieldTypes(IndexCommit commit, Analyzer defaultQueryAnalyzer) throws IOException {
    return getFieldTypes(commit, defaultQueryAnalyzer, IndexSearcher.getDefaultSimilarity());
  }

  public static FieldTypes getFieldTypes(IndexCommit commit, Analyzer defaultQueryAnalyzer, Similarity defaultSimilarity) throws IOException {
    return getFieldTypes(commit.getUserData(), defaultQueryAnalyzer, defaultSimilarity);
  }

  public static FieldTypes getFieldTypes(Map<String,String> commitUserData, Analyzer defaultQueryAnalyzer, Similarity defaultSimilarity) throws IOException {
    return new FieldTypes(commitUserData, defaultQueryAnalyzer, defaultSimilarity);
  }

  public Iterable<String> getFieldNames() {
    return Collections.unmodifiableSet(fields.keySet());
  }

  // nocommit on exception (mismatched schema), this should ensure no changes were actually made:
  public synchronized void addAll(FieldTypes in) {
    Map<String,FieldType> sav = new HashMap<>();
    for(Map.Entry<String,FieldType> ent : fields.entrySet()) {
      sav.put(ent.getKey(), new FieldType(ent.getValue()));
    }

    boolean success = false;
    try {
      for (FieldType fieldType : in.fields.values()) {
        FieldType curFieldType = fields.get(fieldType.name);
        if (curFieldType == null) {
          fields.put(fieldType.name, new FieldType(fieldType));
        } else {
          curFieldType.merge(fieldType);
        }
      }
      success = true;
    } finally {
      if (success == false) {
        // Restore original fields:
        fields.clear();
        for(Map.Entry<String,FieldType> ent : sav.entrySet()) {
          fields.put(ent.getKey(), ent.getValue());
        }
      }
    }
  }

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

  public synchronized void clear() {
    fields.clear();
    enableExistsFilters = true;
    indexedDocs = false;
    addFieldNamesField();
  }

  static void writeNullableInteger(DataOutput out, Integer value) throws IOException {
    if (value == null) {
      out.writeByte((byte) 0);
    } else {
      out.writeByte((byte) 1);
      out.writeVInt(value.intValue());
    }
  }

  static Integer readNullableInteger(DataInput in) throws IOException {
    if (in.readByte() == 0) {
      return null;
    } else {
      return in.readVInt();
    }
  }

  static void writeNullableBoolean(DataOutput out, Boolean value) throws IOException {
    if (value == null) {
      out.writeByte((byte) 0);
    } else if (value == Boolean.TRUE) {
      out.writeByte((byte) 1);
    } else {
      out.writeByte((byte) 2);
    }
  }

  static Boolean readNullableBoolean(DataInput in) throws IOException {
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

  static void writeNullableString(DataOutput out, String value) throws IOException {
    if (value == null) {
      out.writeByte((byte) 0);
    } else {
      out.writeByte((byte) 1);
      out.writeString(value);
    }
  }

  static String readNullableString(DataInput in) throws IOException {
    byte b = in.readByte();
    if (b == 0) {
      return null;
    } else if (b == 1) {
      return in.readString();
    } else {
      throw new CorruptIndexException("invalid byte for nullable string: " + b, in);
    }
  }

  /** Returns true if terms should be left-zero-padded (sorted as if they were right-justified). */
  public boolean rightJustifyTerms(String fieldName) {
    FieldType fieldType = fields.get(fieldName);
    return fieldType != null && fieldType.valueType == ValueType.BIG_INT;
  }

  public ValueType getValueType(String fieldName) {
    // Field must exist:
    FieldType fieldType = getFieldType(fieldName);

    return fieldType.valueType;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FieldTypes:");
    for(FieldType fieldType : fields.values()) {
      sb.append('\n');
      sb.append(fieldType);
    }

    return sb.toString();
  }
}
