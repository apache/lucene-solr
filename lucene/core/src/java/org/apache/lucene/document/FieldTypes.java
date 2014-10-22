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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableFieldType;
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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

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
//   FieldInfos
//   SortField.type
//   DocValuesType
//   subclassing QueryParsers
//   PerFieldPF/DVF
//   PerFieldSimilarityWrapper
//   PerFieldAnalyzerWrapper
//   oal.document

// nocommit maybe have an across-the-board default for "stored"?

// nocommit should we validate field names here?

// nocommit can we somehow always store a "source"?

// nocommit make ValueType public?  add setter so you can set that too?

// language for the field?  (to default collator)

// nocommit sort order, sort options (e.g. for collation)
//   case sensitive, punc sensitive, accent sensitive
//   can we fold in ICUCollationDocValuesField somehow...

// nocommit need index vs search time analysis?

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

// nocommit just persist as FieldInfos?  but that's per-segment, and ... it enforces the low-level constraints?

// nocommit unique/primary key ?

// nocommit must document / make sugar for creating IndexSearcher w/ sim from this class

// nocommit fix all change methods to call validate / rollback

// nocommit boolean, float16

// nocommit collapse IndexableField/Type

// nocommit can we move multi-field-ness out of IW?  so IW only gets a single instance of each field

/** Records how each field is indexed, stored, etc.  This class persists
 *  its state using {@link IndexWriter#setCommitData}, using the
 *  {@link FieldTypes#FIELD_PROPERTIES_KEY} key. */

public class FieldTypes {

  /** Key used to store the field types inside {@link IndexWriter#setCommitData}. */
  public static final String FIELD_PROPERTIES_KEY = "field_properties";

  // nocommit reduce to just number, text, atom?
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

  private final IndexReader reader;

  // nocommit: messy: this is non-final because of circular dependency issues...
  private IndexWriter writer;

  private final Map<String,FieldType> fields = new HashMap<>();

  private final Analyzer defaultIndexAnalyzer;
  private final Analyzer defaultQueryAnalyzer;
  private final Similarity defaultSimilarity;

  /** Just like current oal.document.FieldType, except for each setting it can also record "not-yet-set". */
  static class FieldType implements IndexableFieldType {
    private final String name;

    public FieldType(String name) {
      this.name = name;
    }

    // nocommit volatile for all these instance vars:
    volatile ValueType valueType;
    volatile DocValuesType docValuesType;

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

    // Whether this field's values should be indexed as doc values for sorting:
    private volatile Boolean sortable;
    private volatile Boolean sortReversed;

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
          if (docValuesType != null) {
            illegalState(name, "type " + valueType + " cannot use docValuesType " + docValuesType);
          }
          break;
        case SHORT_TEXT:
          if (docValuesType != null && docValuesType != DocValuesType.BINARY && docValuesType != DocValuesType.SORTED && docValuesType != DocValuesType.SORTED_SET) {
            illegalState(name, "type " + valueType + " cannot use docValuesType " + docValuesType);
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

      // nocommit we need a separate "doc values disabled" setting?
      /*
      if (sortable == Boolean.TRUE && (docValuesType == null || docValuesType == DocValuesType.BINARY)) {
        illegalState(name, "cannot sort when DocValuesType=" + docValuesType);
      }
      */


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
      } else {
        b.append("unset");
      }
      b.append('\n');

      b.append("  docValuesType: ");
      if (docValuesType != null) {
        b.append(docValuesType);
      } else {
        b.append("unset");
      }
      b.append('\n');

      b.append("  indexOptions: ");
      if (indexOptions != null) {
        b.append(indexOptions);
      } else {
        b.append("unset");
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
  }

  /** Create a new index-time (writable) instance using the specified default analyzer, and {@link IndexSearcher#getDefaultSimilarity}
   *  similarity.  Note that you must call {@link #setIndexWriter} before changing any types. */
  public FieldTypes(Analyzer defaultAnalyzer) {
    this(defaultAnalyzer, defaultAnalyzer, IndexSearcher.getDefaultSimilarity());
  }

  /** Create a new index-time (writable) instance using the specified default analyzer and similarity.  Note that you must call {@link
   *  #setIndexWriter} before changing any types. */
  public FieldTypes(Analyzer defaultIndexAnalyzer, Analyzer defaultQueryAnalyzer, Similarity defaultSimilarity) {
    this.reader = null;
    this.defaultIndexAnalyzer = defaultIndexAnalyzer;
    this.defaultQueryAnalyzer = defaultQueryAnalyzer;
    this.defaultSimilarity = defaultSimilarity;
  }

  /** Create a new search-time (read-only) instance using the specified default analyzer, and {@link IndexSearcher#getDefaultSimilarity}
   *  similarity. */
  public FieldTypes(DirectoryReader reader, Analyzer defaultAnalyzer) throws IOException {
    this(reader, defaultAnalyzer, IndexSearcher.getDefaultSimilarity());
  }

  /** Create a new search-time (read-only) instance using the specified default analyzer. */
  public FieldTypes(DirectoryReader reader, Analyzer defaultQueryAnalyzer, Similarity defaultSimilarity) throws IOException {
    this.reader = reader;
    this.defaultIndexAnalyzer = null;
    this.defaultQueryAnalyzer = defaultQueryAnalyzer;
    this.defaultSimilarity = defaultSimilarity;
    loadFields(reader.getIndexCommit().getUserData());
  }

  public synchronized void setIndexWriter(IndexWriter writer) throws IOException {
    if (this.writer == null) {
      if (this.reader == null) {
        this.writer = writer;
        loadFields(writer.getCommitData());
      } else {
        throw new IllegalStateException("this FieldProperies is read-only (has an IndexReader already)");
      }
    } else {
      throw new IllegalStateException("setIndexWriter was already called");
    }
  }

  private void loadFields(Map<String,String> commitUserData) {
    // nocommit must deserialize current fields from commit data
  }

  public synchronized void setPostingsFormat(String fieldName, String postingsFormat) {
    // Will throw exception if this postingsFormat is unrecognized:
    PostingsFormat.forName(postingsFormat);

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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current;
  }

  public synchronized String getPostingsFormat(String fieldName) {
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.postingsFormat;
  }

  public synchronized void setDocValuesFormat(String fieldName, String docValuesFormat) {
    // Will throw exception if this docValuesFormat is unrecognized:
    DocValuesFormat.forName(docValuesFormat);

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

  synchronized FieldType getField(String fieldName) {
    return fields.get(fieldName);
  }

  // nocommit sugar to getIndexWriter?  and it takes care of linking together this & IW?

  // nocommit but how can we randomized IWC for tests?

  /** Returns a new default {@link IndexWriterConfig}, with {@link Analyzer}, {@link Similarity} and {@link Codec}) pre-set. */
  public IndexWriterConfig getDefaultIndexWriterConfig() {
    IndexWriterConfig iwc = new IndexWriterConfig(indexAnalyzer);
    iwc.setSimilarity(similarity);
    iwc.setCodec(codec);

    return iwc;
  }

  private final Similarity similarity = new PerFieldSimilarityWrapper() {
      @Override
      public Similarity get(String fieldName) {
        // Field must exist:
        FieldType field = getFieldType(fieldName);
        if (field.similarity != null) {
          return field.similarity;
        } else {
          return FieldTypes.this.defaultSimilarity;
        }
      }
    };

  // nocommit but how can we randomized Codec in tests?
  private final Codec codec = new Lucene50Codec() {
      // nocommit: too bad we can't just set every format here?  what if we fix this schema to record which format per field, and then
      // remove PerFieldXXXFormat...?
      @Override
      public PostingsFormat getPostingsFormatForField(String fieldName) {
        // Field must exist:
        FieldType field = getFieldType(fieldName);
        if (field != null) {
          if (field.postingsFormat != null) {
            return PostingsFormat.forName(field.postingsFormat);
          } else if (field.blockTreeMinItemsInBlock != null) {
            assert field.blockTreeMaxItemsInBlock != null;
            // nocommit do we now have cleaner API for this?  Ie "get me default PF, changing these settings"...
            return new Lucene41PostingsFormat(field.blockTreeMinItemsInBlock.intValue(),
                                              field.blockTreeMaxItemsInBlock.intValue());
          }
        }
        return super.getPostingsFormatForField(fieldName); 
      }

      @Override
      public DocValuesFormat getDocValuesFormatForField(String fieldName) {
        // Field must exist:
        FieldType field = getFieldType(fieldName);
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
      // Field must exist:
      FieldType field = getFieldType(fieldName);
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
      // Field must exist:
      FieldType field = getFieldType(fieldName);
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
        // Field must exist:
        FieldType field = getFieldType(fieldName);
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
        // Field must exist:
        FieldType field = getFieldType(fieldName);
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
    return codec;
  }

  /** Returns {@link Analyzer} that returns the per-field analyzer for use during indexing. */
  public Analyzer getIndexAnalyzer() {
    if (writer == null) {
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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.indexAnalyzer;
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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.queryAnalyzer;
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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.similarity;
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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.multiValued == Boolean.TRUE;
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
  // nocommit either rename this, or rename enableStored, or both (they are the same letters just shuffled!)
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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.sortable == Boolean.TRUE;
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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.highlighted == Boolean.TRUE;
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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.indexNorms == Boolean.TRUE;
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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.stored == Boolean.TRUE;
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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
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
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.storeTermVectors == Boolean.TRUE;
  }

  public void enableTermVectorOffsets(String fieldName) {
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    if (current.storeTermVectors != Boolean.TRUE) {
      illegalState(fieldName, "cannot enable termVectorOffsets when termVectors haven't been enabled");
    }
    if (current.storeTermVectorOffsets != Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorOffsets = Boolean.TRUE;
      changed();
    }
  }

  public void disableTermVectorOffsets(String fieldName) {
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    if (current.storeTermVectorOffsets == Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorOffsets = Boolean.FALSE;
      changed();
    }
  }

  public boolean getTermVectorOffsets(String fieldName) {
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.storeTermVectorOffsets == Boolean.TRUE;
  }

  public void enableTermVectorPositions(String fieldName) {
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    if (current.storeTermVectors != Boolean.TRUE) {
      illegalState(fieldName, "cannot enable termVectorPositions when termVectors haven't been enabled");
    }
    if (current.storeTermVectorPositions != Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorPositions = Boolean.TRUE;
      changed();
    }
  }

  public void disableTermVectorPositions(String fieldName) {
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    if (current.storeTermVectorPositions == Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorPositions = Boolean.FALSE;
      changed();
    }
  }

  public boolean getTermVectorPositions(String fieldName) {
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.storeTermVectorPositions == Boolean.TRUE;
  }

  public void enableTermVectorPayloads(String fieldName) {
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    if (current.storeTermVectors != Boolean.TRUE) {
      illegalState(fieldName, "cannot enable termVectorPayloads when termVectors haven't been enabled");
    }
    if (current.storeTermVectorPayloads != Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorPayloads = Boolean.TRUE;
      changed();
    }
  }

  public void disableTermVectorPayloads(String fieldName) {
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    if (current.storeTermVectorPayloads == Boolean.TRUE) {
      // nocommit should this change not be allowed...
      current.storeTermVectorPayloads = Boolean.FALSE;
      changed();
    }
  }

  public boolean getTermVectorPayloads(String fieldName) {
    FieldType current = fields.get(fieldName);
    fieldMustExist(fieldName, current);
    return current.storeTermVectorPayloads == Boolean.TRUE;
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
      fields.put(fieldName, current);
      changed();
    } else if (current.indexOptions == null) {
      boolean success = false;
      try {
        current.indexOptions = indexOptions;
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.indexOptions = null;
        }
      }
      changed();
    } else if (current.indexOptions != indexOptions) {
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
      fields.put(fieldName, current);
      changed();
    } else if (current.docValuesType == null) {
      boolean success = false;
      current.docValuesType = dvType;
      try {
        current.validate();
        success = true;
      } finally {
        if (success == false) {
          current.docValuesType = null;
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

  synchronized void recordLargeTextType(String fieldName, boolean allowStored) {
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
      if (field.sortable == null) {
        field.sortable = Boolean.TRUE;
      }
      if (field.multiValued == null) {
        field.multiValued = Boolean.FALSE;
      }
      if (field.stored == null) {
        field.stored = Boolean.TRUE;
      }
      if (field.sortable == Boolean.TRUE && field.docValuesType == null) {
        if (field.multiValued == Boolean.TRUE) {
          field.docValuesType = DocValuesType.SORTED_NUMERIC;
        } else {
          field.docValuesType = DocValuesType.NUMERIC;
        }
      }
      if (field.indexOptions == null) {
        field.indexOptions = IndexOptions.DOCS_ONLY;
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      if (field.numericPrecisionStep == null) {
        if (field.valueType == ValueType.INT || field.valueType == ValueType.FLOAT) {
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
      if (field.sortable == null) {
        field.sortable = Boolean.TRUE;
      }
      if (field.multiValued == null) {
        field.multiValued = Boolean.FALSE;
      }
      if (field.stored == null) {
        field.stored = Boolean.TRUE;
      }
      if (field.sortable == Boolean.TRUE && field.docValuesType == null) {
        if (field.multiValued == Boolean.TRUE) {
          field.docValuesType = DocValuesType.SORTED_SET;
        } else {
          field.docValuesType = DocValuesType.SORTED;
        }
      }
      if (field.indexOptions == null) {
        if (field.highlighted) {
          field.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        } else {
          field.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        }
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.FALSE;
      }
      break;

    case ATOM:
      if (field.highlighted == null) {
        field.highlighted = Boolean.FALSE;
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
      if (field.indexOptions == null) {
        field.indexOptions = IndexOptions.DOCS_ONLY;
      }
      if (field.sortable == Boolean.TRUE && field.docValuesType == null) {
        if (field.multiValued == Boolean.TRUE) {
          field.docValuesType = DocValuesType.SORTED_SET;
        } else {
          field.docValuesType = DocValuesType.SORTED;
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
      if (field.sortable == null) {
        field.sortable = Boolean.FALSE;
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
      break;

    case TEXT:
      if (field.highlighted == null) {
        field.highlighted = Boolean.TRUE;
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
      if (field.indexOptions == null) {
        if (field.highlighted) {
          field.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        } else {
          field.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        }
      }
      if (field.indexNorms == null) {
        field.indexNorms = Boolean.TRUE;
      }
      break;

    default:
      throw new AssertionError("missing value type in switch");
    }

    assert field.highlighted != null;
    assert field.stored != null;
    assert field.multiValued != null;
    assert field.sortable != null;
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

    // nocommit should we really take Number here?  it's too weakly typed?  you could ask for float range on an int field?  should we
    // instead make separate methods for each atomic type?  or should we "type check" the incoming Number?  taking Number is more
    // conventient for query parsers...?

    switch (fieldType.valueType) {
    case INT:
      return NumericRangeQuery.newIntRange(fieldName,
                                           min == null ? null : min.intValue(),
                                           max == null ? null : max.intValue(),
                                           minInclusive, maxInclusive);
    case FLOAT:
      return NumericRangeQuery.newFloatRange(fieldName,
                                             min == null ? null : min.floatValue(),
                                             max == null ? null : max.floatValue(),
                                             minInclusive, maxInclusive);
    case LONG:
      return NumericRangeQuery.newLongRange(fieldName,
                                            min == null ? null : min.longValue(),
                                            max == null ? null : max.longValue(),
                                            minInclusive, maxInclusive);
    case DOUBLE:
      return NumericRangeQuery.newDoubleRange(fieldName,
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
    assert writer != null;
    // nocommit must serialize current fields to IW's commit data, but this is O(N^2)... hmm
    // nocommit the schema format itself needs to be versioned too
  }

  private synchronized void ensureWritable() {
    if (writer == null) {
      throw new IllegalStateException("FieldProperies is read-only (setIndexWriter was not called)");
    }
  }

  static void illegalState(String fieldName, String message) {
    throw new IllegalStateException("field \"" + fieldName + "\": " + message);
  }

  static void fieldMustExist(String fieldName, FieldType valueType) {
    if (valueType == null) {
      throw new IllegalArgumentException("field \"" + fieldName + "\" is not recognized");
    }
  }
}
