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

package org.apache.solr.schema;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocTermOrdsRangeFilter;
import org.apache.lucene.search.DocTermOrdsRewriteMethod;
import org.apache.lucene.search.DocValuesRangeFilter;
import org.apache.lucene.search.DocValuesRewriteMethod;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.analysis.SolrAnalyzer;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.Sorting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.analysis.util.AbstractAnalysisFactory.LUCENE_MATCH_VERSION_PARAM;

/**
 * Base class for all field types used by an index schema.
 *
 *
 */
public abstract class FieldType extends FieldProperties {
  public static final Logger log = LoggerFactory.getLogger(FieldType.class);

  /**
   * The default poly field separator.
   *
   * @see #createFields(SchemaField, Object, float)
   * @see #isPolyField()
   */
  public static final String POLY_FIELD_SEPARATOR = "___";

  /** The name of the type (not the name of the field) */
  protected String typeName;
  /** additional arguments specified in the field type declaration */
  protected Map<String,String> args;
  /** properties explicitly set to true */
  protected int trueProperties;
  /** properties explicitly set to false */
  protected int falseProperties;
  protected int properties;
  private boolean isExplicitQueryAnalyzer;
  private boolean isExplicitAnalyzer;

  /** Returns true if fields of this type should be tokenized */
  public boolean isTokenized() {
    return (properties & TOKENIZED) != 0;
  }

  /** Returns true if fields can have multiple values */
  public boolean isMultiValued() {
    return (properties & MULTIVALUED) != 0;
  }

  /** Check if a property is set */
  protected boolean hasProperty( int p ) {
    return (properties & p) != 0;
  }

  /**
   * A "polyField" is a FieldType that can produce more than one IndexableField instance for a single value, via the {@link #createFields(org.apache.solr.schema.SchemaField, Object, float)} method.  This is useful
   * when hiding the implementation details of a field from the Solr end user.  For instance, a spatial point may be represented by multiple different fields.
   * @return true if the {@link #createFields(org.apache.solr.schema.SchemaField, Object, float)} method may return more than one field
   */
  public boolean isPolyField(){
    return false;
  }



  /** Returns true if a single field value of this type has multiple logical values
   *  for the purposes of faceting, sorting, etc.  Text fields normally return
   *  true since each token/word is a logical value.
   */
  public boolean multiValuedFieldCache() {
    return isTokenized();
  }

  /** subclasses should initialize themselves with the args provided
   * and remove valid arguments.  leftover arguments will cause an exception.
   * Common boolean properties have already been handled.
   *
   */
  protected void init(IndexSchema schema, Map<String, String> args) {

  }

  // Handle additional arguments...
  protected void setArgs(IndexSchema schema, Map<String,String> args) {
    // default to STORED, INDEXED, OMIT_TF_POSITIONS and MULTIVALUED depending on schema version
    properties = (STORED | INDEXED);
    float schemaVersion = schema.getVersion();
    if (schemaVersion < 1.1f) properties |= MULTIVALUED;
    if (schemaVersion > 1.1f) properties |= OMIT_TF_POSITIONS;
    if (schemaVersion < 1.3) {
      args.remove("compressThreshold");
    }

    this.args = Collections.unmodifiableMap(args);
    Map<String,String> initArgs = new HashMap<>(args);
    initArgs.remove(CLASS_NAME); // consume the class arg 

    trueProperties = FieldProperties.parseProperties(initArgs,true,false);
    falseProperties = FieldProperties.parseProperties(initArgs,false,false);

    properties &= ~falseProperties;
    properties |= trueProperties;

    for (String prop : FieldProperties.propertyNames) initArgs.remove(prop);

    init(schema, initArgs);

    String positionInc = initArgs.get(POSITION_INCREMENT_GAP);
    if (positionInc != null) {
      Analyzer analyzer = getIndexAnalyzer();
      if (analyzer instanceof SolrAnalyzer) {
        ((SolrAnalyzer)analyzer).setPositionIncrementGap(Integer.parseInt(positionInc));
      } else {
        throw new RuntimeException("Can't set " + POSITION_INCREMENT_GAP + " on custom analyzer " + analyzer.getClass());
      }
      analyzer = getQueryAnalyzer();
      if (analyzer instanceof SolrAnalyzer) {
        ((SolrAnalyzer)analyzer).setPositionIncrementGap(Integer.parseInt(positionInc));
      } else {
        throw new RuntimeException("Can't set " + POSITION_INCREMENT_GAP + " on custom analyzer " + analyzer.getClass());
      }
      initArgs.remove(POSITION_INCREMENT_GAP);
    }

    this.postingsFormat = initArgs.remove(POSTINGS_FORMAT);
    this.docValuesFormat = initArgs.remove(DOC_VALUES_FORMAT);

    if (initArgs.size() > 0) {
      throw new RuntimeException("schema fieldtype " + typeName
              + "("+ this.getClass().getName() + ")"
              + " invalid arguments:" + initArgs);
    }
  }

  /** :TODO: document this method */
  protected void restrictProps(int props) {
    if ((properties & props) != 0) {
      throw new RuntimeException("schema fieldtype " + typeName
              + "("+ this.getClass().getName() + ")"
              + " invalid properties:" + propertiesToString(properties & props));
    }
  }

  /** The Name of this FieldType as specified in the schema file */
  public String getTypeName() {
    return typeName;
  }

  void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  @Override
  public String toString() {
    return typeName + "{class=" + this.getClass().getName()
//            + propertiesToString(properties)
            + (indexAnalyzer != null ? ",analyzer=" + indexAnalyzer.getClass().getName() : "")
            + ",args=" + args
            +"}";
  }


  /**
   * Used for adding a document when a field needs to be created from a
   * type and a string.
   *
   * <p>
   * By default, the indexed value is the same as the stored value
   * (taken from toInternal()).   Having a different representation for
   * external, internal, and indexed would present quite a few problems
   * given the current Lucene architecture.  An analyzer for adding docs
   * would need to translate internal->indexed while an analyzer for
   * querying would need to translate external-&gt;indexed.
   * </p>
   * <p>
   * The only other alternative to having internal==indexed would be to have
   * internal==external.   In this case, toInternal should convert to
   * the indexed representation, toExternal() should do nothing, and
   * createField() should *not* call toInternal, but use the external
   * value and set tokenized=true to get Lucene to convert to the
   * internal(indexed) form.
   * </p>
   *
   * :TODO: clean up and clarify this explanation.
   *
   * @see #toInternal
   *
   *
   */
  public StorableField createField(SchemaField field, Object value, float boost) {
    if (!field.indexed() && !field.stored()) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: " + field);
      return null;
    }
    
    String val;
    try {
      val = toInternal(value.toString());
    } catch (RuntimeException e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Error while creating field '" + field + "' from value '" + value + "'", e);
    }
    if (val==null) return null;

    org.apache.lucene.document.FieldType newType = new org.apache.lucene.document.FieldType();
    newType.setTokenized(field.isTokenized());
    newType.setStored(field.stored());
    newType.setOmitNorms(field.omitNorms());
    newType.setIndexOptions(field.indexed() ? getIndexOptions(field, val) : null);
    newType.setStoreTermVectors(field.storeTermVector());
    newType.setStoreTermVectorOffsets(field.storeTermOffsets());
    newType.setStoreTermVectorPositions(field.storeTermPositions());

    return createField(field.getName(), val, newType, boost);
  }

  /**
   * Create the field from native Lucene parts.  Mostly intended for use by FieldTypes outputing multiple
   * Fields per SchemaField
   * @param name The name of the field
   * @param val The _internal_ value to index
   * @param type {@link org.apache.lucene.document.FieldType}
   * @param boost The boost value
   * @return the {@link org.apache.lucene.index.IndexableField}.
   */
  protected StorableField createField(String name, String val, org.apache.lucene.document.FieldType type, float boost){
    Field f = new Field(name, val, type);
    f.setBoost(boost);
    return f;
  }

  /**
   * Given a {@link org.apache.solr.schema.SchemaField}, create one or more {@link org.apache.lucene.index.IndexableField} instances
   * @param field the {@link org.apache.solr.schema.SchemaField}
   * @param value The value to add to the field
   * @param boost The boost to apply
   * @return An array of {@link org.apache.lucene.index.IndexableField}
   *
   * @see #createField(SchemaField, Object, float)
   * @see #isPolyField()
   */
  public List<StorableField> createFields(SchemaField field, Object value, float boost) {
    StorableField f = createField( field, value, boost);
    if (field.hasDocValues() && f.fieldType().docValueType() == null) {
      // field types that support doc values should either override createField
      // to return a field with doc values or extend createFields if this can't
      // be done in a single field instance (see StrField for example)
      throw new UnsupportedOperationException("This field type does not support doc values: " + this);
    }
    return f==null ? Collections.<StorableField>emptyList() : Collections.singletonList(f);
  }

  protected IndexOptions getIndexOptions(SchemaField field, String internalVal) {

    IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    if (field.omitTermFreqAndPositions()) {
      options = IndexOptions.DOCS_ONLY;
    } else if (field.omitPositions()) {
      options = IndexOptions.DOCS_AND_FREQS;
    } else if (field.storeOffsetsWithPositions()) {
      options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
    }

    return options;
  }

  /**
   * Convert an external value (from XML update command or from query string)
   * into the internal format for both storing and indexing (which can be modified by any analyzers).
   * @see #toExternal
   */
  public String toInternal(String val) {
    // - used in delete when a Term needs to be created.
    // - used by the default getTokenizer() and createField()
    return val;
  }

  /**
   * Convert the stored-field format to an external (string, human readable)
   * value
   * @see #toInternal
   */
  public String toExternal(StorableField f) {
    // currently used in writing XML of the search result (but perhaps
    // a more efficient toXML(IndexableField f, Writer w) should be used
    // in the future.
    return f.stringValue();
  }

  /**
   * Convert the stored-field format to an external object.
   * @see #toInternal
   * @since solr 1.3
   */
  public Object toObject(StorableField f) {
    return toExternal(f); // by default use the string
  }

  public Object toObject(SchemaField sf, BytesRef term) {
    final CharsRefBuilder ref = new CharsRefBuilder();
    indexedToReadable(term, ref);
    final StorableField f = createField(sf, ref.toString(), 1.0f);
    return toObject(f);
  }

  /** Given an indexed term, return the human readable representation */
  public String indexedToReadable(String indexedForm) {
    return indexedForm;
  }

  /** Given an indexed term, append the human readable representation*/
  public CharsRef indexedToReadable(BytesRef input, CharsRefBuilder output) {
    output.copyUTF8Bytes(input);
    return output.get();
  }

  /** Given the stored field, return the human readable representation */
  public String storedToReadable(StorableField f) {
    return toExternal(f);
  }

  /** Given the stored field, return the indexed form */
  public String storedToIndexed(StorableField f) {
    // right now, the transformation of single valued fields like SortableInt
    // is done when the Field is created, not at analysis time... this means
    // that the indexed form is the same as the stored field form.
    return f.stringValue();
  }

  /** Given the readable value, return the term value that will match it. */
  public String readableToIndexed(String val) {
    return toInternal(val);
  }

  /** Given the readable value, return the term value that will match it. */
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    final String internal = readableToIndexed(val.toString());
    result.copyChars(internal);
  }

  public void setIsExplicitQueryAnalyzer(boolean isExplicitQueryAnalyzer) {
    this.isExplicitQueryAnalyzer = isExplicitQueryAnalyzer;
  }

  public boolean isExplicitQueryAnalyzer() {
    return isExplicitQueryAnalyzer;
  }

  public void setIsExplicitAnalyzer(boolean explicitAnalyzer) {
    isExplicitAnalyzer = explicitAnalyzer;
  }

  public boolean isExplicitAnalyzer() {
    return isExplicitAnalyzer;
  }

  /**
   * @return the string used to specify the concrete class name in a serialized representation: the class arg.  
   *         If the concrete class name was not specified via a class arg, returns {@code getClass().getName()}.
   */
  public String getClassArg() {
    if (null != args) {
      String className = args.get(CLASS_NAME);
      if (null != className) {
        return className;
      }
    }
    return getClass().getName();
  }


  /**
   * Returns a Query instance for doing prefix searches on this field type.
   * Also, other QueryParser implementations may have different semantics.
   * <p/>
   * Sub-classes should override this method to provide their own range query implementation.
   *
   * @param parser       the {@link org.apache.solr.search.QParser} calling the method
   * @param sf           the schema field
   * @param termStr      the term string for prefix query
   * @return a Query instance to perform prefix search
   *
   */
  public Query getPrefixQuery(QParser parser, SchemaField sf, String termStr) {
    PrefixQuery query = new PrefixQuery(new Term(sf.getName(), termStr));
    query.setRewriteMethod(sf.getType().getRewriteMethod(parser, sf));
    return query;
  }
  
  /**
   * DocValues is not enabled for a field, but its indexed, docvalues can be constructed 
   * on the fly (uninverted, aka fieldcache) on the first request to sort, facet, etc. 
   * This specifies the structure to use.
   * 
   * @param sf field instance
   * @return type to uninvert, or {@code null} (to disallow uninversion for the field)
   */
  public abstract UninvertingReader.Type getUninversionType(SchemaField sf);

  /**
   * Default analyzer for types that only produce 1 verbatim token...
   * A maximum size of chars to be read must be specified
   */
  protected final class DefaultAnalyzer extends SolrAnalyzer {
    final int maxChars;

    DefaultAnalyzer(int maxChars) {
      this.maxChars=maxChars;
    }

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer ts = new Tokenizer() {
        final char[] cbuf = new char[maxChars];
        final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
        @Override
        public boolean incrementToken() throws IOException {
          clearAttributes();
          int n = input.read(cbuf,0,maxChars);
          if (n<=0) return false;
          String s = toInternal(new String(cbuf,0,n));
          termAtt.setEmpty().append(s);
          offsetAtt.setOffset(correctOffset(0),correctOffset(n));
          return true;
        }
      };

      return new TokenStreamComponents(ts);
    }
  }

  private Analyzer indexAnalyzer = new DefaultAnalyzer(256);

  private Analyzer queryAnalyzer = indexAnalyzer;

  /**
   * Returns the Analyzer to be used when indexing fields of this type.
   * <p>
   * This method may be called many times, at any time.
   * </p>
   * @see #getQueryAnalyzer
   */
  public Analyzer getIndexAnalyzer() {
    return indexAnalyzer;
  }

  /**
   * Returns the Analyzer to be used when searching fields of this type.
   * <p>
   * This method may be called many times, at any time.
   * </p>
   * @see #getIndexAnalyzer
   */
  public Analyzer getQueryAnalyzer() {
    return queryAnalyzer;
  }

  /**
   * Returns true if this type supports index and query analyzers, false otherwise.
   */
  protected boolean supportsAnalyzers() {
    return false;
  }


  /**
   * Sets the Analyzer to be used when indexing fields of this type.
   *
   * <p>
   * Subclasses should override {@link #supportsAnalyzers()} to
   * enable this function.
   * </p>
   *
   * @see #supportsAnalyzers()
   * @see #setQueryAnalyzer
   * @see #getIndexAnalyzer
   */
  public final void setIndexAnalyzer(Analyzer analyzer) {
    if (supportsAnalyzers()) {
      indexAnalyzer = analyzer;
    } else {
      throw new SolrException
        (ErrorCode.SERVER_ERROR,
         "FieldType: " + this.getClass().getSimpleName() +
         " (" + typeName + ") does not support specifying an analyzer");
    }
  }

  /**
   * Sets the Analyzer to be used when querying fields of this type.
   *
   * <p>
   * Subclasses should override {@link #supportsAnalyzers()} to
   * enable this function.
   * </p>
   *
   * @see #supportsAnalyzers()
   * @see #setIndexAnalyzer
   * @see #getQueryAnalyzer
   */
  public final void setQueryAnalyzer(Analyzer analyzer) {
    if (supportsAnalyzers()) {
      queryAnalyzer = analyzer;
    } else {
      throw new SolrException
        (ErrorCode.SERVER_ERROR,
         "FieldType: " + this.getClass().getSimpleName() +
         " (" + typeName + ") does not support specifying an analyzer");
    }
  }

  /** @lucene.internal */
  protected SimilarityFactory similarityFactory;

  /** @lucene.internal */
  protected Similarity similarity;

  /**
   * Gets the Similarity used when scoring fields of this type
   * 
   * <p>
   * The default implementation returns null, which means this type
   * has no custom similarity associated with it.
   * </p>
   * 
   * @lucene.internal
   */
  public Similarity getSimilarity() {
    return similarity;
  }

  /**
   * Gets the factory for the Similarity used when scoring fields of this type
   *
   * <p>
   * The default implementation returns null, which means this type
   * has no custom similarity factory associated with it.
   * </p>
   *
   * @lucene.internal
   */
  public SimilarityFactory getSimilarityFactory() {
    return similarityFactory;
  }


  /** Return the numeric type of this field, or null if this field is not a
   *  numeric field. */
  public org.apache.lucene.document.FieldType.NumericType getNumericType() {
    return null;
  }

  /**
   * Sets the Similarity used when scoring fields of this type
   * @lucene.internal
   */
  public void setSimilarity(SimilarityFactory similarityFactory) {
    this.similarityFactory = similarityFactory;
    this.similarity = similarityFactory.getSimilarity();
  }
  
  /**
   * The postings format used for this field type
   */
  protected String postingsFormat;
  
  public String getPostingsFormat() {
    return postingsFormat;
  }

  /**
   * The docvalues format used for this field type
   */
  protected String docValuesFormat;

  public final String getDocValuesFormat() {
    return docValuesFormat;
  }

  /**
   * calls back to TextResponseWriter to write the field value
   */
  public abstract void write(TextResponseWriter writer, String name, StorableField f) throws IOException;


  /**
   * Returns the SortField instance that should be used to sort fields
   * of this type.
   * @see SchemaField#checkSortability
   */
  public abstract SortField getSortField(SchemaField field, boolean top);

  /**
   * Utility usable by subclasses when they want to get basic String sorting
   * using common checks.
   * @see SchemaField#checkSortability
   */
  protected SortField getStringSort(SchemaField field, boolean reverse) {
    field.checkSortability();
    return Sorting.getStringSortField(field.name, reverse, field.sortMissingLast(),field.sortMissingFirst());
  }

  /** called to get the default value source (normally, from the
   *  Lucene FieldCache.)
   */
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    field.checkFieldCacheSource(parser);
    return new StrFieldSource(field.name);
  }

  /**
   * Returns a Query instance for doing range searches on this field type. {@link org.apache.solr.search.SolrQueryParser}
   * currently passes part1 and part2 as null if they are '*' respectively. minInclusive and maxInclusive are both true
   * currently by SolrQueryParser but that may change in the future. Also, other QueryParser implementations may have
   * different semantics.
   * <p/>
   * Sub-classes should override this method to provide their own range query implementation. They should strive to
   * handle nulls in part1 and/or part2 as well as unequal minInclusive and maxInclusive parameters gracefully.
   *
   * @param parser       the {@link org.apache.solr.search.QParser} calling the method
   * @param field        the schema field
   * @param part1        the lower boundary of the range, nulls are allowed.
   * @param part2        the upper boundary of the range, nulls are allowed
   * @param minInclusive whether the minimum of the range is inclusive or not
   * @param maxInclusive whether the maximum of the range is inclusive or not
   *  @return a Query instance to perform range search according to given parameters
   *
   */
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    // TODO: change these all to use readableToIndexed/bytes instead (e.g. for unicode collation)
    if (field.hasDocValues() && !field.indexed()) {
      if (field.multiValued()) {
        return new ConstantScoreQuery(DocTermOrdsRangeFilter.newBytesRefRange(
            field.getName(),
            part1 == null ? null : new BytesRef(toInternal(part1)),
            part2 == null ? null : new BytesRef(toInternal(part2)),
            minInclusive, maxInclusive));
      } else {
        return new ConstantScoreQuery(DocValuesRangeFilter.newStringRange(
            field.getName(), 
            part1 == null ? null : toInternal(part1),
            part2 == null ? null : toInternal(part2),
            minInclusive, maxInclusive));
      }
    } else {
      MultiTermQuery rangeQuery = TermRangeQuery.newStringRange(
            field.getName(),
            part1 == null ? null : toInternal(part1),
            part2 == null ? null : toInternal(part2),
            minInclusive, maxInclusive);
      rangeQuery.setRewriteMethod(getRewriteMethod(parser, field));
      return rangeQuery;
    }
  }

  /**
   * Returns a Query instance for doing searches against a field.
   * @param parser The {@link org.apache.solr.search.QParser} calling the method
   * @param field The {@link org.apache.solr.schema.SchemaField} of the field to search
   * @param externalVal The String representation of the value to search
   * @return The {@link org.apache.lucene.search.Query} instance.  This implementation returns a {@link org.apache.lucene.search.TermQuery} but overriding queries may not
   * 
   */
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    BytesRefBuilder br = new BytesRefBuilder();
    readableToIndexed(externalVal, br);
    if (field.hasDocValues() && !field.indexed()) {
      // match-only
      return getRangeQuery(parser, field, externalVal, externalVal, true, true);
    } else {
      return new TermQuery(new Term(field.getName(), br.toBytesRef()));
    }
  }
  
  /**
   * Expert: Returns the rewrite method for multiterm queries such as wildcards.
   * @param parser The {@link org.apache.solr.search.QParser} calling the method
   * @param field The {@link org.apache.solr.schema.SchemaField} of the field to search
   * @return A suitable rewrite method for rewriting multi-term queries to primitive queries.
   */
  public MultiTermQuery.RewriteMethod getRewriteMethod(QParser parser, SchemaField field) {
    if (!field.indexed() && field.hasDocValues()) {
      return field.multiValued() ? new DocTermOrdsRewriteMethod() : new DocValuesRewriteMethod();
    } else {
      return MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE;
    }
  }

  /**
   * Check's {@link org.apache.solr.schema.SchemaField} instances constructed 
   * using this field type to ensure that they are valid.
   *
   * <p>
   * This method is called by the <code>SchemaField</code> constructor to 
   * check that it's initialization does not violate any fundemental 
   * requirements of the <code>FieldType</code>.  The default implementation 
   * does nothing, but subclasses may chose to throw a {@link SolrException}  
   * if invariants are violated by the <code>SchemaField.</code>
   * </p>
   */
  public void checkSchemaField(final SchemaField field) {
    // override if your field type supports doc values
    if (field.hasDocValues()) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Field type " + this + " does not support doc values");
    }
  }

  public static final String TYPE = "type";
  public static final String TYPE_NAME = "name";
  public static final String CLASS_NAME = "class";
  public static final String ANALYZER = "analyzer";
  public static final String INDEX = "index";
  public static final String INDEX_ANALYZER = "indexAnalyzer";
  public static final String QUERY = "query";
  public static final String QUERY_ANALYZER = "queryAnalyzer";
  public static final String MULTI_TERM = "multiterm";
  public static final String MULTI_TERM_ANALYZER = "multiTermAnalyzer";
  public static final String SIMILARITY = "similarity";
  public static final String CHAR_FILTER = "charFilter";
  public static final String CHAR_FILTERS = "charFilters";
  public static final String TOKENIZER = "tokenizer";
  public static final String FILTER = "filter";
  public static final String FILTERS = "filters";

  private static final String POSTINGS_FORMAT = "postingsFormat";
  private static final String DOC_VALUES_FORMAT = "docValuesFormat";
  private static final String AUTO_GENERATE_PHRASE_QUERIES = "autoGeneratePhraseQueries";
  private static final String ARGS = "args";
  private static final String POSITION_INCREMENT_GAP = "positionIncrementGap";

  /**
   * Get a map of property name -> value for this field type. 
   * @param showDefaults if true, include default properties.
   */
  public SimpleOrderedMap<Object> getNamedPropertyValues(boolean showDefaults) {
    SimpleOrderedMap<Object> namedPropertyValues = new SimpleOrderedMap<>();
    namedPropertyValues.add(TYPE_NAME, getTypeName());
    namedPropertyValues.add(CLASS_NAME, getClassArg());
    if (showDefaults) {
      Map<String,String> fieldTypeArgs = getNonFieldPropertyArgs();
      if (null != fieldTypeArgs) {
        for (String key : fieldTypeArgs.keySet()) {
        if ( ! CLASS_NAME.equals(key) && ! TYPE_NAME.equals(key)) {
            namedPropertyValues.add(key, fieldTypeArgs.get(key));
          }
        }
      }
      if (this instanceof TextField) {
        namedPropertyValues.add(AUTO_GENERATE_PHRASE_QUERIES, ((TextField) this).getAutoGeneratePhraseQueries());
      }
      namedPropertyValues.add(getPropertyName(INDEXED), hasProperty(INDEXED));
      namedPropertyValues.add(getPropertyName(STORED), hasProperty(STORED));
      namedPropertyValues.add(getPropertyName(DOC_VALUES), hasProperty(DOC_VALUES));
      namedPropertyValues.add(getPropertyName(STORE_TERMVECTORS), hasProperty(STORE_TERMVECTORS));
      namedPropertyValues.add(getPropertyName(STORE_TERMPOSITIONS), hasProperty(STORE_TERMPOSITIONS));
      namedPropertyValues.add(getPropertyName(STORE_TERMOFFSETS), hasProperty(STORE_TERMOFFSETS));
      namedPropertyValues.add(getPropertyName(OMIT_NORMS), hasProperty(OMIT_NORMS));
      namedPropertyValues.add(getPropertyName(OMIT_TF_POSITIONS), hasProperty(OMIT_TF_POSITIONS));
      namedPropertyValues.add(getPropertyName(OMIT_POSITIONS), hasProperty(OMIT_POSITIONS));
      namedPropertyValues.add(getPropertyName(STORE_OFFSETS), hasProperty(STORE_OFFSETS));
      namedPropertyValues.add(getPropertyName(MULTIVALUED), hasProperty(MULTIVALUED));
      if (hasProperty(SORT_MISSING_FIRST)) {
        namedPropertyValues.add(getPropertyName(SORT_MISSING_FIRST), true);
      } else if (hasProperty(SORT_MISSING_LAST)) {
        namedPropertyValues.add(getPropertyName(SORT_MISSING_LAST), true);
      }
      namedPropertyValues.add(getPropertyName(TOKENIZED), isTokenized());
      // The BINARY property is always false
      // namedPropertyValues.add(getPropertyName(BINARY), hasProperty(BINARY));
      if (null != getSimilarityFactory()) {
        namedPropertyValues.add(SIMILARITY, getSimilarityFactory().getNamedPropertyValues());
      }
      if (null != getPostingsFormat()) {
        namedPropertyValues.add(POSTINGS_FORMAT, getPostingsFormat());
      }
      if (null != getDocValuesFormat()) {
        namedPropertyValues.add(DOC_VALUES_FORMAT, getDocValuesFormat());
      }
    } else { // Don't show defaults
      Set<String> fieldProperties = new HashSet<>();
      for (String propertyName : FieldProperties.propertyNames) {
        fieldProperties.add(propertyName);
      }

      for (String key : args.keySet()) {
        if (fieldProperties.contains(key)) {
          namedPropertyValues.add(key, StrUtils.parseBool(args.get(key)));
        } else if (!CLASS_NAME.equals(key) && !TYPE_NAME.equals(key)) {
          namedPropertyValues.add(key, args.get(key));
        }
      }
    }
    
    if (isExplicitAnalyzer()) {
      String analyzerProperty = isExplicitQueryAnalyzer() ? INDEX_ANALYZER : ANALYZER;
      namedPropertyValues.add(analyzerProperty, getAnalyzerProperties(getIndexAnalyzer()));
    } 
    if (isExplicitQueryAnalyzer()) {
      String analyzerProperty = isExplicitAnalyzer() ? QUERY_ANALYZER : ANALYZER;
      namedPropertyValues.add(analyzerProperty, getAnalyzerProperties(getQueryAnalyzer()));
    }
    if (this instanceof TextField) {
      if (((TextField)this).isExplicitMultiTermAnalyzer()) {
        namedPropertyValues.add(MULTI_TERM_ANALYZER, getAnalyzerProperties(((TextField) this).getMultiTermAnalyzer()));
      }
    }

    return namedPropertyValues;
  }

  /** Returns args to this field type that aren't standard field properties */
  protected Map<String,String> getNonFieldPropertyArgs() {
    Map<String,String> initArgs =  new HashMap<>(args);
    for (String prop : FieldProperties.propertyNames) {
      initArgs.remove(prop);
    }
    return initArgs;
  }

  /** 
   * Returns a description of the given analyzer, by either reporting the Analyzer name
   * if it's not a TokenizerChain, or if it is, querying each analysis factory for its
   * name and args.
   */
  protected static SimpleOrderedMap<Object> getAnalyzerProperties(Analyzer analyzer) {
    SimpleOrderedMap<Object> analyzerProps = new SimpleOrderedMap<>();

    if (analyzer instanceof TokenizerChain) {
      Map<String,String> factoryArgs;
      TokenizerChain tokenizerChain = (TokenizerChain)analyzer;
      CharFilterFactory[] charFilterFactories = tokenizerChain.getCharFilterFactories();
      if (null != charFilterFactories && charFilterFactories.length > 0) {
        List<SimpleOrderedMap<Object>> charFilterProps = new ArrayList<>();
        for (CharFilterFactory charFilterFactory : charFilterFactories) {
          SimpleOrderedMap<Object> props = new SimpleOrderedMap<>();
          props.add(CLASS_NAME, charFilterFactory.getClassArg());
          factoryArgs = charFilterFactory.getOriginalArgs();
          if (null != factoryArgs) {
            for (String key : factoryArgs.keySet()) {
              if ( ! CLASS_NAME.equals(key)) {
                if (LUCENE_MATCH_VERSION_PARAM.equals(key)) {
                  if (charFilterFactory.isExplicitLuceneMatchVersion()) {
                    props.add(key, factoryArgs.get(key));
                  }
                } else {
                   props.add(key, factoryArgs.get(key));
                }
              }
            }
          }
          charFilterProps.add(props);
        }
        analyzerProps.add(CHAR_FILTERS, charFilterProps);
      }

      SimpleOrderedMap<Object> tokenizerProps = new SimpleOrderedMap<>();
      TokenizerFactory tokenizerFactory = tokenizerChain.getTokenizerFactory();
      tokenizerProps.add(CLASS_NAME, tokenizerFactory.getClassArg());
      factoryArgs = tokenizerFactory.getOriginalArgs();
      if (null != factoryArgs) {
        for (String key : factoryArgs.keySet()) {
          if ( ! CLASS_NAME.equals(key)) {
            if (LUCENE_MATCH_VERSION_PARAM.equals(key)) {
              if (tokenizerFactory.isExplicitLuceneMatchVersion()) {
                tokenizerProps.add(key, factoryArgs.get(key));
              }
            } else {
              tokenizerProps.add(key, factoryArgs.get(key));
            }
          }
        }
      }
      analyzerProps.add(TOKENIZER, tokenizerProps);

      TokenFilterFactory[] filterFactories = tokenizerChain.getTokenFilterFactories();
      if (null != filterFactories && filterFactories.length > 0) {
        List<SimpleOrderedMap<Object>> filterProps = new ArrayList<>();
        for (TokenFilterFactory filterFactory : filterFactories) {
          SimpleOrderedMap<Object> props = new SimpleOrderedMap<>();
          props.add(CLASS_NAME, filterFactory.getClassArg());
          factoryArgs = filterFactory.getOriginalArgs();
          if (null != factoryArgs) {
            for (String key : factoryArgs.keySet()) {
              if ( ! CLASS_NAME.equals(key)) {
                if (LUCENE_MATCH_VERSION_PARAM.equals(key)) {
                  if (filterFactory.isExplicitLuceneMatchVersion()) {
                    props.add(key, factoryArgs.get(key));
                  }
                } else {
                  props.add(key, factoryArgs.get(key));
                }
              }
            }
          }
          filterProps.add(props);
        }
        analyzerProps.add(FILTERS, filterProps);
      }
    } else { // analyzer is not instanceof TokenizerChain
      analyzerProps.add(CLASS_NAME, analyzer.getClass().getName());
    }
    return analyzerProps;
  }

  /**Converts any Object to a java Object native to this field type
   */
  public Object toNativeType(Object val) {
    return val;
  }
  
  /** 
   * Convert a value used by the FieldComparator for this FieldType's SortField
   * into a marshalable value for distributed sorting.
   */
  public Object marshalSortValue(Object value) {
    return value;
  }
  
  /**
   * Convert a value marshaled via {@link #marshalSortValue} back 
   * into a value usable by the FieldComparator for this FieldType's SortField
   */
  public Object unmarshalSortValue(Object value) {
    return value;
  }

  /**
   * Marshals a string-based field value.
   */
  protected static Object marshalStringSortValue(Object value) {
    if (null == value) {
      return null;
    }
    CharsRefBuilder spare = new CharsRefBuilder();
    spare.copyUTF8Bytes((BytesRef)value);
    return spare.toString();
  }

  /**
   * Unmarshals a string-based field value.
   */
  protected static Object unmarshalStringSortValue(Object value) {
    if (null == value) {
      return null;
    }
    BytesRefBuilder spare = new BytesRefBuilder();
    String stringVal = (String)value;
    spare.copyChars(stringVal);
    return spare.get();
  }

  /**
   * Marshals a binary field value.
   */
  protected static Object marshalBase64SortValue(Object value) {
    if (null == value) {
      return null;
    }
    final BytesRef val = (BytesRef)value;
    return Base64.byteArrayToBase64(val.bytes, val.offset, val.length);
  }

  /**
   * Unmarshals a binary field value.
   */
  protected static Object unmarshalBase64SortValue(Object value) {
    if (null == value) {
      return null;
    }
    final String val = (String)value;
    final byte[] bytes = Base64.base64ToByteArray(val);
    return new BytesRef(bytes);
  }
}
