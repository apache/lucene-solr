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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.DocValuesRewriteMethod;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.Version;
import org.apache.solr.analysis.SolrAnalyzer;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.query.SolrRangeQuery;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.uninverting.UninvertingReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.analysis.util.AbstractAnalysisFactory.LUCENE_MATCH_VERSION_PARAM;

/**
 * Base class for all field types used by an index schema.
 *
 * @since 3.1
 */
public abstract class FieldType extends FieldProperties {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The default poly field separator.
   *
   * @see #createFields(SchemaField, Object)
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
   * A "polyField" is a FieldType that can produce more than one IndexableField instance for a single value, via the {@link #createFields(org.apache.solr.schema.SchemaField, Object)} method.  This is useful
   * when hiding the implementation details of a field from the Solr end user.  For instance, a spatial point may be represented by multiple different fields.
   * @return true if the {@link #createFields(org.apache.solr.schema.SchemaField, Object)} method may return more than one field
   */
  public boolean isPolyField(){
    return false;
  }
  
  public boolean isPointField() {
    return false;
  }

  public boolean isUtf8Field(){return false;}

  /**
   * Returns true if the fields' docValues should be used for obtaining stored value
   */
  public boolean useDocValuesAsStored() {
    return (properties & USE_DOCVALUES_AS_STORED) != 0;
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

  public boolean write(IteratorWriter.ItemWriter itemWriter) {
    return false;
  }

  /**
   * Initializes the field type.  Subclasses should usually override {@link #init(IndexSchema, Map)}
   * which is called by this method.
   */
  protected void setArgs(IndexSchema schema, Map<String,String> args) {
    // default to STORED, INDEXED, OMIT_TF_POSITIONS and MULTIVALUED depending on schema version
    properties = (STORED | INDEXED);
    float schemaVersion = schema.getVersion();
    if (schemaVersion < 1.1f) properties |= MULTIVALUED;
    if (schemaVersion > 1.1f) properties |= OMIT_TF_POSITIONS;
    if (schemaVersion < 1.3) {
      args.remove("compressThreshold");
    }
    if (schemaVersion >= 1.6f) properties |= USE_DOCVALUES_AS_STORED;
    
    properties |= UNINVERTIBLE;
    
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
   * would need to translate internal-&gt;indexed while an analyzer for
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
  public IndexableField createField(SchemaField field, Object value) {
    if (!field.indexed() && !field.stored()) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: {}", field);
      return null;
    }
    
    String val;
    try {
      val = toInternal(value.toString());
    } catch (RuntimeException e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Error while creating field '" + field + "' from value '" + value + "'", e);
    }
    if (val==null) return null;

      /*org.apache.lucene.document.FieldType newType = new org.apache.lucene.document.FieldType();
      newType.setTokenized(field.isTokenized());
      newType.setStored(field.stored());
      newType.setOmitNorms(field.omitNorms());
      newType.setIndexOptions(field.indexed() ? getIndexOptions(field, val) : IndexOptions.NONE);
      newType.setStoreTermVectors(field.storeTermVector());
      newType.setStoreTermVectorOffsets(field.storeTermOffsets());
      newType.setStoreTermVectorPositions(field.storeTermPositions());
      newType.setStoreTermVectorPayloads(field.storeTermPayloads());*/
    return createField(field.getName(), val, field);
  }

  /**
   * Create the field from native Lucene parts.  Mostly intended for use by FieldTypes outputing multiple
   * Fields per SchemaField
   * @param name The name of the field
   * @param val The _internal_ value to index
   * @param type {@link org.apache.lucene.document.FieldType}
   * @return the {@link org.apache.lucene.index.IndexableField}.
   */
  protected IndexableField createField(String name, String val, org.apache.lucene.index.IndexableFieldType type){
    return new Field(name, val, type);
  }

  /**
   * Given a {@link org.apache.solr.schema.SchemaField}, create one or more {@link org.apache.lucene.index.IndexableField} instances
   * @param field the {@link org.apache.solr.schema.SchemaField}
   * @param value The value to add to the field
   * @return An array of {@link org.apache.lucene.index.IndexableField}
   *
   * @see #createField(SchemaField, Object)
   * @see #isPolyField()
   */
  public List<IndexableField> createFields(SchemaField field, Object value) {
    IndexableField f = createField( field, value);
    if (field.hasDocValues() && f.fieldType().docValuesType() == null) {
      // field types that support doc values should either override createField
      // to return a field with doc values or extend createFields if this can't
      // be done in a single field instance (see StrField for example)
      throw new UnsupportedOperationException("This field type does not support doc values: " + this);
    }
    return f==null ? Collections.<IndexableField>emptyList() : Collections.singletonList(f);
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
  public String toExternal(IndexableField f) {
    // currently used in writing XML of the search result (but perhaps
    // a more efficient toXML(IndexableField f, Writer w) should be used
    // in the future.
    String val = f.stringValue();
    if (val == null) {
      // docValues will use the binary value
      val = f.binaryValue().utf8ToString();
    }
    return val;
  }

  /**
   * Convert the stored-field format to an external object.
   * @see #toInternal
   * @since solr 1.3
   */
  public Object toObject(IndexableField f) {
    return toExternal(f); // by default use the string
  }

  public Object toObject(SchemaField sf, BytesRef term) {
    final CharsRefBuilder ref = new CharsRefBuilder();
    indexedToReadable(term, ref);
    final IndexableField f = createField(sf, ref.toString());
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
  public String storedToReadable(IndexableField f) {
    return toExternal(f);
  }

  /** Given the stored field, return the indexed form */
  public String storedToIndexed(IndexableField f) {
    // right now, the transformation of single valued fields like SortableInt
    // is done when the Field is created, not at analysis time... this means
    // that the indexed form is the same as the stored field form.
    return f.stringValue();
  }

  /** Given the readable value, return the term value that will match it. */
  public String readableToIndexed(String val) {
    return toInternal(val);
  }

  /** Given the readable value, return the term value that will match it.
   * This method will modify the size and length of the {@code result} 
   * parameter and write from offset 0
   */
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
   * <p>
   * Sub-classes should override this method to provide their own range query implementation.
   *
   * @param parser       the {@link org.apache.solr.search.QParser} calling the method
   * @param sf           the schema field
   * @param termStr      the term string for prefix query, if blank then this query should match all docs with this field
   * @return a Query instance to perform prefix search
   */
  public Query getPrefixQuery(QParser parser, SchemaField sf, String termStr) {
    if ("".equals(termStr)) {
      return getExistenceQuery(parser, sf);
    }
    PrefixQuery query = new PrefixQuery(new Term(sf.getName(), termStr));
    query.setRewriteMethod(sf.getType().getRewriteMethod(parser, sf));
    return query;
  }
  
  /**
   * <p>
   * If DocValues is not enabled for a field, but it's indexed, docvalues can be constructed 
   * on the fly (uninverted, aka fieldcache) on the first request to sort, facet, etc. 
   * This specifies the structure to use.
   * </p>
   * <p>
   * This method will not be used if the field is (effectively) <code>uninvertible="false"</code>
   * </p>
   * 
   * @param sf field instance
   * @return type to uninvert, or {@code null} (to disallow uninversion for the field)
   * @see SchemaField#isUninvertible()
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
        final BytesTermAttribute bytesAtt = isPointField() ? addAttribute(BytesTermAttribute.class) : null;
        final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
        @Override
        public boolean incrementToken() throws IOException {
          clearAttributes();
          int n = input.read(cbuf,0,maxChars);
          if (n<=0) return false;
          if (isPointField()) {
            BytesRef b = ((PointField)FieldType.this).toInternalByteRef(new String(cbuf, 0, n));
            bytesAtt.setBytesRef(b);
          } else {
            String s = toInternal(new String(cbuf, 0, n));
            termAtt.setEmpty().append(s);
          }
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

  /**
   * Return the numeric type of this field, or null if this field is not a
   * numeric field. 
   */
  public NumberType getNumberType() {
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
   * <p>
   * Sub-classes should prefer using {@link #toExternal(IndexableField)} or {@link #toObject(IndexableField)}
   * to get the writeable external value of <code>f</code> instead of directly using <code>f.stringValue()</code> or <code>f.binaryValue()</code>
   */
  public abstract void write(TextResponseWriter writer, String name, IndexableField f) throws IOException;


  /**
   * Returns the SortField instance that should be used to sort fields
   * of this type.
   * @see SchemaField#checkSortability
   * @see #getStringSort
   * @see #getNumericSort
   */
  public abstract SortField getSortField(SchemaField field, boolean top);

  /**
   * <p>A Helper utility method for use by subclasses.</p>
   * <p>This method deals with:</p>
   * <ul>
   *  <li>{@link SchemaField#checkSortability}</li>
   *  <li>Creating a {@link SortField} on <code>field</code> with the specified 
   *      <code>reverse</code> &amp; <code>sortType</code></li>
   *  <li>Setting the {@link SortField#setMissingValue} to <code>missingLow</code> or <code>missingHigh</code>
   *      as appropriate based on the value of <code>reverse</code> and the 
   *      <code>sortMissingFirst</code> &amp; <code>sortMissingLast</code> properties of the 
   *      <code>field</code></li>
   * </ul>
   *
   * @param field The SchemaField to sort on.  May use <code>sortMissingFirst</code> or <code>sortMissingLast</code> or neither.
   * @param sortType The sort Type of the underlying values in the <code>field</code>
   * @param reverse True if natural order of the <code>sortType</code> should be reversed
   * @param missingLow The <code>missingValue</code> to be used if the other params indicate that docs w/o values should sort as "low" as possible.
   * @param missingHigh The <code>missingValue</code> to be used if the other params indicate that docs w/o values should sort as "high" as possible.
   * @see #getSortedSetSortField
   */
  protected static SortField getSortField(SchemaField field, SortField.Type sortType, boolean reverse,
                                          Object missingLow, Object missingHigh) {
    field.checkSortability();

    SortField sf = new SortField(field.getName(), sortType, reverse);
    applySetMissingValue(field, sf, missingLow, missingHigh);
    
    return sf;
  }

  /**
   * Same as {@link #getSortField} but using {@link SortedSetSortField}
   */
  protected static SortField getSortedSetSortField(SchemaField field, SortedSetSelector.Type selector,
                                                   boolean reverse, Object missingLow, Object missingHigh) {
                                                   
    field.checkSortability();
    SortField sf = new SortedSetSortField(field.getName(), reverse, selector);
    applySetMissingValue(field, sf, missingLow, missingHigh);
    
    return sf;
  }
  
  /**
   * Same as {@link #getSortField} but using {@link SortedNumericSortField}.
   */
  protected static SortField getSortedNumericSortField(SchemaField field, SortField.Type sortType,
                                                       SortedNumericSelector.Type selector,
                                                       boolean reverse, Object missingLow, Object missingHigh) {
                                                   
    field.checkSortability();
    SortField sf = new SortedNumericSortField(field.getName(), sortType, reverse, selector);
    applySetMissingValue(field, sf, missingLow, missingHigh);
    
    return sf;
  }
  
  /** 
   * @see #getSortField 
   * @see #getSortedSetSortField 
   */
  private static void applySetMissingValue(SchemaField field, SortField sortField, 
                                           Object missingLow, Object missingHigh) {
    final boolean reverse = sortField.getReverse();
    
    if (field.sortMissingLast()) {
      sortField.setMissingValue(reverse ? missingLow : missingHigh);
    } else if (field.sortMissingFirst()) {
      sortField.setMissingValue(reverse ? missingHigh : missingLow);
    }
  }

  /**
   * Utility usable by subclasses when they want to get basic String sorting
   * using common checks.
   * @see SchemaField#checkSortability
   * @see #getSortedSetSortField
   * @see #getSortField
   */
  protected SortField getStringSort(SchemaField field, boolean reverse) {
    if (field.multiValued()) {
      MultiValueSelector selector = field.type.getDefaultMultiValueSelectorForSort(field, reverse);
      if (null != selector) {
        return getSortedSetSortField(field, selector.getSortedSetSelectorType(),
                                     reverse, SortField.STRING_FIRST, SortField.STRING_LAST);
      }
    }
    
    // else...
    // either single valued, or don't support implicit multi selector
    // (in which case let getSortField() give the error)
    return getSortField(field, SortField.Type.STRING, reverse, SortField.STRING_FIRST, SortField.STRING_LAST);
  }

  /**
   * Utility usable by subclasses when they want to get basic Numeric sorting
   * using common checks.
   *
   * @see SchemaField#checkSortability
   * @see #getSortedNumericSortField
   * @see #getSortField
   */
  protected SortField getNumericSort(SchemaField field, NumberType type, boolean reverse) {
    if (field.multiValued()) {
      MultiValueSelector selector = field.type.getDefaultMultiValueSelectorForSort(field, reverse);
      if (null != selector) {
        return getSortedNumericSortField(field, type.sortType, selector.getSortedNumericSelectorType(),
                                         reverse, type.sortMissingLow, type.sortMissingHigh);
      }
    }
    
    // else...
    // either single valued, or don't support implicit multi selector
    // (in which case let getSortField() give the error)
    return getSortField(field, type.sortType, reverse, type.sortMissingLow, type.sortMissingHigh);
  }

  

  /** called to get the default value source (normally, from the
   *  Lucene FieldCache.)
   */
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    field.checkFieldCacheSource();
    return new StrFieldSource(field.name);
  }

  /**
   * Method for dynamically building a ValueSource based on a single value of a multivalued field.
   *
   * The default implementation throws an error except in the trivial case where this method is used on 
   * a {@link SchemaField} that is in fact not-multivalued, in which case it delegates to 
   * {@link #getValueSource}
   *
   * @see MultiValueSelector
   */
  public ValueSource getSingleValueSource(MultiValueSelector choice, SchemaField field, QParser parser) {
    // trivial base case
    if (!field.multiValued()) {
      // single value matches any selector
      return getValueSource(field, parser);
    }
    
    throw new SolrException(ErrorCode.BAD_REQUEST, "Selecting a single value from a multivalued field is not supported for this field: " + field.getName() + " (type: " + this.getTypeName() + ")");
  }
  
  /**
   * Method for indicating which {@link MultiValueSelector} (if any) should be used when
   * sorting on a multivalued field of this type for the specified direction (asc/desc).  
   * The default implementation returns <code>null</code> (for all inputs).
   *
   * @param field The SchemaField (of this type) in question
   * @param reverse false if this is an ascending sort, true if this is a descending sort.
   * @return the implicit selector to use for this direction, or null if implicit sorting on the specified direction is not supported and should return an error.
   * @see MultiValueSelector
   */
  public MultiValueSelector getDefaultMultiValueSelectorForSort(SchemaField field, boolean reverse) {
    // trivial base case
    return null;
  }

  /**
   * Returns a Query instance for doing range searches on this field type. {@link org.apache.solr.search.SolrQueryParser}
   * currently passes <code>part1</code> and <code>part2</code> as null if they are '*' respectively. <code>minInclusive</code> and <code>maxInclusive</code> are both true
   * currently by SolrQueryParser but that may change in the future. Also, other QueryParser implementations may have
   * different semantics.
   * <p>
   * By default range queries with '*'s or nulls on either side are treated as existence queries and are created with {@link #getExistenceQuery}.
   * If unbounded range queries should not be treated as existence queries for a certain fieldType, then {@link #treatUnboundedRangeAsExistence} should be overriden.
   * <p>
   * Sub-classes should override the {@link #getSpecializedRangeQuery} method to provide their own range query implementation.
   *
   * @param parser       the {@link org.apache.solr.search.QParser} calling the method
   * @param field        the schema field
   * @param part1        the lower boundary of the range, nulls are allowed.
   * @param part2        the upper boundary of the range, nulls are allowe
   * @param minInclusive whether the minimum of the range is inclusive or not
   * @param maxInclusive whether the maximum of the range is inclusive or not
   * @return a Query instance to perform range search according to given parameters
   */
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    if (part1 == null && part2 == null && treatUnboundedRangeAsExistence(field)) {
      return getExistenceQuery(parser, field);
    }
    return getSpecializedRangeQuery(parser, field, part1, part2, minInclusive, maxInclusive);
  }

  /**
   * Returns whether an unbounded range query should be treated the same as an existence query for the given field type.
   *
   * @param field the schema field
   * @return whether unbounded range and existence are equivalent for the given field type.
   */
  protected boolean treatUnboundedRangeAsExistence(SchemaField field) {
    return true;
  }

  /**
   * Returns a Query instance for doing range searches on this field type. {@link org.apache.solr.search.SolrQueryParser}
   * currently passes <code>part1</code> and <code>part2</code> as null if they are '*' respectively. <code>minInclusive</code> and <code>maxInclusive</code> are both true
   * currently by SolrQueryParser but that may change in the future. Also, other QueryParser implementations may have
   * different semantics.
   * <p>
   * Sub-classes should override this method to provide their own range query implementation. They should strive to
   * handle nulls in <code>part1</code> and/or <code>part2</code> as well as unequal <code>minInclusive</code> and <code>maxInclusive</code> parameters gracefully.
   * <p>
   * This method does not, and should not, check for or handle existence queries, please look at {@link #getRangeQuery} for that logic.
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
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    // TODO: change these all to use readableToIndexed/bytes instead (e.g. for unicode collation)
    final BytesRef miValue = part1 == null ? null : new BytesRef(toInternal(part1));
    final BytesRef maxValue = part2 == null ? null : new BytesRef(toInternal(part2));

    if (field.hasDocValues() && !field.indexed()) {
      return SortedSetDocValuesField.newSlowRangeQuery(
          field.getName(),
          miValue, maxValue,
          minInclusive, maxInclusive);
    } else {
      SolrRangeQuery rangeQuery = new SolrRangeQuery(
          field.getName(),
          miValue, maxValue,
          minInclusive, maxInclusive);
      return rangeQuery;
    }
  }

  /**
   * Returns a Query instance for doing existence searches for a field.
   * If the field does not have docValues or norms, this method will call {@link #getSpecializedExistenceQuery}, which defaults to an unbounded rangeQuery.
   * <p>
   * This method should only be overriden whenever a fieldType does not support {@link org.apache.lucene.search.DocValuesFieldExistsQuery} or {@link org.apache.lucene.search.NormsFieldExistsQuery}.
   * If a fieldType does not support an unbounded rangeQuery as an existenceQuery (such as <code>double</code> or <code>float</code> fields), {@link #getSpecializedExistenceQuery} should be overriden.
   *
   * @param parser The {@link org.apache.solr.search.QParser} calling the method
   * @param field The {@link org.apache.solr.schema.SchemaField} of the field to search
   * @return The {@link org.apache.lucene.search.Query} instance.
   */
  public Query getExistenceQuery(QParser parser, SchemaField field) {
    if (field.hasDocValues()) {
      return new DocValuesFieldExistsQuery(field.getName());
    } else if (!field.omitNorms() && !isPointField()) { //TODO: Remove !isPointField() for SOLR-14199
      return new NormsFieldExistsQuery(field.getName());
    } else {
      // Default to an unbounded range query
      return getSpecializedExistenceQuery(parser, field);
    }
  }

  /**
   * Returns a Query instance for doing existence searches for a field without certain options, such as docValues or norms.
   * <p>
   * This method can be overriden to implement specialized existence logic for fieldTypes.
   * The default query returned is an unbounded range query.
   *
   * @param parser The {@link org.apache.solr.search.QParser} calling the method
   * @param field The {@link org.apache.solr.schema.SchemaField} of the field to search
   * @return The {@link org.apache.lucene.search.Query} instance.
   */
  protected Query getSpecializedExistenceQuery(QParser parser, SchemaField field) {
    return getSpecializedRangeQuery(parser, field, null, null, true, true);
  }

  /**
   * Returns a Query instance for doing searches against a field.
   * @param parser The {@link org.apache.solr.search.QParser} calling the method
   * @param field The {@link org.apache.solr.schema.SchemaField} of the field to search
   * @param externalVal The String representation of the value to search
   * @return The {@link org.apache.lucene.search.Query} instance.  This implementation returns a {@link org.apache.lucene.search.TermQuery} but overriding queries may not
   */
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    if (field.hasDocValues() && !field.indexed()) {
      // match-only
      return getRangeQuery(parser, field, externalVal, externalVal, true, true);
    } else {
      BytesRefBuilder br = new BytesRefBuilder();
      readableToIndexed(externalVal, br);
      return new TermQuery(new Term(field.getName(), br));
    }
  }
  
  /**
   * Returns a Query instance for doing a single term search against a field. This term will not be analyzed before searching.
   * @param parser The {@link org.apache.solr.search.QParser} calling the method
   * @param field The {@link org.apache.solr.schema.SchemaField} of the field to search
   * @param externalVal The String representation of the term value to search
   * @return The {@link org.apache.lucene.search.Query} instance.
   */
  public Query getFieldTermQuery(QParser parser, SchemaField field, String externalVal) {
    return getFieldQuery(parser, field, externalVal);
  }

  /** @lucene.experimental  */
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVals) {
    if (!field.indexed()) {
      // TODO: if the field isn't indexed, this feels like the wrong query type to use?
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (String externalVal : externalVals) {
        Query subq = getFieldQuery(parser, field, externalVal);
        builder.add(subq, BooleanClause.Occur.SHOULD);
      }
      return QueryUtils.build(builder, parser);
    }

    List<BytesRef> lst = new ArrayList<>(externalVals.size());
    BytesRefBuilder br = new BytesRefBuilder();
    for (String externalVal : externalVals) {
      readableToIndexed(externalVal, br);
      lst.add( br.toBytesRef() );
    }
    return new TermInSetQuery(field.getName() , lst);
  }

  /**
   * Expert: Returns the rewrite method for multiterm queries such as wildcards.
   * @param parser The {@link org.apache.solr.search.QParser} calling the method
   * @param field The {@link org.apache.solr.schema.SchemaField} of the field to search
   * @return A suitable rewrite method for rewriting multi-term queries to primitive queries.
   */
  public MultiTermQuery.RewriteMethod getRewriteMethod(QParser parser, SchemaField field) {
    if (!field.indexed() && field.hasDocValues()) {
      return new DocValuesRewriteMethod();
    } else {
      return MultiTermQuery.CONSTANT_SCORE_REWRITE;
    }
  }

  /**
   * Check's {@link org.apache.solr.schema.SchemaField} instances constructed 
   * using this field type to ensure that they are valid.
   *
   * <p>
   * This method is called by the <code>SchemaField</code> constructor to 
   * check that its initialization does not violate any fundamental
   * requirements of the <code>FieldType</code>.
   * Subclasses may choose to throw a {@link SolrException}
   * if invariants are violated by the <code>SchemaField.</code>
   * </p>
   */
  public void checkSchemaField(final SchemaField field) {
    if (field.hasDocValues()) {
      checkSupportsDocValues();
    }
    if (field.isLarge() && field.multiValued()) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Field type " + this + " is 'large'; can't support multiValued");
    }
    if (field.isLarge() && getNumberType() != null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Field type " + this + " is 'large'; can't support numerics");
    }
  }

  /** Called by {@link #checkSchemaField(SchemaField)} if the field has docValues. By default none do. */
  protected void checkSupportsDocValues() {
    throw new SolrException(ErrorCode.SERVER_ERROR, "Field type " + this + " does not support doc values");
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

  protected static final String AUTO_GENERATE_PHRASE_QUERIES = "autoGeneratePhraseQueries";
  protected static final String ENABLE_GRAPH_QUERIES = "enableGraphQueries";
  private static final String ARGS = "args";
  private static final String POSITION_INCREMENT_GAP = "positionIncrementGap";
  protected static final String SYNONYM_QUERY_STYLE = "synonymQueryStyle";

  /**
   * Get a map of property name -&gt; value for this field type. 
   * @param showDefaults if true, include default properties.
   */
  public SimpleOrderedMap<Object> getNamedPropertyValues(boolean showDefaults) {
    SimpleOrderedMap<Object> namedPropertyValues = new SimpleOrderedMap<>();
    namedPropertyValues.add(TYPE_NAME, getTypeName());
    namedPropertyValues.add(CLASS_NAME, getClassArg());
    if (showDefaults) {
      Map<String,String> fieldTypeArgs = getNonFieldPropertyArgs();
      if (null != fieldTypeArgs) {
        for (Map.Entry<String, String> entry : fieldTypeArgs.entrySet()) {
          String key = entry.getKey();
          if ( ! CLASS_NAME.equals(key) && ! TYPE_NAME.equals(key)) {
            namedPropertyValues.add(key, entry.getValue());
          }
        }
      }
      if (this instanceof TextField) {
        namedPropertyValues.add(AUTO_GENERATE_PHRASE_QUERIES, ((TextField) this).getAutoGeneratePhraseQueries());
        namedPropertyValues.add(ENABLE_GRAPH_QUERIES, ((TextField) this).getEnableGraphQueries());
        namedPropertyValues.add(SYNONYM_QUERY_STYLE, ((TextField) this).getSynonymQueryStyle());
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
      namedPropertyValues.add(getPropertyName(LARGE_FIELD), hasProperty(LARGE_FIELD));
      namedPropertyValues.add(getPropertyName(UNINVERTIBLE), hasProperty(UNINVERTIBLE));
      if (hasProperty(SORT_MISSING_FIRST)) {
        namedPropertyValues.add(getPropertyName(SORT_MISSING_FIRST), true);
      } else if (hasProperty(SORT_MISSING_LAST)) {
        namedPropertyValues.add(getPropertyName(SORT_MISSING_LAST), true);
      }
      namedPropertyValues.add(getPropertyName(TOKENIZED), isTokenized());
      // The BINARY property is always false
      // namedPropertyValues.add(getPropertyName(BINARY), hasProperty(BINARY));
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

      for (Map.Entry<String, String> entry : args.entrySet()) {
        String key = entry.getKey();
        if (fieldProperties.contains(key)) {
          namedPropertyValues.add(key, StrUtils.parseBool(entry.getValue()));
        } else if (!CLASS_NAME.equals(key) && !TYPE_NAME.equals(key)) {
          namedPropertyValues.add(key, entry.getValue());
        }
      }
    }

    if (null != getSimilarityFactory()) {
      namedPropertyValues.add(SIMILARITY, getSimilarityFactory().getNamedPropertyValues());
    }
    
    if (this instanceof HasImplicitIndexAnalyzer) {
      if (isExplicitQueryAnalyzer()) {
        namedPropertyValues.add(QUERY_ANALYZER, getAnalyzerProperties(getQueryAnalyzer()));
      }
    } else {
      if (isExplicitAnalyzer()) {
        String analyzerProperty = isExplicitQueryAnalyzer() ? INDEX_ANALYZER : ANALYZER;
        namedPropertyValues.add(analyzerProperty, getAnalyzerProperties(getIndexAnalyzer()));
      }
      if (isExplicitQueryAnalyzer()) {
        String analyzerProperty = isExplicitAnalyzer() ? QUERY_ANALYZER : ANALYZER;
        namedPropertyValues.add(analyzerProperty, getAnalyzerProperties(getQueryAnalyzer()));
      }
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
   * Returns a description of the given analyzer, by either reporting the Analyzer class
   * name (and optionally luceneMatchVersion) if it's not a TokenizerChain, or if it is,
   * querying each analysis factory for its name and args.
   */
  protected static SimpleOrderedMap<Object> getAnalyzerProperties(Analyzer analyzer) {
    SimpleOrderedMap<Object> analyzerProps = new SimpleOrderedMap<>();

    if (analyzer instanceof TokenizerChain) {
      Map<String,String> factoryArgs;
      TokenizerChain tokenizerChain = (TokenizerChain)analyzer;
      CharFilterFactory[] charFilterFactories = tokenizerChain.getCharFilterFactories();
      if (0 < charFilterFactories.length) {
        List<SimpleOrderedMap<Object>> charFilterProps = new ArrayList<>();
        for (CharFilterFactory charFilterFactory : charFilterFactories) {
          SimpleOrderedMap<Object> props = new SimpleOrderedMap<>();
          props.add(CLASS_NAME, charFilterFactory.getClassArg());
          factoryArgs = charFilterFactory.getOriginalArgs();
          if (null != factoryArgs) {
            for (Map.Entry<String, String> entry : factoryArgs.entrySet()) {
              String key = entry.getKey();
              if ( ! CLASS_NAME.equals(key)) {
                if (LUCENE_MATCH_VERSION_PARAM.equals(key)) {
                  if (charFilterFactory.isExplicitLuceneMatchVersion()) {
                    props.add(key, entry.getValue());
                  }
                } else {
                   props.add(key, entry.getValue());
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
        for (Map.Entry<String, String> entry : factoryArgs.entrySet()) {
          String key = entry.getKey();
          if ( ! CLASS_NAME.equals(key)) {
            if (LUCENE_MATCH_VERSION_PARAM.equals(key)) {
              if (tokenizerFactory.isExplicitLuceneMatchVersion()) {
                tokenizerProps.add(key, entry.getValue());
              }
            } else {
              tokenizerProps.add(key, entry.getValue());
            }
          }
        }
      }
      analyzerProps.add(TOKENIZER, tokenizerProps);

      TokenFilterFactory[] filterFactories = tokenizerChain.getTokenFilterFactories();
      if (0 < filterFactories.length) {
        List<SimpleOrderedMap<Object>> filterProps = new ArrayList<>();
        for (TokenFilterFactory filterFactory : filterFactories) {
          SimpleOrderedMap<Object> props = new SimpleOrderedMap<>();
          props.add(CLASS_NAME, filterFactory.getClassArg());
          factoryArgs = filterFactory.getOriginalArgs();
          if (null != factoryArgs) {
            for (Map.Entry<String, String> entry : factoryArgs.entrySet()) {
              String key = entry.getKey();
              if ( ! CLASS_NAME.equals(key)) {
                if (LUCENE_MATCH_VERSION_PARAM.equals(key)) {
                  if (filterFactory.isExplicitLuceneMatchVersion()) {
                    props.add(key, entry.getValue());
                  }
                } else {
                  props.add(key, entry.getValue());
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
      if (analyzer.getVersion() != Version.LATEST) {
        analyzerProps.add(LUCENE_MATCH_VERSION_PARAM, analyzer.getVersion().toString());
      }
    }
    return analyzerProps;
  }

  /**Converts any Object to a java Object native to this field type
   */
  public Object toNativeType(Object val) {
    if (val instanceof CharSequence) {
      return ((CharSequence) val).toString();
    }
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

  /**
   * An enumeration representing various options that may exist for selecting a single value from a 
   * multivalued field.  This class is designed to be an abstract representation, agnostic of some of 
   * the underlying specifics.  Not all enum value are garunteeded work in all contexts -- null checks 
   * must be dont by the caller for the specific methods needed.
   *
   * @see FieldType#getSingleValueSource
   */
  public enum MultiValueSelector {
    // trying to be agnostic about SortedSetSelector.Type vs SortedNumericSelector.Type
    MIN(SortedSetSelector.Type.MIN, SortedNumericSelector.Type.MIN),
    MAX(SortedSetSelector.Type.MAX, SortedNumericSelector.Type.MAX);

    @Override
    public String toString() { return super.toString().toLowerCase(Locale.ROOT); }
    
    /** 
     * The appropriate <code>SortedSetSelector.Type</code> option for this <code>MultiValueSelector</code>,
     * may be null if there is no equivalent
     */
    public SortedSetSelector.Type getSortedSetSelectorType() {
      return sType;
    }

    /** 
     * The appropriate <code>SortedNumericSelector.Type</code> option for this <code>MultiValueSelector</code>,
     * may be null if there is no equivalent
     */
    public SortedNumericSelector.Type getSortedNumericSelectorType() {
      return nType;
    }
    
    private final SortedSetSelector.Type sType;
    private final SortedNumericSelector.Type nType;
    
    private MultiValueSelector(SortedSetSelector.Type sType, SortedNumericSelector.Type nType) {
      this.sType = sType;
      this.nType = nType;
    }

    /**
     * Returns a MultiValueSelector matching the specified (case insensitive) label, or null if 
     * no corrisponding MultiValueSelector exists.
     * 
     * @param label a non null label to be checked for a corrisponding MultiValueSelector
     * @return a MultiValueSelector or null if no MultiValueSelector matches the specified label
     */
    public static MultiValueSelector lookup(String label) {
      if (null == label) {
        throw new NullPointerException("label must not be null when calling MultiValueSelector.lookup");
      }
      try {
        return valueOf(label.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        return null;
      }
    }

  }
  
}
