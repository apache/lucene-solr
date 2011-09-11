/**
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
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.analysis.SolrAnalyzer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.Sorting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

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
  int properties;


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

  protected String getArg(String n, Map<String,String> args) {
    String s = args.remove(n);
    if (s == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Missing parameter '"+n+"' for FieldType=" + typeName +args);
    }
    return s;
  }

  // Handle additional arguments...
  void setArgs(IndexSchema schema, Map<String,String> args) {
    // default to STORED, INDEXED, OMIT_TF_POSITIONS and MULTIVALUED depending on schema version
    properties = (STORED | INDEXED);
    float schemaVersion = schema.getVersion();
    if (schemaVersion < 1.1f) properties |= MULTIVALUED;
    if (schemaVersion > 1.1f) properties |= OMIT_TF_POSITIONS;
    if (schemaVersion < 1.3) {
      args.remove("compressThreshold");
    }

    this.args=args;
    Map<String,String> initArgs = new HashMap<String,String>(args);

    trueProperties = FieldProperties.parseProperties(initArgs,true);
    falseProperties = FieldProperties.parseProperties(initArgs,false);

    properties &= ~falseProperties;
    properties |= trueProperties;

    for (String prop : FieldProperties.propertyNames) initArgs.remove(prop);

    init(schema, initArgs);

    String positionInc = initArgs.get("positionIncrementGap");
    if (positionInc != null) {
      Analyzer analyzer = getAnalyzer();
      if (analyzer instanceof SolrAnalyzer) {
        ((SolrAnalyzer)analyzer).setPositionIncrementGap(Integer.parseInt(positionInc));
      } else {
        throw new RuntimeException("Can't set positionIncrementGap on custom analyzer " + analyzer.getClass());
      }
      analyzer = getQueryAnalyzer();
      if (analyzer instanceof SolrAnalyzer) {
        ((SolrAnalyzer)analyzer).setPositionIncrementGap(Integer.parseInt(positionInc));
      } else {
        throw new RuntimeException("Can't set positionIncrementGap on custom analyzer " + analyzer.getClass());
      }
      initArgs.remove("positionIncrementGap");
    }

    final String codec = initArgs.get("codec");
    if (codec != null) {
      this.codec = codec;
      initArgs.remove("codec");
    }

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
            + (analyzer != null ? ",analyzer=" + analyzer.getClass().getName() : "")
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
  public IndexableField createField(SchemaField field, Object value, float boost) {
    if (!field.indexed() && !field.stored()) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: " + field);
      return null;
    }
    
    String val;
    try {
      val = toInternal(value.toString());
    } catch (RuntimeException e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Error while creating field '" + field + "' from value '" + value + "'", e, false);
    }
    if (val==null) return null;

    org.apache.lucene.document.FieldType newType = new org.apache.lucene.document.FieldType();
    newType.setIndexed(field.indexed());
    newType.setTokenized(field.isTokenized());
    newType.setStored(field.stored());
    newType.setOmitNorms(field.omitNorms());
    newType.setIndexOptions(getIndexOptions(field, val));
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
  protected IndexableField createField(String name, String val, org.apache.lucene.document.FieldType type, float boost){
    Field f = new Field(name, type, val);
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
  public IndexableField[] createFields(SchemaField field, Object value, float boost) {
    IndexableField f = createField( field, value, boost);
    return f==null ? new IndexableField[]{} : new IndexableField[]{f};
  }
  protected IndexOptions getIndexOptions(SchemaField field,
                                         String internalVal) {
    IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    if (field.omitTermFreqAndPositions()) {
      options = IndexOptions.DOCS_ONLY;
    } else if (field.omitPositions()) {
      options = IndexOptions.DOCS_AND_FREQS;
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
  public String toExternal(IndexableField f) {
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
  public Object toObject(IndexableField f) {
    return toExternal(f); // by default use the string
  }

  public Object toObject(SchemaField sf, BytesRef term) {
    final CharsRef ref = new CharsRef(term.length);
    indexedToReadable(term, ref);
    final IndexableField f = createField(sf, ref.toString(), 1.0f);
    return toObject(f);
  }

  /** Given an indexed term, return the human readable representation */
  public String indexedToReadable(String indexedForm) {
    return indexedForm;
  }

  /** Given an indexed term, append the human readable representation*/
  public CharsRef indexedToReadable(BytesRef input, CharsRef output) {
    input.utf8ToChars(output);
    return output;
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

  /** Given the readable value, return the term value that will match it. */
  public void readableToIndexed(CharSequence val, BytesRef result) {
    final String internal = readableToIndexed(val.toString());
    UnicodeUtil.UTF16toUTF8(internal, 0, internal.length(), result);
  }

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
    public TokenStreamInfo getStream(String fieldName, Reader reader) {
      Tokenizer ts = new Tokenizer(reader) {
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

      return new TokenStreamInfo(ts, ts);
    }
  }

  /**
   * Analyzer set by schema for text types to use when indexing fields
   * of this type, subclasses can set analyzer themselves or override
   * getAnalyzer()
   * @see #getAnalyzer
   * @see #setAnalyzer
   */
  protected Analyzer analyzer=new DefaultAnalyzer(256);

  /**
   * Analyzer set by schema for text types to use when searching fields
   * of this type, subclasses can set analyzer themselves or override
   * getAnalyzer()
   * @see #getQueryAnalyzer
   * @see #setQueryAnalyzer
   */
  protected Analyzer queryAnalyzer=analyzer;

  /**
   * Returns the Analyzer to be used when indexing fields of this type.
   * <p>
   * This method may be called many times, at any time.
   * </p>
   * @see #getQueryAnalyzer
   */
  public Analyzer getAnalyzer() {
    return analyzer;
  }

  /**
   * Returns the Analyzer to be used when searching fields of this type.
   * <p>
   * This method may be called many times, at any time.
   * </p>
   * @see #getAnalyzer
   */
  public Analyzer getQueryAnalyzer() {
    return queryAnalyzer;
  }

  private final String analyzerError = 
    "FieldType: " + this.getClass().getSimpleName() + 
    " (" + typeName + ") does not support specifying an analyzer";

  /**
   * Sets the Analyzer to be used when indexing fields of this type.
   *
   * <p>
   * The default implementation throws a SolrException.  
   * Subclasses that override this method need to ensure the behavior 
   * of the analyzer is consistent with the implementation of toInternal.
   * </p>
   * 
   * @see #toInternal
   * @see #setQueryAnalyzer
   * @see #getAnalyzer
   */
  public void setAnalyzer(Analyzer analyzer) {
    SolrException e = new SolrException
      (ErrorCode.SERVER_ERROR,
       "FieldType: " + this.getClass().getSimpleName() + 
       " (" + typeName + ") does not support specifying an analyzer");
    SolrException.logOnce(log,null,e);
    throw e;
  }

  /**
   * Sets the Analyzer to be used when querying fields of this type.
   *
   * <p>
   * The default implementation throws a SolrException.  
   * Subclasses that override this method need to ensure the behavior 
   * of the analyzer is consistent with the implementation of toInternal.
   * </p>
   * 
   * @see #toInternal
   * @see #setAnalyzer
   * @see #getQueryAnalyzer
   */
  public void setQueryAnalyzer(Analyzer analyzer) {
    SolrException e = new SolrException
      (ErrorCode.SERVER_ERROR,
       "FieldType: " + this.getClass().getSimpleName() +
       " (" + typeName + ") does not support specifying an analyzer");
    SolrException.logOnce(log,null,e);
    throw e;
  }

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
   * This method exists to internally support SolrSimilarityProvider. 
   * Custom application code interested in a field's Similarity should
   * instead query via the searcher's SimilarityProvider.
   * @lucene.internal
   */
  public Similarity getSimilarity() {
    return similarity;
  }
  
  /**
   * Sets the Similarity used when scoring fields of this type
   * @lucene.internal
   */
  public void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
  }
  
  /**
   * The codec ID used for this field type
   */
  protected String codec;
  
  public String getCodec() {
    return codec;
  }
  
  /**
   * calls back to TextResponseWriter to write the field value
   */
  public abstract void write(TextResponseWriter writer, String name, IndexableField f) throws IOException;


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
   * @param parser
   * @param field        the schema field
   * @param part1        the lower boundary of the range, nulls are allowed.
   * @param part2        the upper boundary of the range, nulls are allowed
   * @param minInclusive whether the minimum of the range is inclusive or not
   * @param maxInclusive whether the maximum of the range is inclusive or not
   *  @return a Query instance to perform range search according to given parameters
   *
   */
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    // constant score mode is now enabled per default
    return TermRangeQuery.newStringRange(
            field.getName(),
            part1 == null ? null : toInternal(part1),
            part2 == null ? null : toInternal(part2),
            minInclusive, maxInclusive);
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
    BytesRef br = new BytesRef();
    readableToIndexed(externalVal, br);
    return new TermQuery(new Term(field.getName(), br));
  }
}
