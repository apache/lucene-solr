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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.OrdFieldSource;
import org.apache.solr.search.Sorting;
import org.apache.solr.search.QParser;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.request.TextResponseWriter;
import org.apache.solr.analysis.SolrAnalyzer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.MapSolrParams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.io.Reader;
import java.io.IOException;

/**
 * Base class for all field types used by an index schema.
 *
 * @version $Id$
 */
public abstract class FieldType extends FieldProperties {
  public static final Logger log = LoggerFactory.getLogger(FieldType.class);

  /**
   * The default poly field separator.
   *
   * @see #createFields(SchemaField, String, float)
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

  /**
   * A "polyField" is a FieldType that can produce more than one Field per FieldType, via the {@link #createFields(org.apache.solr.schema.SchemaField, String, float)} method.  This is useful
   * when hiding the implementation details of a field from the Solr end user.  For instance, a spatial point may be represented by three different field types, all of which may produce 1 or more
   * fields.
   * @return true if the {@link #createFields(org.apache.solr.schema.SchemaField, String, float)} method may return more than one field
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
    if (schema.getVersion()< 1.1f) properties |= MULTIVALUED;
    if (schema.getVersion()> 1.1f) properties |= OMIT_TF_POSITIONS;

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
  public Field createField(SchemaField field, String externalVal, float boost) {
    if (!field.indexed() && !field.stored()) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: " + field);
      return null;
    }
    String val;
    try {
      val = toInternal(externalVal);
    } catch (RuntimeException e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Error while creating field '" + field + "' from value '" + externalVal + "'", e, false);
    }
    if (val==null) return null;

    return createField(field.getName(), val, getFieldStore(field, val),
            getFieldIndex(field, val), getFieldTermVec(field, val), field.omitNorms(),
            field.omitTf(), boost);
  }



  /**
   * Create multiple fields from a single field and multiple values.  Fields are named as SchemaField.getName() + {@link #POLY_FIELD_SEPARATOR} + i, where
   * i starts at 0.
   * <p/>
   * If the field is stored, then an extra field gets created that contains the storageVal.  It is this field that also
   *
   * @param field The {@link org.apache.solr.schema.SchemaField}
   * @param props The properties to use
   * @param delegatedType An optional type to use.  If null, then field.getType() is used.  Useful for poly fields.
   * @param storageVal If the field stores, then this value will be used for the stored field
   * @param boost The boost to apply to all fields
   * @param externalVals The values to use
   * @return The fields
   */
  protected Fieldable[] createFields(SchemaField field, int props,
                                 FieldType delegatedType, String storageVal,
                                 float boost, String ... externalVals) {
    int n = field.indexed() ? externalVals.length : 0;
    n += field.stored() ? 1 : 0;
    if (delegatedType == null) { //if the type isn't being overriden, then just use the base one
      delegatedType = field.getType();
    }
    Field[] results = new Field[n];
    //Field.Store.NO,Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO, true, true

    if (externalVals.length > 0) {
      if (field.indexed()) {
        String name = field.getName() + "_";
        String suffix = POLY_FIELD_SEPARATOR + delegatedType.typeName;

        int len = name.length();
        StringBuilder bldr = new StringBuilder(len + 3 + suffix.length());//should be enough buffer to handle most values of j.
        bldr.append(name);
        for (int j = 0; j < externalVals.length; j++) {
          //SchemaField is final, as is name, so we need to recreate each time
          //put the counter before the separator, b/c dynamic fields can't be asterisks on both the front and the end of the String
          bldr.append(j).append(suffix);
          SchemaField sf = SchemaField.create(bldr.toString(),
                  delegatedType, props, null);
                  //schema.getDynamicField(name  + "_" + j + POLY_FIELD_SEPARATOR + delegatedType.typeName);
                  /**/
          //new SchemaField(name, ft, p, defaultValue )
          //QUESTION: should we allow for vectors, etc?  Not sure that it makes sense
          results[j] = delegatedType.createField(sf, externalVals[j], boost);
          bldr.setLength(len);//cut the builder back to just the length of the prefix, but keep the capacity
        }
      }
      Field.TermVector fieldTermVec = getFieldTermVec(field, storageVal);
      if (field.stored() || fieldTermVec.equals(Field.TermVector.YES)
              || fieldTermVec.equals(Field.TermVector.WITH_OFFSETS)
              || fieldTermVec.equals(Field.TermVector.WITH_POSITIONS)
              || fieldTermVec.equals(Field.TermVector.WITH_POSITIONS_OFFSETS)
      ) {

          //QUESTION: should we allow for vectors, etc?  Not sure that it makes sense
        results[results.length - 1] = createField(field.getName(), storageVal, getFieldStore(field, storageVal),
                Field.Index.NO,
                fieldTermVec, field.omitNorms(), field.omitTf(), boost);
         
      }

    }
    return results;
  }

  /**
   * Create the field from native Lucene parts.  Mostly intended for use by FieldTypes outputing multiple
   * Fields per SchemaField
   * @param name The name of the field
   * @param val The _internal_ value to index
   * @param storage {@link org.apache.lucene.document.Field.Store}
   * @param index {@link org.apache.lucene.document.Field.Index}
   * @param vec {@link org.apache.lucene.document.Field.TermVector}
   * @param omitNorms true if norms should be omitted
   * @param omitTFPos true if term freq and position should be omitted.
   * @param boost The boost value
   * @return the {@link org.apache.lucene.document.Field}.
   */
  protected Field createField(String name, String val, Field.Store storage, Field.Index index,
                                    Field.TermVector vec, boolean omitNorms, boolean omitTFPos, float boost){
    Field f = new Field(name,
                        val,
                        storage,
                        index,
                        vec);
    f.setOmitNorms(omitNorms);
    f.setOmitTermFreqAndPositions(omitTFPos);
    f.setBoost(boost);
    return f;
  }

  /**
   * Given a {@link org.apache.solr.schema.SchemaField}, create one or more {@link org.apache.lucene.document.Field} instances
   * @param field the {@link org.apache.solr.schema.SchemaField}
   * @param externalVal The value to add to the field
   * @param boost The boost to apply
   * @return The {@link org.apache.lucene.document.Field} instances
   *
   * @see #createField(SchemaField, String, float)
   * @see #isPolyField()
   */
  public Fieldable[] createFields(SchemaField field, String externalVal, float boost) {
    Field f = createField( field, externalVal, boost);
    if( f != null ) {
      return new Field[] { f };
    }
    return null;
  }

  /* Helpers for field construction */
  protected Field.TermVector getFieldTermVec(SchemaField field,
                                             String internalVal) {
    Field.TermVector ftv = Field.TermVector.NO;
    if (field.storeTermPositions() && field.storeTermOffsets())
      ftv = Field.TermVector.WITH_POSITIONS_OFFSETS;
    else if (field.storeTermPositions())
      ftv = Field.TermVector.WITH_POSITIONS;
    else if (field.storeTermOffsets())
      ftv = Field.TermVector.WITH_OFFSETS;
    else if (field.storeTermVector())
      ftv = Field.TermVector.YES;
    return ftv;
  }
  protected Field.Store getFieldStore(SchemaField field,
                                      String internalVal) {
    return field.stored() ? Field.Store.YES : Field.Store.NO;
  }
  protected Field.Index getFieldIndex(SchemaField field,
                                      String internalVal) {
    return field.indexed() ? (isTokenized() ? Field.Index.TOKENIZED :
                              Field.Index.UN_TOKENIZED) : Field.Index.NO;
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
  public String toExternal(Fieldable f) {
    // currently used in writing XML of the search result (but perhaps
    // a more efficient toXML(Fieldable f, Writer w) should be used
    // in the future.
    return f.stringValue();
  }

  /**
   * Convert the stored-field format to an external object.
   * @see #toInternal
   * @since solr 1.3
   */
  public Object toObject(Fieldable f) {
    return toExternal(f); // by default use the string
  }

  /** Given an indexed term, return the human readable representation */
  public String indexedToReadable(String indexedForm) {
    return indexedForm;
  }

  /** Given the stored field, return the human readable representation */
  public String storedToReadable(Fieldable f) {
    return toExternal(f);
  }

  /** Given the stored field, return the indexed form */
  public String storedToIndexed(Fieldable f) {
    // right now, the transformation of single valued fields like SortableInt
    // is done when the Field is created, not at analysis time... this means
    // that the indexed form is the same as the stored field form.
    return f.stringValue();
  }

  /** Given the readable value, return the term value that will match it. */
  public String readableToIndexed(String val) {
    return toInternal(val);
  }

  /**
   * Default analyzer for types that only produce 1 verbatim token...
   * A maximum size of chars to be read must be specified
   */
  protected class DefaultAnalyzer extends SolrAnalyzer {
    final int maxChars;

    DefaultAnalyzer(int maxChars) {
      this.maxChars=maxChars;
    }

    public TokenStreamInfo getStream(String fieldName, Reader reader) {
      Tokenizer ts = new Tokenizer(reader) {
        final char[] cbuf = new char[maxChars];
        final TermAttribute termAtt = (TermAttribute) addAttribute(TermAttribute.class);
        final OffsetAttribute offsetAtt = (OffsetAttribute) addAttribute(OffsetAttribute.class);
        @Override
        public boolean incrementToken() throws IOException {
          clearAttributes();
          int n = input.read(cbuf,0,maxChars);
          if (n<=0) return false;
          String s = toInternal(new String(cbuf,0,n));
          termAtt.setTermBuffer(s);
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
   */
  protected Analyzer analyzer=new DefaultAnalyzer(256);

  /**
   * Analyzer set by schema for text types to use when searching fields
   * of this type, subclasses can set analyzer themselves or override
   * getAnalyzer()
   * @see #getQueryAnalyzer
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

  /**
   * Sets the Analyzer to be used when indexing fields of this type.
   * @see #getAnalyzer
   */
  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
    log.trace("FieldType: " + typeName + ".setAnalyzer(" + analyzer.getClass().getName() + ")" );
  }

  /**
   * Sets the Analyzer to be used when querying fields of this type.
   * @see #getQueryAnalyzer
   */
  public void setQueryAnalyzer(Analyzer analyzer) {
    this.queryAnalyzer = analyzer;
    log.trace("FieldType: " + typeName + ".setQueryAnalyzer(" + analyzer.getClass().getName() + ")" );
  }

  /**
   * Renders the specified field as XML
   */
  public abstract void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException;

  /**
   * calls back to TextResponseWriter to write the field value
   */
  public abstract void write(TextResponseWriter writer, String name, Fieldable f) throws IOException;


  /**
   * Returns the SortField instance that should be used to sort fields
   * of this type.
   */
  public abstract SortField getSortField(SchemaField field, boolean top);

  /**
   * Utility usable by subclasses when they want to get basic String sorting.
   */
  protected SortField getStringSort(SchemaField field, boolean reverse) {
    return Sorting.getStringSortField(field.name, reverse, field.sortMissingLast(),field.sortMissingFirst());
  }

  /** called to get the default value source (normally, from the
   *  Lucene FieldCache.)
   */
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    return getValueSource(field);
  }


  /**
   * @deprecated use {@link #getValueSource(SchemaField, QParser)}
   */
  @Deprecated
  public ValueSource getValueSource(SchemaField field) {
    return new OrdFieldSource(field.name);
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
*    @return a Query instance to perform range search according to given parameters
   *
   * @see org.apache.solr.search.SolrQueryParser#getRangeQuery(String, String, String, boolean)
   */
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    // constant score mode is now enabled per default
    return new TermRangeQuery(
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
    return new TermQuery(new Term(field.getName(), toInternal(externalVal)));
  }


  /**
   * Return a collection of all the Fields in the index where the {@link org.apache.solr.schema.SchemaField}
   * @param polyField The instance of the {@link org.apache.solr.schema.SchemaField} to find the actual field names from
   * @return The {@link java.util.Collection} of names of the actual fields that are a poly field.
   *
   *
   */
  /*protected Collection<String> getPolyFieldNames(SchemaField polyField){
    if (polyField.isPolyField()) {
      if (polyField != null) {
        //we need the names of all the fields.  Do this lazily and then cache?


      }
    } //TODO: Should we throw an exception here in an else clause?
    return Collections.emptyList();
  }*/


}
