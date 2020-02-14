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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.response.TextResponseWriter;

/**
 * Encapsulates all information about a Field in a Solr Schema
 *
 *
 */
public final class SchemaField extends FieldProperties implements IndexableFieldType {
  private static final String FIELD_NAME = "name";
  private static final String TYPE_NAME = "type";
  private static final String DEFAULT_VALUE = "default";

  final String name;
  final FieldType type;
  final int properties;
  final String defaultValue;
  boolean required = false;  // this can't be final since it may be changed dynamically
  
  /** Declared field property overrides */
  Map<String,Object> args = Collections.emptyMap();


  /** Create a new SchemaField with the given name and type,
   *  using all the default properties from the type.
   */
  public SchemaField(String name, FieldType type) {
    this(name, type, type.properties, null);
  }

  /** Create a new SchemaField from an existing one by using all
   * of the properties of the prototype except the field name.
   */
  public SchemaField(SchemaField prototype, String name) {
    this(name, prototype.type, prototype.properties, prototype.defaultValue);
    args = prototype.args;
  }

  /** Create a new SchemaField with the given name and type,
   * and with the specified properties.  Properties are *not*
   * inherited from the type in this case, so users of this
   * constructor should derive the properties from type.getSolrProperties()
   *  using all the default properties from the type.
   */
  public SchemaField(String name, FieldType type, int properties, String defaultValue ) {
    this.name = name;
    this.type = type;
    this.properties = properties;
    this.defaultValue = defaultValue;
    
    // initialize with the required property flag
    required = (properties & REQUIRED) !=0;

    type.checkSchemaField(this);
  }

  public String getName() { return name; }
  public FieldType getType() { return type; }
  public int getProperties() { return properties; }

  public boolean isUninvertible() { return (properties & UNINVERTIBLE)!=0; }
  public boolean indexed() { return (properties & INDEXED)!=0; }
  public boolean stored() { return (properties & STORED)!=0; }
  public boolean hasDocValues() { return (properties & DOC_VALUES) != 0; }
  public boolean storeTermVector() { return (properties & STORE_TERMVECTORS)!=0; }
  public boolean storeTermPositions() { return (properties & STORE_TERMPOSITIONS)!=0; }
  public boolean storeTermOffsets() { return (properties & STORE_TERMOFFSETS)!=0; }
  public boolean storeTermPayloads() { return (properties & STORE_TERMPAYLOADS)!=0; }
  public boolean omitNorms() { return (properties & OMIT_NORMS)!=0; }

  public boolean omitTermFreqAndPositions() { return (properties & OMIT_TF_POSITIONS)!=0; }
  public boolean omitPositions() { return (properties & OMIT_POSITIONS)!=0; }
  public boolean storeOffsetsWithPositions() { return (properties & STORE_OFFSETS)!=0; }

  public boolean useDocValuesAsStored() { return (properties & USE_DOCVALUES_AS_STORED)!=0; }

  public boolean multiValued() { return (properties & MULTIVALUED)!=0; }
  public boolean sortMissingFirst() { return (properties & SORT_MISSING_FIRST)!=0; }
  public boolean sortMissingLast() { return (properties & SORT_MISSING_LAST)!=0; }
  public boolean isRequired() { return required; }
  public boolean isLarge() { return (properties & LARGE_FIELD)!=0;}
  public Map<String,?> getArgs() { return Collections.unmodifiableMap(args); }

  // things that should be determined by field type, not set as options
  boolean isTokenized() { return (properties & TOKENIZED)!=0; }
  boolean isBinary() { return (properties & BINARY)!=0; }

  public IndexableField createField(Object val) {
    return type.createField(this,val);
  }

  public List<IndexableField> createFields(Object val) {
    return type.createFields(this,val);
  }

  /**
   * If true, then use {@link #createFields(Object)}, else use {@link #createField} to save an extra allocation
   * @return true if this field is a poly field
   */
  public boolean isPolyField(){
    return type.isPolyField();
  }


  @Override
  public String toString() {
    return name + "{type="+type.getTypeName()
      + ((defaultValue==null)?"":(",default="+defaultValue))
      + ",properties=" + propertiesToString(properties)
      + ( required ? ", required=true" : "" )
      + "}";
  }

  public void write(TextResponseWriter writer, String name, IndexableField val) throws IOException {
    // name is passed in because it may be null if name should not be used.
    type.write(writer,name,val);
  }

  /**
   * Delegates to the FieldType for this field
   * @see FieldType#getSortField
   */
  public SortField getSortField(boolean top) {
    return type.getSortField(this, top);
  }

  /**
   * Expert/advanced method to get the field {@link org.apache.lucene.codecs.PostingsFormat}.
   * @return The {@code postingsFormat} declared; or null if unspecified.
   */
  public String getPostingsFormat() {
    return (String) args.getOrDefault(POSTINGS_FORMAT, type.getPostingsFormat());
  }

  /**
   * Expert/advanced method to get the field {@link org.apache.lucene.codecs.DocValuesFormat}.
   * @return The {@code docValuesFormat} declared; or null if unspecified.
   */
  public String getDocValuesFormat() {
    return (String) args.getOrDefault(DOC_VALUES_FORMAT, type.getDocValuesFormat());
  }

  /**
   * Sanity checks that the properties of this field type are plausible 
   * for a field that may be used in sorting, throwing an appropriate 
   * exception (including the field name) if it is not.  FieldType subclasses 
   * can choose to call this method in their getSortField implementation
   * @see FieldType#getSortField
   */
  public void checkSortability() throws SolrException {
    if ( multiValued()
         // if either of these are non-null, then we should not error
         && null == this.type.getDefaultMultiValueSelectorForSort(this,true)
         && null == this.type.getDefaultMultiValueSelectorForSort(this,false) ) {
      
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                              "can not sort on multivalued field: " 
                              + getName() + " of type: " + this.type.getTypeName());
    }
    if (! hasDocValues() ) {
      if ( ! ( indexed() && isUninvertible() && null != this.type.getUninversionType(this) ) ) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                                "can not sort on a field w/o docValues unless it is indexed=true uninvertible=true and the type supports Uninversion: " 
                                + getName());
      }
    }
  }

  /** 
   * Sanity checks that the properties of this field type are plausible for a field
   * that may be used to get a {@link org.apache.lucene.queries.function.valuesource.FieldCacheSource},
   * throwing an appropriate exception (including the field name) if it is not.  
   * FieldType subclasses can choose to call this method in their 
   * getValueSource implementation 
   * @see FieldType#getValueSource
   */
  public void checkFieldCacheSource() throws SolrException {
    if ( multiValued() ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                              "can not use FieldCache on multivalued field: " 
                              + getName());
    }
    if (! hasDocValues() ) {
      if ( ! ( indexed() && isUninvertible() && null != this.type.getUninversionType(this) ) ) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                                "can not use FieldCache on a field w/o docValues unless it is indexed uninvertible=true and the type supports Uninversion: " 
                                + getName());
      }
    }
  }

  static SchemaField create(String name, FieldType ft, Map<String,?> props) {

    String defaultValue = null;
    if (props.containsKey(DEFAULT_VALUE)) {
      defaultValue = (String)props.get(DEFAULT_VALUE);
    }
    SchemaField field = new SchemaField(name, ft, calcProps(name, ft, props), defaultValue);
    field.args = new HashMap<>(props);
    return field;
  }

  /**
   * Create a SchemaField w/ the props specified.  Does not support a default value.
   * @param name The name of the SchemaField
   * @param ft The {@link org.apache.solr.schema.FieldType} of the field
   * @param props The props.  See {@link #calcProps(String, org.apache.solr.schema.FieldType, java.util.Map)}
   * @param defValue The default Value for the field
   * @return The SchemaField
   *
   * @see #create(String, FieldType, java.util.Map)
   */
  static SchemaField create(String name, FieldType ft, int props, String defValue){
    return new SchemaField(name, ft, props, defValue);
  }

  static int calcProps(String name, FieldType ft, Map<String,?> props) {
    int trueProps = parseProperties(props,true,true);
    int falseProps = parseProperties(props,false,true);

    int p = ft.properties;

    //
    // If any properties were explicitly turned off, then turn off other properties
    // that depend on that.
    //
    if (on(falseProps,STORED)) {
      int pp = STORED | BINARY | LARGE_FIELD;
      if (on(pp,trueProps)) {
        throw new RuntimeException("SchemaField: " + name + " conflicting stored field options:" + props);
      }
      p &= ~pp;
    }

    if (on(falseProps,INDEXED)) {
      int pp = (INDEXED 
              | STORE_TERMVECTORS | STORE_TERMPOSITIONS | STORE_TERMOFFSETS | STORE_TERMPAYLOADS | UNINVERTIBLE);
      if (on(pp,trueProps)) {
        throw new RuntimeException("SchemaField: " + name + " conflicting 'true' field options for non-indexed field:" + props);
      }
      p &= ~pp;
    }
    
    if (on(falseProps,UNINVERTIBLE) && on(falseProps,DOC_VALUES)) {
      int pp = (SORT_MISSING_FIRST | SORT_MISSING_LAST);
      if (on(pp,trueProps)) {
        throw new RuntimeException("SchemaField: " + name + " conflicting 'true' field options for non-docValues/non-uninvertible field:" + props);
      }
      p &= ~pp;
    }
    
    if (on(falseProps,INDEXED)) {
      int pp = (OMIT_NORMS | OMIT_TF_POSITIONS | OMIT_POSITIONS);
      if (on(pp,falseProps)) {
        throw new RuntimeException("SchemaField: " + name + " conflicting 'false' field options for non-indexed field:" + props);
      }
      p &= ~pp;

    }

    if (on(trueProps,OMIT_TF_POSITIONS)) {
      int pp = (OMIT_POSITIONS | OMIT_TF_POSITIONS);
      if (on(pp, falseProps)) {
        throw new RuntimeException("SchemaField: " + name + " conflicting tf and position field options:" + props);
      }
      p &= ~pp;
    }

    if (on(falseProps,STORE_TERMVECTORS)) {
      int pp = (STORE_TERMVECTORS | STORE_TERMPOSITIONS | STORE_TERMOFFSETS | STORE_TERMPAYLOADS);
      if (on(pp,trueProps)) {
        throw new RuntimeException("SchemaField: " + name + " conflicting termvector field options:" + props);
      }
      p &= ~pp;
    }

    // override sort flags
    if (on(trueProps,SORT_MISSING_FIRST)) {
      p &= ~SORT_MISSING_LAST;
    }

    if (on(trueProps,SORT_MISSING_LAST)) {
      p &= ~SORT_MISSING_FIRST;
    }

    p &= ~falseProps;
    p |= trueProps;
    return p;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return(obj instanceof SchemaField) && name.equals(((SchemaField)obj).name);
  }

  /**
   * Get a map of property name -&gt; value for this field.  If showDefaults is true,
   * include default properties (those inherited from the declared property type and
   * not overridden in the field declaration).
   */
  public SimpleOrderedMap<Object> getNamedPropertyValues(boolean showDefaults) {
    SimpleOrderedMap<Object> properties = new SimpleOrderedMap<>();
    properties.add(FIELD_NAME, getName());
    properties.add(TYPE_NAME, getType().getTypeName());
    if (showDefaults) {
      if (null != getDefaultValue()) {
        properties.add(DEFAULT_VALUE, getDefaultValue());
      }
      properties.add(getPropertyName(INDEXED), indexed());
      properties.add(getPropertyName(STORED), stored());
      properties.add(getPropertyName(DOC_VALUES), hasDocValues());
      properties.add(getPropertyName(STORE_TERMVECTORS), storeTermVector());
      properties.add(getPropertyName(STORE_TERMPOSITIONS), storeTermPositions());
      properties.add(getPropertyName(STORE_TERMOFFSETS), storeTermOffsets());
      properties.add(getPropertyName(STORE_TERMPAYLOADS), storeTermPayloads());
      properties.add(getPropertyName(OMIT_NORMS), omitNorms());
      properties.add(getPropertyName(OMIT_TF_POSITIONS), omitTermFreqAndPositions());
      properties.add(getPropertyName(OMIT_POSITIONS), omitPositions());
      properties.add(getPropertyName(STORE_OFFSETS), storeOffsetsWithPositions());
      properties.add(getPropertyName(MULTIVALUED), multiValued());
      properties.add(getPropertyName(LARGE_FIELD), isLarge());
      properties.add(getPropertyName(UNINVERTIBLE), isUninvertible());
      if (sortMissingFirst()) {
        properties.add(getPropertyName(SORT_MISSING_FIRST), sortMissingFirst());
      } else if (sortMissingLast()) {
        properties.add(getPropertyName(SORT_MISSING_LAST), sortMissingLast());
      }
      properties.add(getPropertyName(REQUIRED), isRequired());
      properties.add(getPropertyName(TOKENIZED), isTokenized());
      properties.add(getPropertyName(USE_DOCVALUES_AS_STORED), useDocValuesAsStored());
      // The BINARY property is always false
      // properties.add(getPropertyName(BINARY), isBinary());
    } else {
      for (Map.Entry<String,?> arg : args.entrySet()) {
        String key = arg.getKey();
        Object value = arg.getValue();
        if (key.equals(DEFAULT_VALUE)) {
          properties.add(key, value);
        } else {
          boolean boolVal = value instanceof Boolean ? (Boolean)value : Boolean.parseBoolean(value.toString());
          properties.add(key, boolVal);
        }
      }
    }
    return properties;
  }

  @Override
  public boolean tokenized() {
    return isTokenized();
  }

  @Override
  public boolean storeTermVectors() {
    return storeTermVector();
  }

  @Override
  public boolean storeTermVectorOffsets() {
    return storeTermOffsets();
  }

  @Override
  public boolean storeTermVectorPositions() {
    return storeTermPositions();
  }

  @Override
  public boolean storeTermVectorPayloads() {
    return storeTermPayloads();
  }

  @Override
  public IndexOptions indexOptions() {
    if (!indexed()) {
      return IndexOptions.NONE;
    }
    
    IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    if (omitTermFreqAndPositions()) {
      options = IndexOptions.DOCS;
    } else if (omitPositions()) {
      options = IndexOptions.DOCS_AND_FREQS;
    } else if (storeOffsetsWithPositions()) {
      options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
    }

    return options;
  }

  @Override
  public DocValuesType docValuesType() {
    return DocValuesType.NONE;
  }

  @Override
  public int pointDimensionCount() {
    return 0;
  }

  @Override
  public int pointIndexDimensionCount() {
    return 0;
  }

  @Override
  public int pointNumBytes() {
    return 0;
  }

  @Override
  public Map<String, String> getAttributes() {
    return null;
  }
}
