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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.SortField;
import org.apache.solr.search.QParser;

import org.apache.solr.response.TextResponseWriter;

import java.util.Map;
import java.io.IOException;

/**
 * Encapsulates all information about a Field in a Solr Schema
 *
 *
 */
public final class SchemaField extends FieldProperties {
  final String name;
  final FieldType type;
  final int properties;
  final String defaultValue;
  boolean required = false;  // this can't be final since it may be changed dynamically


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
    this(name, prototype.type, prototype.properties, prototype.defaultValue );
  }

 /** Create a new SchemaField with the given name and type,
   * and with the specified properties.  Properties are *not*
   * inherited from the type in this case, so users of this
   * constructor should derive the properties from type.getProperties()
   *  using all the default properties from the type.
   */
  public SchemaField(String name, FieldType type, int properties, String defaultValue ) {
    this.name = name;
    this.type = type;
    this.properties = properties;
    this.defaultValue = defaultValue;
    
    // initalize with the required property flag
    required = (properties & REQUIRED) !=0;
  }

  public String getName() { return name; }
  public FieldType getType() { return type; }
  int getProperties() { return properties; }

  public boolean indexed() { return (properties & INDEXED)!=0; }
  public boolean stored() { return (properties & STORED)!=0; }
  public boolean storeTermVector() { return (properties & STORE_TERMVECTORS)!=0; }
  public boolean storeTermPositions() { return (properties & STORE_TERMPOSITIONS)!=0; }
  public boolean storeTermOffsets() { return (properties & STORE_TERMOFFSETS)!=0; }
  public boolean omitNorms() { return (properties & OMIT_NORMS)!=0; }
  public boolean omitTf() { return (properties & OMIT_TF_POSITIONS)!=0; }
  public boolean multiValued() { return (properties & MULTIVALUED)!=0; }
  public boolean sortMissingFirst() { return (properties & SORT_MISSING_FIRST)!=0; }
  public boolean sortMissingLast() { return (properties & SORT_MISSING_LAST)!=0; }
  public boolean isRequired() { return required; } 

  // things that should be determined by field type, not set as options
  boolean isTokenized() { return (properties & TOKENIZED)!=0; }
  boolean isBinary() { return (properties & BINARY)!=0; }


  public Fieldable createField(Object val, float boost) {
    return type.createField(this,val,boost);
  }
  
  public Fieldable[] createFields(Object val, float boost) {
    return type.createFields(this,val,boost);
  }

  /**
   * If true, then use {@link #createFields(Object, float)}, else use {@link #createField} to save an extra allocation
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

  public void write(TextResponseWriter writer, String name, Fieldable val) throws IOException {
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
   * Sanity checks that the properties of this field type are plausible 
   * for a field that may be used in sorting, throwing an appropriate 
   * exception (including the field name) if it is not.  FieldType subclasses 
   * can choose to call this method in their getSortField implementation
   * @see FieldType#getSortField
   */
  public void checkSortability() throws SolrException {
    if (! indexed() ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                              "can not sort on unindexed field: " 
                              + getName());
    }
    if ( multiValued() ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                              "can not sort on multivalued field: " 
                              + getName());
    }
  }

  /** 
   * Sanity checks that the properties of this field type are plausible 
   * for a field that may be used to get a FieldCacheSource, throwing 
   * an appropriate exception (including the field name) if it is not.  
   * FieldType subclasses can choose to call this method in their 
   * getValueSource implementation 
   * @see FieldType#getValueSource
   */
  public void checkFieldCacheSource(QParser parser) throws SolrException {
    if (! indexed() ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                              "can not use FieldCache on unindexed field: " 
                              + getName());
    }
    if ( multiValued() ) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                              "can not use FieldCache on multivalued field: " 
                              + getName());
    }
    
  }

  static SchemaField create(String name, FieldType ft, Map<String,String> props) {

    String defaultValue = null;
    if( props.containsKey( "default" ) ) {
    	defaultValue = props.get( "default" );
    }
    return new SchemaField(name, ft, calcProps(name, ft, props), defaultValue );
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

  static int calcProps(String name, FieldType ft, Map<String, String> props) {
    int trueProps = parseProperties(props,true);
    int falseProps = parseProperties(props,false);

    int p = ft.properties;

    //
    // If any properties were explicitly turned off, then turn off other properties
    // that depend on that.
    //
    if (on(falseProps,STORED)) {
      int pp = STORED | BINARY;
      if (on(pp,trueProps)) {
        throw new RuntimeException("SchemaField: " + name + " conflicting stored field options:" + props);
      }
      p &= ~pp;
    }

    if (on(falseProps,INDEXED)) {
      int pp = (INDEXED | OMIT_NORMS | OMIT_TF_POSITIONS
              | STORE_TERMVECTORS | STORE_TERMPOSITIONS | STORE_TERMOFFSETS
              | SORT_MISSING_FIRST | SORT_MISSING_LAST);
      if (on(pp,trueProps)) {
        throw new RuntimeException("SchemaField: " + name + " conflicting indexed field options:" + props);
      }
      p &= ~pp;

    }

    if (on(falseProps,STORE_TERMVECTORS)) {
      int pp = (STORE_TERMVECTORS | STORE_TERMPOSITIONS | STORE_TERMOFFSETS);
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
}






