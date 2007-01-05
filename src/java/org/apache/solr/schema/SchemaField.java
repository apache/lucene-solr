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
import org.apache.lucene.search.SortField;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.request.TextResponseWriter;

import java.util.Map;
import java.io.IOException;

/**
 * Encapsulates all information about a Field in a Solr Schema
 *
 * @author yonik
 * @version $Id$
 */
public final class SchemaField extends FieldProperties {
  final String name;
  final FieldType type;
  final int properties;
  final String defaultValue;


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
  public boolean multiValued() { return (properties & MULTIVALUED)!=0; }
  public boolean sortMissingFirst() { return (properties & SORT_MISSING_FIRST)!=0; }
  public boolean sortMissingLast() { return (properties & SORT_MISSING_LAST)!=0; }
  public boolean isCompressed() { return (properties & COMPRESSED)!=0; }

  // things that should be determined by field type, not set as options
  boolean isTokenized() { return (properties & TOKENIZED)!=0; }
  boolean isBinary() { return (properties & BINARY)!=0; }

  public Field createField(String val, float boost) {
    return type.createField(this,val,boost);
  }

  public String toString() {
    return name + "{type="+type.getTypeName()
      + ((defaultValue==null)?"":(",default="+defaultValue))
      + ",properties=" + propertiesToString(properties)
      + "}";
  }

  public void write(XMLWriter writer, String name, Fieldable val) throws IOException {
    // name is passed in because it may be null if name should not be used.
    type.write(writer,name,val);
  }

  public void write(TextResponseWriter writer, String name, Fieldable val) throws IOException {
    // name is passed in because it may be null if name should not be used.
    type.write(writer,name,val);
  }

  public SortField getSortField(boolean top) {
    return type.getSortField(this, top);
  }


  static SchemaField create(String name, FieldType ft, Map props) {
    int trueProps = parseProperties(props,true);
    int falseProps = parseProperties(props,false);

    int p = ft.properties;

    //
    // If any properties were explicitly turned off, then turn off other properties
    // that depend on that.
    //
    if (on(falseProps,STORED)) {
      int pp = STORED | BINARY | COMPRESSED;
      if (on(pp,trueProps)) {
        throw new RuntimeException("SchemaField: " + name + " conflicting stored field options:" + props);
      }
      p &= ~pp;
    }

    if (on(falseProps,INDEXED)) {
      int pp = (INDEXED | OMIT_NORMS
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

    String defaultValue = null;
    if( props.containsKey( "default" ) ) {
    	defaultValue = (String)props.get( "default" );
    }
    return new SchemaField(name, ft, p, defaultValue );
  }

  public String getDefaultValue() {
    return defaultValue;
  }
}




