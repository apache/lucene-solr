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

package org.apache.solr.common;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represent the field and boost information needed to construct and index
 * a Lucene Document.  Like the SolrDocument, the field values should
 * match those specified in schema.xml 
 *
 *
 * @since solr 1.3
 */
public class SolrInputDocument implements Map<String,SolrInputField>, Iterable<SolrInputField>, Serializable
{
  private final Map<String,SolrInputField> _fields;
  private float _documentBoost = 1.0f;

  public SolrInputDocument() {
    _fields = new LinkedHashMap<String,SolrInputField>();
  }
  
  public SolrInputDocument(Map<String,SolrInputField> fields) {
    _fields = fields;
  }
  
  /**
   * Remove all fields and boosts from the document
   */
  @Override
  public void clear()
  {
    if( _fields != null ) {
      _fields.clear();
    }
  }

  ///////////////////////////////////////////////////////////////////
  // Add / Set fields
  ///////////////////////////////////////////////////////////////////

  /** 
   * Add a field with implied null value for boost.
   * 
   * The class type of value and the name parameter should match schema.xml. 
   * schema.xml can be found in conf directory under the solr home by default.
   * 
   * @param name Name of the field, should match one of the field names defined under "fields" tag in schema.xml.
   * @param value Value of the field, should be of same class type as defined by "type" attribute of the corresponding field in schema.xml. 
   * @see #addField(String, Object, float)
   */
  public void addField(String name, Object value) 
  {
    addField(name, value, 1.0f );
  }
  
  /** Get the first value for a field.
   * 
   * @param name name of the field to fetch
   * @return first value of the field or null if not present
   */
  public Object getFieldValue(String name) 
  {
    SolrInputField field = getField(name);
    Object o = null;
    if (field!=null) o = field.getFirstValue();
    return o;
  }
  
  /** Get all the values for a field.
   * 
   * @param name name of the field to fetch
   * @return value of the field or null if not set
   */
  public Collection<Object> getFieldValues(String name) 
  {
    SolrInputField field = getField(name);
    if (field!=null) {
      return field.getValues();
    }
    return null;
  } 
  
  /** Get all field names.
   * 
   * @return Set of all field names.
   */
  public Collection<String> getFieldNames() 
  {
    return _fields.keySet();
  }
  
  /** Set a field with implied null value for boost.
   * 
   * @see #setField(String, Object, float)
   * @param name name of the field to set
   * @param value value of the field
   */
  public void setField(String name, Object value) 
  {
    setField(name, value, 1.0f );
  }
  
  public void setField(String name, Object value, float boost ) 
  {
    SolrInputField field = new SolrInputField( name );
    _fields.put( name, field );
    field.setValue( value, boost );
  }

  /**
   * Adds a field with the given name, value and boost.  If a field with the
   * name already exists, then the given value is appended to the value of that
   * field, with the new boost. If the value is a collection, then each of its
   * values will be added to the field.
   *
   * The class type of value and the name parameter should match schema.xml. 
   * schema.xml can be found in conf directory under the solr home by default.
   * 
   * @param name Name of the field, should match one of the field names defined under "fields" tag in schema.xml.
   * @param value Value of the field, should be of same class type as defined by "type" attribute of the corresponding field in schema.xml. 
   * @param boost Boost value for the field
   */
  public void addField(String name, Object value, float boost ) 
  {
    SolrInputField field = _fields.get( name );
    if( field == null || field.value == null ) {
      setField(name, value, boost);
    }
    else {
      field.addValue( value, boost );
    }
  }

  /**
   * Remove a field from the document
   * 
   * @param name The field name whose field is to be removed from the document
   * @return the previous field with <tt>name</tt>, or
   *         <tt>null</tt> if there was no field for <tt>key</tt>.
   */
  public SolrInputField removeField(String name) {
    return _fields.remove( name );
  }

  ///////////////////////////////////////////////////////////////////
  // Get the field values
  ///////////////////////////////////////////////////////////////////

  public SolrInputField getField( String field )
  {
    return _fields.get( field );
  }

  @Override
  public Iterator<SolrInputField> iterator() {
    return _fields.values().iterator();
  }
  
  public float getDocumentBoost() {
    return _documentBoost;
  }

  public void setDocumentBoost(float documentBoost) {
    _documentBoost = documentBoost;
  }
  
  @Override
  public String toString()
  {
    return "SolrInputDocument" + _fields.values();
  }
  
  public SolrInputDocument deepCopy() {
    SolrInputDocument clone = new SolrInputDocument();
    Set<Entry<String,SolrInputField>> entries = _fields.entrySet();
    for (Map.Entry<String,SolrInputField> fieldEntry : entries) {
      clone._fields.put(fieldEntry.getKey(), fieldEntry.getValue().deepCopy());
    }
    clone._documentBoost = _documentBoost;
    return clone;
  }

  //---------------------------------------------------
  // MAP interface
  //---------------------------------------------------

  @Override
  public boolean containsKey(Object key) {
    return _fields.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return _fields.containsValue(value);
  }

  @Override
  public Set<Entry<String, SolrInputField>> entrySet() {
    return _fields.entrySet();
  }

  @Override
  public SolrInputField get(Object key) {
    return _fields.get(key);
  }

  @Override
  public boolean isEmpty() {
    return _fields.isEmpty();
  }

  @Override
  public Set<String> keySet() {
    return _fields.keySet();
  }

  @Override
  public SolrInputField put(String key, SolrInputField value) {
    return _fields.put(key, value);
  }

  @Override
  public void putAll(Map<? extends String, ? extends SolrInputField> t) {
    _fields.putAll( t );
  }

  @Override
  public SolrInputField remove(Object key) {
    return _fields.remove(key);
  }

  @Override
  public int size() {
    return _fields.size();
  }

  @Override
  public Collection<SolrInputField> values() {
    return _fields.values();
  }
}
