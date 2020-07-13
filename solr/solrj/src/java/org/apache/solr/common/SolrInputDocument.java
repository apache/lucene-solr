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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.solr.common.params.CommonParams;

/**
 * Represent the field-value information needed to construct and index
 * a Lucene Document.  Like the SolrDocument, the field values should
 * match those specified in schema.xml
 *
 *
 * @since solr 1.3
 */
public class SolrInputDocument extends SolrDocumentBase<SolrInputField, SolrInputDocument> implements Iterable<SolrInputField>
{
  private final Map<String,SolrInputField> _fields;
  private List<SolrInputDocument> _childDocuments;

  public SolrInputDocument(String... fields) {
    _fields = new LinkedHashMap<>();
    assert fields.length % 2 == 0;
    for (int i = 0; i < fields.length; i += 2) {
      addField(fields[i], fields[i + 1]);
    }
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    BiConsumer<CharSequence, Object> bc = ew.getBiConsumer();
    BiConsumer<CharSequence, Object> wrapper = (k, o) -> {
      if (o instanceof SolrInputField) {
        o = ((SolrInputField) o).getValue();
      }
      bc.accept(k, o);
    };
    _fields.forEach(wrapper);
    if (_childDocuments != null) {
      ew.put(CommonParams.CHILDDOC, _childDocuments);
    }
  }

  public SolrInputDocument(Map<String,SolrInputField> fields) {
    _fields = fields;
  }
  
  /**
   * Remove all fields from the document
   */
  @Override
  public void clear()
  {
    if( _fields != null ) {
      _fields.clear();
    }
    _childDocuments = null;
  }

  ///////////////////////////////////////////////////////////////////
  // Add / Set fields
  ///////////////////////////////////////////////////////////////////

  /** 
   * Add a field value to any existing values that may or may not exist.
   * 
   * The class type of value and the name parameter should match schema.xml. 
   * schema.xml can be found in conf directory under the solr home by default.
   * 
   * @param name Name of the field, should match one of the field names defined under "fields" tag in schema.xml.
   * @param value Value of the field, should be of same class type as defined by "type" attribute of the corresponding field in schema.xml. 
   */
  public void addField(String name, Object value) 
  {
    SolrInputField field = _fields.get( name );
    if( field == null || field.value == null ) {
      setField(name, value);
    }
    else {
      field.addValue( value );
    }
  }
  
  /** Get the first value for a field.
   * 
   * @param name name of the field to fetch
   * @return first value of the field or null if not present
   */
  @Override
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
  @Override
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
  @Override
  public Collection<String> getFieldNames()
  {
    return _fields.keySet();
  }

  /** Set a field value; replacing the existing value if present.
   *
   * @param name name of the field to set
   * @param value value of the field
   */
  public void setField(String name, Object value )
  {
    SolrInputField field = new SolrInputField( name );
    _fields.put( name, field );
    field.setValue( value );
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

  @Override
  public String toString()
  {
    return "SolrInputDocument(fields: " + _fields.values()
        + ( _childDocuments == null ? "" : (", children: " + _childDocuments) )
        + ")";
  }

  public SolrInputDocument deepCopy() {
    SolrInputDocument clone = new SolrInputDocument();
    Set<Entry<String,SolrInputField>> entries = _fields.entrySet();
    for (Map.Entry<String,SolrInputField> fieldEntry : entries) {
      clone._fields.put(fieldEntry.getKey(), fieldEntry.getValue().deepCopy());
    }

    if (_childDocuments != null) {
      clone._childDocuments = new ArrayList<>(_childDocuments.size());
      for (SolrInputDocument child : _childDocuments) {
        clone._childDocuments.add(child.deepCopy());
      }
    }

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

  @Override
  public void addChildDocument(SolrInputDocument child) {
   if (_childDocuments == null) {
     _childDocuments = new ArrayList<>();
   }
    _childDocuments.add(child);
  }

  public void addChildDocuments(Collection<SolrInputDocument> children) {
    for (SolrInputDocument child : children) {
      addChildDocument(child);
    }
  }

  /** Returns the list of child documents, or null if none. */
  public List<SolrInputDocument> getChildDocuments() {
    return _childDocuments;
  }

  public boolean hasChildDocuments() {
    boolean isEmpty = (_childDocuments == null || _childDocuments.isEmpty());
    return !isEmpty;
  }

  @Override
  @Deprecated
  public int getChildDocumentCount() {
    return hasChildDocuments() ? _childDocuments.size(): 0;
  }
}
