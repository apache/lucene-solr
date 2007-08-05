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

package org.apache.solr.common;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Collection;

/**
 * Represent the field and boost information needed to construct and index
 * a Lucene Document.  Like the SolrDocument, the field values should
 * match those specified in schema.xml 
 *
 * @version $Id$
 * @since solr 1.3
 */
public class SolrInputDocument implements Iterable<SolrInputField>
{
  private final Map<String,SolrInputField> _fields;
  private float _documentBoost = 1.0f;

  public SolrInputDocument()
  {
    _fields = new HashMap<String,SolrInputField>();
  }
  
  /**
   * Remove all fields and boosts from the document
   */
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
   * @see addField(String, Object, Float)
   * @param name name of the field to add
   * @param value value of the field
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
   * @see setField(String, Object, Float)
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
   * Remove all fields and boosts from the document
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
   * @param key The field name whose field is to be removed from the document
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
    return "SolrInputDocumnt["+_fields+"]";
  }
}
