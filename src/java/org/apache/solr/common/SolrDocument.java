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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * A concrete representation of a document within a Solr index.  Unlike a lucene
 * Document, a SolrDocument may have an Object value matching the type defined in
 * schema.xml
 * 
 * For indexing documents, use the SolrInputDocumet that contains extra information
 * for document and field boosting.
 * 
 * @author ryan
 * @version $Id$
 * @since solr 1.3
 */
public class SolrDocument 
{
  private Map<String,Collection<Object>> _fields = null;
  
  public SolrDocument()
  {
    _fields = new HashMap<String,Collection<Object>>();
  }

  /**
   * Let sub classes return something other then a List.  
   * Perhaps a Set or LinkedHashSet
   */
  protected Collection<Object> getEmptyCollection( String name )
  {
    return new ArrayList<Object>();
  }

  /**
   * @return a list of fields defined in this document
   */
  public Collection<String> getFieldNames() {
    return _fields.keySet();
  }

  ///////////////////////////////////////////////////////////////////
  // Add / Set / Remove Fields
  ///////////////////////////////////////////////////////////////////

  /**
   * Remove all fields from the document
   */
  public void clear()
  {
    _fields.clear();
  }
  
  
  /**
   * Remove all fields with the name
   */
  public boolean removeFields(String name) 
  {
    return _fields.remove( name ) != null;
  }

  /**
   * Set a field with the given object.  If the object is an Array or Iterable, it will 
   * set multiple fields with the included contents.  This will replace any existing 
   * field with the given name
   */
  public void setField(String name, Object value) 
  {
    Collection<Object> existing = _fields.get( name );
    if( existing != null ) {
      existing.clear();
    }
    this.addField(name, value);
  }

  /**
   * This will add a field to the document.  If fields already exist with this name
   * it will append the collection
   */
  public void addField(String name, Object value) 
  { 
    Collection<Object> existing = _fields.get( name );
    if( existing == null ) {
      existing = getEmptyCollection(name);
      _fields.put( name, existing );
    }
    
    // Arrays are iterable?  They appear to be, but not in the docs...
    if( value instanceof Iterable ) {
      for( Object o : (Iterable)value ) {
        this.addField( name, o );  
      }
    }
    else if( value instanceof Object[] ) {
      for( Object o : (Object[])value ) {
        this.addField( name, o );  
      }
    }
    else {
      existing.add( value );
    }
  }

  ///////////////////////////////////////////////////////////////////
  // Get the field values
  ///////////////////////////////////////////////////////////////////

  /**
   * returns the first value for this field
   */
  public Object getFieldValue(String name) {
    Collection v = _fields.get( name );
    if( v != null && v.size() > 0 ) {
      return v.iterator().next();
    }
    return null;
  }

  /**
   * Get a collection or all the values for a given field name
   */
  public Collection<Object> getFieldValues(String name) {
    return _fields.get( name );
  }
  
// TODO? should this be in the API?
//  /**
//   * Return a named list version
//   */
//  public NamedList<Object> toNamedList()
//  {
//    NamedList<Object> nl = new NamedList<Object>();
//    for( Map.Entry<String, Collection<Object>> entry : _fields.entrySet() ) {
//      Collection<Object> v = entry.getValue();
//      if( v.size() == 0 ) {
//        nl.add( entry.getKey(), null );
//      }
//      else if( v.size() > 1 ) {
//        nl.add( entry.getKey(), v );
//      }
//      else { // Add a single value
//        nl.add( entry.getKey(), v.iterator().next() );
//      }
//    }
//    return nl;
//  }
  
  @Override
  public String toString()
  {
    return "SolrDocument["+getFieldNames()+"]";
  }
  
  /**
   * Expose a Map interface to the solr field value collection.
   */
  public Map<String,Collection<Object>> getFieldValuesMap()
  {
    return _fields;
  }

  /**
   * Expose a Map interface to the solr fields.  This function is useful for JSTL
   */
  public Map<String,Object> getFieldValueMap() {
    return new Map<String,Object>() {
      /** Get the field Value */
      public Object get(Object key) { 
        return getFieldValue( (String)key ); 
      }
      
      /** Set the field Value */
      public Object put(String key, Object value) {
        setField( key, value );
        return null;
      }

      /** Remove the field Value */
      public Object remove(Object key) {
        removeFields( (String)key ); 
        return null;
      }
      
      // Easily Supported methods
      public boolean containsKey(Object key) { return _fields.containsKey( key ); }
      public Set<String>  keySet()           { return _fields.keySet();  }
      public int          size()             { return _fields.size();    }
      public boolean      isEmpty()          { return _fields.isEmpty(); }

      // Unsupported operations.  These are not necessary for JSTL
      public void clear() { throw new UnsupportedOperationException(); }
      public boolean containsValue(Object value) {throw new UnsupportedOperationException();}
      public Set<java.util.Map.Entry<String, Object>> entrySet() {throw new UnsupportedOperationException();}
      public void putAll(Map<? extends String, ? extends Object> t) {throw new UnsupportedOperationException();}
      public Collection<Object> values() {throw new UnsupportedOperationException();}
    };
  }
}
