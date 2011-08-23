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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.util.NamedList;


/**
 * A concrete representation of a document within a Solr index.  Unlike a lucene
 * Document, a SolrDocument may have an Object value matching the type defined in
 * schema.xml
 * 
 * For indexing documents, use the SolrInputDocument that contains extra information
 * for document and field boosting.
 * 
 *
 * @since solr 1.3
 */
public class SolrDocument implements Map<String,Object>, Iterable<Map.Entry<String, Object>>, Serializable
{
  private final Map<String,Object> _fields;
  
  public SolrDocument()
  {
    _fields = new LinkedHashMap<String,Object>();
  }

  /**
   * @return a list of field names defined in this document - this Collection is directly backed by this SolrDocument.
   * @see #keySet
   */
  public Collection<String> getFieldNames() {
    return this.keySet();
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
    return this.remove( name ) != null;
  }

  /**
   * Set a field with the given object.  If the object is an Array, it will 
   * set multiple fields with the included contents.  This will replace any existing 
   * field with the given name
   */
  @SuppressWarnings("unchecked")
  public void setField(String name, Object value) 
  {
    if( value instanceof Object[] ) {
      value = new ArrayList(Arrays.asList( (Object[])value ));
    }
    else if( value instanceof Collection ) {
      // nothing
    }
    else if( value instanceof NamedList ) {
      // nothing
    }
    else if( value instanceof Iterable ) {
      ArrayList<Object> lst = new ArrayList<Object>();
      for( Object o : (Iterable)value ) {
        lst.add( o );
      }
      value = lst;
    }
    _fields.put(name, value);
  }
  
  /**
   * This will add a field to the document.  If fields already exist with this name
   * it will append the collection
   */
  @SuppressWarnings("unchecked")
  public void addField(String name, Object value) 
  { 
    Object existing = _fields.get(name);
    if (existing == null) {
      this.setField( name, value );
      return;
    }
    
    Collection<Object> vals = null;
    if( existing instanceof Collection ) {
      vals = (Collection<Object>)existing;
    }
    else {
      vals = new ArrayList<Object>( 3 );
      vals.add( existing );
    }
    
    // Add the values to the collection
    if( value instanceof Iterable ) {
      for( Object o : (Iterable<Object>)value ) {
        vals.add( o );
      }
    }
    else if( value instanceof Object[] ) {
      for( Object o : (Object[])value ) {
        vals.add( o );
      }
    }
    else {
      vals.add( value );
    }
    _fields.put( name, vals );
  }

  ///////////////////////////////////////////////////////////////////
  // Get the field values
  ///////////////////////////////////////////////////////////////////

  /**
   * returns the first value for a field
   */
  public Object getFirstValue(String name) {
    Object v = _fields.get( name );
    if (v == null || !(v instanceof Collection)) return v;
    Collection c = (Collection)v;
    if (c.size() > 0 ) {
      return c.iterator().next();
    }
    return null;
  }
  
  /**
   * Get the value or collection of values for a given field.  
   */
  public Object getFieldValue(String name) {
    return _fields.get( name );
  }

  /**
   * Get a collection of values for a given field name
   */
  @SuppressWarnings("unchecked")
  public Collection<Object> getFieldValues(String name) {
    Object v = _fields.get( name );
    if( v instanceof Collection ) {
      return (Collection<Object>)v;
    }
    if( v != null ) {
      ArrayList<Object> arr = new ArrayList<Object>(1);
      arr.add( v );
      return arr;
    }
    return null;
  }
    
  @Override
  public String toString()
  {
    return "SolrDocument["+_fields.toString()+"]";
  }

  /**
   * Iterate of String->Object keys
   */
  public Iterator<Entry<String, Object>> iterator() {
    return _fields.entrySet().iterator();
  }
  
  //-----------------------------------------------------------------------------------------
  // JSTL Helpers
  //-----------------------------------------------------------------------------------------
  
  /**
   * Expose a Map interface to the solr field value collection.
   */
  public Map<String,Collection<Object>> getFieldValuesMap()
  {
    return new Map<String,Collection<Object>>() {
      /** Get the field Value */
      public Collection<Object> get(Object key) { 
        return getFieldValues( (String)key ); 
      }
      
      // Easily Supported methods
      public boolean containsKey(Object key) { return _fields.containsKey( key ); }
      public Set<String>  keySet()           { return _fields.keySet();  }
      public int          size()             { return _fields.size();    }
      public boolean      isEmpty()          { return _fields.isEmpty(); }

      // Unsupported operations.  These are not necessary for JSTL
      public void clear() { throw new UnsupportedOperationException(); }
      public boolean containsValue(Object value) {throw new UnsupportedOperationException();}
      public Set<java.util.Map.Entry<String, Collection<Object>>> entrySet() {throw new UnsupportedOperationException();}
      public void putAll(Map<? extends String, ? extends Collection<Object>> t) {throw new UnsupportedOperationException();}
      public Collection<Collection<Object>> values() {throw new UnsupportedOperationException();}
      public Collection<Object> put(String key, Collection<Object> value) {throw new UnsupportedOperationException();}
      public Collection<Object> remove(Object key) {throw new UnsupportedOperationException();}
      @Override
      public String toString() {return _fields.toString();}
    };
  }

  /**
   * Expose a Map interface to the solr fields.  This function is useful for JSTL
   */
  public Map<String,Object> getFieldValueMap() {
    return new Map<String,Object>() {
      /** Get the field Value */
      public Object get(Object key) { 
        return getFirstValue( (String)key ); 
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
      public Collection<Object> put(String key, Object value) {throw new UnsupportedOperationException();}
      public Collection<Object> remove(Object key) {throw new UnsupportedOperationException();}      
      @Override
      public String toString() {return _fields.toString();}
   };
  }

  //---------------------------------------------------
  // MAP interface
  //---------------------------------------------------

  public boolean containsKey(Object key) {
    return _fields.containsKey(key);
  }

  public boolean containsValue(Object value) {
    return _fields.containsValue(value);
  }

  public Set<Entry<String, Object>> entrySet() {
    return _fields.entrySet();
  }

  public Object get(Object key) {
    return _fields.get(key);
  }

  public boolean isEmpty() {
    return _fields.isEmpty();
  }

  public Set<String> keySet() {
    return _fields.keySet();
  }

  public Object put(String key, Object value) {
    return _fields.put(key, value);
  }

  public void putAll(Map<? extends String, ? extends Object> t) {
    _fields.putAll( t );
  }

  public Object remove(Object key) {
    return _fields.remove(key);
  }

  public int size() {
    return _fields.size();
  }

  public Collection<Object> values() {
    return _fields.values();
  }
}
