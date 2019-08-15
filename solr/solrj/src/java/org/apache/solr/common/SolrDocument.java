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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.util.NamedList;

import static org.apache.solr.common.util.ByteArrayUtf8CharSequence.convertCharSeq;


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
public class SolrDocument extends SolrDocumentBase<Object, SolrDocument> implements Iterable<Map.Entry<String, Object>>
{
  protected final Map<String,Object> _fields;
  
  private List<SolrDocument> _childDocuments;
  
  public SolrDocument()
  {
    _fields = new LinkedHashMap<>();
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    _fields.forEach(ew.getBiConsumer());
  }

  public SolrDocument(Map<String, Object> fields) {
    this._fields = fields;
  }

  /**
   * @return a list of field names defined in this document - this Collection is directly backed by this SolrDocument.
   * @see #keySet
   */
  @Override
  public Collection<String> getFieldNames() {
    return this.keySet();
  }

  ///////////////////////////////////////////////////////////////////
  // Add / Set / Remove Fields
  ///////////////////////////////////////////////////////////////////

  /**
   * Remove all fields from the document
   */
  @Override
  public void clear()
  {
    _fields.clear();

    if(_childDocuments != null) {
      _childDocuments.clear();
    }
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
    else if( value instanceof Iterable && !(value instanceof SolrDocumentBase)) {
      ArrayList<Object> lst = new ArrayList<>();
      for( Object o : (Iterable)value ) {
        lst.add( o );
      }
      value = lst;
    }
    _fields.put(name, value);
  }
  
  /**
   * This will add a field to the document.  If fields already exist with this
   * name it will append value to the collection. If the value is Collection,
   * each value will be added independently. 
   * 
   * The class type of value and the name parameter should match schema.xml. 
   * schema.xml can be found in conf directory under the solr home by default.
   * 
   * @param name Name of the field, should match one of the field names defined under "fields" tag in schema.xml.
   * @param value Value of the field, should be of same class type as defined by "type" attribute of the corresponding field in schema.xml. 
   */
  @SuppressWarnings("unchecked")
  @Override
  public void addField(String name, Object value) 
  { 
    Object existing = _fields.get(name);
    if (existing == null) {
      if( value instanceof Collection ) {
        Collection<Object> c = new ArrayList<>( 3 );
        for ( Object o : (Collection<Object>)value ) {
          c.add(o);
        }
        this.setField( name, c );
      } else {
        this.setField( name, value );
      }
      return;
    }
    
    Collection<Object> vals = null;
    if( existing instanceof Collection ) {
      vals = (Collection<Object>)existing;
    }
    else {
      vals = new ArrayList<>( 3 );
      vals.add( existing );
    }
    
    // Add the values to the collection
    if( value instanceof Iterable && !(value instanceof SolrDocumentBase)) {
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
  @Override
  public Object getFieldValue(String name) {
    return _fields.get( name );
  }

  /**
   * Get a collection of values for a given field name
   */
  @SuppressWarnings("unchecked")
  @Override
  public Collection<Object> getFieldValues(String name) {
    Object v = _fields.get( name );
    if( v instanceof Collection ) {
      return (Collection<Object>)v;
    }
    if( v != null ) {
      ArrayList<Object> arr = new ArrayList<>(1);
      arr.add( v );
      return arr;
    }
    return null;
  }
    
  @Override
  public String toString()
  {
    return "SolrDocument"+_fields;
  }

  /**
   * Iterate of String-&gt;Object keys
   */
  @Override
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
      @Override
      public Collection<Object> get(Object key) { 
        return getFieldValues( (String)key ); 
      }
      
      // Easily Supported methods
      @Override
      public boolean containsKey(Object key) { return _fields.containsKey( key ); }
      @Override
      public Set<String>  keySet()           { return _fields.keySet();  }
      @Override
      public int          size()             { return _fields.size();    }
      @Override
      public boolean      isEmpty()          { return _fields.isEmpty(); }

      // Unsupported operations.  These are not necessary for JSTL
      @Override
      public void clear() { throw new UnsupportedOperationException(); }
      @Override
      public boolean containsValue(Object value) {throw new UnsupportedOperationException();}
      @Override
      public Set<java.util.Map.Entry<String, Collection<Object>>> entrySet() {throw new UnsupportedOperationException();}
      @Override
      public void putAll(Map<? extends String, ? extends Collection<Object>> t) {throw new UnsupportedOperationException();}
      @Override
      public Collection<Collection<Object>> values() {throw new UnsupportedOperationException();}
      @Override
      public Collection<Object> put(String key, Collection<Object> value) {throw new UnsupportedOperationException();}
      @Override
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
      @Override
      public Object get(Object key) { 
        return convertCharSeq(getFirstValue( (String)key));
      }
      
      // Easily Supported methods
      @Override
      public boolean containsKey(Object key) { return _fields.containsKey( key ); }
      @Override
      public Set<String>  keySet()           { return (Set<String>) convertCharSeq(_fields.keySet());  }
      @Override
      public int          size()             { return _fields.size();    }
      @Override
      public boolean      isEmpty()          { return _fields.isEmpty(); }

      // Unsupported operations.  These are not necessary for JSTL
      @Override
      public void clear() { throw new UnsupportedOperationException(); }
      @Override
      public boolean containsValue(Object value) {throw new UnsupportedOperationException();}
      @Override
      public Set<java.util.Map.Entry<String, Object>> entrySet() {throw new UnsupportedOperationException();}
      @Override
      public void putAll(Map<? extends String, ? extends Object> t) {throw new UnsupportedOperationException();}
      @Override
      public Collection<Object> values() {throw new UnsupportedOperationException();}
      @Override
      public Collection<Object> put(String key, Object value) {throw new UnsupportedOperationException();}
      @Override
      public Collection<Object> remove(Object key) {throw new UnsupportedOperationException();}      
      @Override
      public String toString() {return _fields.toString();}
   };
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
  public Set<Entry<String, Object>> entrySet() {
    return _fields.entrySet();
  }
  //TODO: Shouldn't the input parameter here be a String?  The _fields map requires a String.
  @Override
  public Object get(Object key) {
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
  public Object put(String key, Object value) {
    return _fields.put(key, value);
  }

  @Override
  public void putAll(Map<? extends String, ? extends Object> t) {
    _fields.putAll( t );
  }

  @Override
  public Object remove(Object key) {
    return convertCharSeq(_fields.remove(key));
  }

  @Override
  public int size() {
    return _fields.size();
  }

  @Override
  public Collection<Object> values() {
    return convertCharSeq(_fields.values());
  }

  @Override
  public void addChildDocument(SolrDocument child) {
    if (_childDocuments == null) {
      _childDocuments = new ArrayList<>();
    }
     _childDocuments.add(child);
   }
   
  @Override
   public void addChildDocuments(Collection<SolrDocument> children) {
     for (SolrDocument child : children) {
       addChildDocument(child);
     }
   }

   @Override
   public List<SolrDocument> getChildDocuments() {
     return _childDocuments;
   }
   
   @Override
   public boolean hasChildDocuments() {
     boolean isEmpty = (_childDocuments == null || _childDocuments.isEmpty());
     return !isEmpty;
   }

  @Override
  
  @Deprecated
  public int getChildDocumentCount() {
    if (_childDocuments == null) return 0;
    return _childDocuments.size();
  }
}
