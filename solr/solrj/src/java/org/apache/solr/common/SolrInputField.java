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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static org.apache.solr.common.util.ByteArrayUtf8CharSequence.convertCharSeq;

/**
 *
 * @since solr 1.3
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SolrInputField implements Iterable<Object>, Serializable
{
  String name;
  Object value = null; 
  
  public SolrInputField( String n )
  {
    this.name = n;
  }

  //---------------------------------------------------------------
  //---------------------------------------------------------------

  /**
   * Set the value for a field.  Arrays will be converted to a collection. If
   * a collection is given, then that collection will be used as the backing
   * collection for the values.
   */
  public void setValue(Object v) {
    if( v instanceof Object[] ) {
      Object[] arr = (Object[])v;
      Collection<Object> c = new ArrayList<>( arr.length );
      for( Object o : arr ) {
        c.add( o );
      }
      value = c;
    }
    else {
      value = v;
    }
  }

  /**
   * Add values to a field.  If the added value is a collection, each value
   * will be added individually.
   */
  public void addValue(Object v) {
    if( value == null ) {
      if ( v instanceof Collection ) {
        Collection<Object> c = new ArrayList<>( 3 );
        for ( Object o : (Collection<Object>)v ) {
          c.add( o );
        }
        setValue(c);
      } else {
        setValue(v);
      }

      return;
    }
    
    Collection<Object> vals = null;
    if( value instanceof Collection ) {
      vals = (Collection<Object>)value;
    }
    else {
      vals = new ArrayList<>( 3 );
      vals.add( value );
      value = vals;
    }
    
    // Add the new values to a collection, if childDoc add as is without iteration
    if( v instanceof Iterable && !(v instanceof SolrDocumentBase)) {
      for( Object o : (Iterable<Object>)v ) {
        vals.add( o );
      }
    }
    else if( v instanceof Object[] ) {
      for( Object o : (Object[])v ) {
        vals.add( o );
      }
    }
    else {
      vals.add( v );
    }
  }

  //---------------------------------------------------------------
  //---------------------------------------------------------------
  
  public Object getFirstValue() {
    if( value instanceof Collection ) {
      Collection c = (Collection<Object>)value;
      if( c.size() > 0 ) {
        return convertCharSeq(c.iterator().next());
      }
      return null;
    }
    return convertCharSeq(value);
  }

  /**
   * @return the value for this field.  If the field has multiple values, this
   * will be a collection.
   */
  public Object getValue() {
    return convertCharSeq(value);
  }


  /**
   * Return a value as is without converting and CharSequence Objects
   */
  public Object getRawValue() {
    return value;
  }

  /**
   * Return the first value as is without converting and CharSequence Objects
   */
  public Object getFirstRawValue() {
    if (value instanceof Collection) {
      Collection c = (Collection<Object>) value;
      if (c.size() > 0) {
        return c.iterator().next();
      }
      return null;
    }
    return value;
  }

  /**
   * @return the values for this field.  This will return a collection even
   * if the field is not multi-valued
   */
  public Collection<Object> getValues() {
    if (value instanceof Collection) {
      return convertCharSeq((Collection<Object>) value);
    }
    if( value != null ) {
      Collection<Object> vals = new ArrayList<>(1);
      vals.add(convertCharSeq(value));
      return vals;
    }
    return null;
  }

  /**
   * @return the number of values for this field
   */
  public int getValueCount() {
    if( value instanceof Collection ) {
      return ((Collection)value).size();
    }
    return (value == null) ? 0 : 1;
  }
  
  //---------------------------------------------------------------
  //---------------------------------------------------------------

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public Iterator<Object> iterator(){
    if( value instanceof Collection ) {
      return (convertCharSeq ((Collection)value)).iterator();
    }
    return new Iterator<Object>() {
      boolean nxt = (value!=null);

      @Override
      public boolean hasNext() {
        return nxt;
      }

      @Override
      public Object next() {
        nxt = false;
        return convertCharSeq(value);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };

  }

  public Iterator<Object> getRawIterator() {
    if( value instanceof Collection ) {
      return ((Collection)value).iterator();
    }
    return new Iterator<Object>() {
      boolean nxt = (value!=null);
      
      @Override
      public boolean hasNext() {
        return nxt;
      }

      @Override
      public Object next() {
        nxt = false;
        return value;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public String toString()
  {
    return name + "=" + value;
  }

  public SolrInputField deepCopy() {
    SolrInputField clone = new SolrInputField(name);
    // We can't clone here, so we rely on simple primitives
    if (value instanceof Collection) {
      Collection<Object> values = (Collection<Object>) value;
      Collection<Object> cloneValues = new ArrayList<>(values.size());
      cloneValues.addAll(values);
      clone.value = cloneValues;
    } else {
      clone.value = value;
    }
    return clone;
  }
}
