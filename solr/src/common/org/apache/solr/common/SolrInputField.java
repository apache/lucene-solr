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
import java.util.Collection;
import java.util.Iterator;

/**
 *
 * @since solr 1.3
 */
public class SolrInputField implements Iterable<Object>, Serializable
{
  String name;
  Object value = null; 
  float boost = 1.0f;
  
  public SolrInputField( String n )
  {
    this.name = n;
  }

  //---------------------------------------------------------------
  //---------------------------------------------------------------

  /**
   * Set the value for a field.  Arrays will be converted to a collection.
   */
  public void setValue(Object v, float b) {
    boost = b;

    if( v instanceof Object[] ) {
      Object[] arr = (Object[])v;
      Collection<Object> c = new ArrayList<Object>( arr.length );
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
   * Add values to a field.  if the added value is a collection, each value
   * will be added individually
   */
  @SuppressWarnings("unchecked")
  public void addValue(Object v, float b) {
    if( value == null ) {
      setValue(v, b);
      return;
    }
    
    // The lucene API and solr XML field specification make it possible to set boosts
    // on multi-value fields even though lucene indexing does not support this.
    // To keep behavior consistent with what happens in the lucene index, we accumulate
    // the product of all boosts specified for this field.
    boost *= b;
    
    Collection<Object> vals = null;
    if( value instanceof Collection ) {
      vals = (Collection<Object>)value;
    }
    else {
      vals = new ArrayList<Object>( 3 );
      vals.add( value );
      value = vals;
    }
    
    // Add the new values to a collection
    if( v instanceof Iterable ) {
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
  
  @SuppressWarnings("unchecked")
  public Object getFirstValue() {
    if( value instanceof Collection ) {
      Collection c = (Collection<Object>)value;
      if( c.size() > 0 ) {
        return c.iterator().next();
      }
      return null;
    }
    return value;
  }

  /**
   * @return the value for this field.  If the field has multiple values, this
   * will be a collection.
   */
  public Object getValue() {
    return value;
  }

  /**
   * @return the values for this field.  This will return a collection even
   * if the field is not multi-valued
   */
  @SuppressWarnings("unchecked")
  public Collection<Object> getValues() {
    if( value instanceof Collection ) {
      return (Collection<Object>)value;
    }
    if( value != null ) {
      Collection<Object> vals = new ArrayList<Object>(1);
      vals.add( value );
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
  
  public float getBoost() {
    return boost;
  }

  public void setBoost(float boost) {
    this.boost = boost;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @SuppressWarnings("unchecked")
  public Iterator<Object> iterator() {
    if( value instanceof Collection ) {
      return ((Collection)value).iterator();
    }
    return new Iterator<Object>() {
      boolean nxt = (value!=null);
      
      public boolean hasNext() {
        return nxt;
      }

      public Object next() {
        nxt = false;
        return value;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public String toString()
  {
    return name + "("+boost+")={" + value + "}";
  }
}
