/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.util;

import java.util.*;
import java.io.Serializable;

/**
 * A simple container class for modeling an ordered list of name/value pairs.
 *
 * <p>
 * Unlike Maps:
 * </p>
 * <ul>
 *  <li>Names may be repeated</li>
 *  <li>Order of elements is maintained</li>
 *  <li>Elements may be accessed by numeric index</li>
 *  <li>Names and Values can both be null</li>
 * </ul>
 *
 * <p>
 * :TODO: In the future, it would be nice if this extended Map or Collection,
 * had iterators, used java5 generics, had a faster lookup for
 * large lists, etc...
 * It could also have an interface, and multiple implementations.
 * One might have indexed lookup, one might not.
 * </p>
 *
 * @author yonik
 * @version $Id$
 */
public class NamedList implements Cloneable, Serializable {
  protected final List nvPairs;

  /** Creates an empty instance */
  public NamedList() {
    nvPairs = new ArrayList();
  }

  /**
   * Creates an instance backed by an explicitly specified list of
   * pairwise names/values.
   *
   * @param nameValuePairs underlying List which should be used to implement a NamedList; modifying this List will affect the NamedList.
   */
  public NamedList(List nameValuePairs) {
    nvPairs=nameValuePairs;
  }

  /** The total number of name/value pairs */
  public int size() {
    return nvPairs.size() >> 1;
  }

  /**
   * The name of the pair at the specified List index
   *
   * @return null if no name exists
   */
  public String getName(int idx) {
    return (String)nvPairs.get(idx << 1);
  }

  /**
   * The value of the pair at the specified List index
   *
   * @return may be null
   */
  public Object getVal(int idx) {
    return nvPairs.get((idx << 1) + 1);
  }
  
  /**
   * Adds a name/value pair to the end of the list.
   */
  public void add(String name, Object val) {
    nvPairs.add(name);
    nvPairs.add(val);
  }

  /**
   * Modifies the name of the pair at the specified index.
   */
  public void setName(int idx, String name) {
    nvPairs.set(idx<<1, name);
  }

  /**
   * Modifies the value of the pair at the specified index.
   */
  public void setVal(int idx, Object val) {
    nvPairs.set((idx<<1)+1, val);
  }

  /**
   * Scans the list sequentially beginning at the specified index and
   * returns the index of the first pair with the specified name.
   *
   * @param name name to look for, may be null
   * @param start index to begin searching from
   * @return The index of the first matching pair, -1 if no match
   */
  public int indexOf(String name, int start) {
    int sz = size();
    for (int i=start; i<sz; i++) {
      String n = getName(i);
      if (name==null) {
        if (n==null) return i; // matched null
      } else if (name.equals(n)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Gets the value for the first instance of the specified name
   * found.
   * 
   * @return null if not found or if the value stored was null.
   * @see #indexOf
   * @see #get(String,int)
   */
  public Object get(String name) {
    return get(name,0);
  }

  /**
   * Gets the value for the first instance of the specified name
   * found starting at the specified index.
   * 
   * @return null if not found or if the value stored was null.
   * @see #indexOf
   */
  public Object get(String name, int start) {
    int sz = size();
    for (int i=start; i<sz; i++) {
      String n = getName(i);
      if (name==null) {
        if (n==null) return getVal(i);
      } else if (name.equals(n)) {
        return getVal(i);
      }
    }
    return null;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append('{');
    int sz = size();
    for (int i=0; i<sz; i++) {
      if (i != 0) sb.append(',');
      sb.append(getName(i));
      sb.append('=');
      sb.append(getVal(i));
    }
    sb.append('}');

    return sb.toString();
  }

  /**
   * Iterates over the Map and sequentially adds it's key/value pairs
   */
  public boolean addAll(Map args) {
    Set eset = args.entrySet();
    Iterator iter = eset.iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry)iter.next();
      add(entry.getKey().toString(), entry.getValue());
    }
    return args.size()>0;
  }

  /** Appends the elements of the given NamedList to this one. */
  public boolean addAll(NamedList nl) {
    nvPairs.addAll(nl.nvPairs);
    return nl.size()>0;
  }

  /**
   * Makes a <i>shallow copy</i> of the named list.
   */
  public NamedList clone() {
    ArrayList newList = new ArrayList(nvPairs.size());
    newList.addAll(nvPairs);
    return new NamedList(newList);
  }

}
