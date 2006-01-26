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
 * @author yonik
 * @version $Id$
 */
//
// A quick hack of a class to represent a list of name-value pairs.
// Unlike a map, order is maintained, and names may
// be repeated.  Names and values may be null.
//
// In the future, it would be nice if this extended Map or Collection,
// had iterators, used java5 generics, had a faster lookup for
// large lists, etc...
// It could also have an interface, and multiple implementations.
// One might have indexed lookup, one might not.
//
public class NamedList implements Cloneable, Serializable {
  protected final List nvPairs;

  public NamedList() {
    nvPairs = new ArrayList();
  }

  public NamedList(List nameValuePairs) {
    nvPairs=nameValuePairs;
  }

  public int size() {
    return nvPairs.size() >> 1;
  }

  public String getName(int idx) {
    return (String)nvPairs.get(idx << 1);
  }

  public Object getVal(int idx) {
    return nvPairs.get((idx << 1) + 1);
  }

  public void add(String name, Object val) {
    nvPairs.add(name);
    nvPairs.add(val);
  }

  public void setName(int idx, String name) {
    nvPairs.set(idx<<1, name);
  }

  public void setVal(int idx, Object val) {
    nvPairs.set((idx<<1)+1, val);
  }

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


  // gets the value for the first specified name. returns null if not
  // found or if the value stored was null.
  public Object get(String name) {
    return get(name,0);
  }

  // gets the value for the first specified name starting start.
  // returns null if not found or if the value stored was null.
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


  public boolean addAll(Map args) {
    Set eset = args.entrySet();
    Iterator iter = eset.iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry)iter.next();
      add(entry.getKey().toString(), entry.getValue());
    }
    return false;
  }

  /**
   * Makes a *shallow copy* of the named list.
   */
  public NamedList clone() {
    ArrayList newList = new ArrayList(nvPairs.size());
    newList.addAll(nvPairs);
    return new NamedList(newList);
  }

}
