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
package org.apache.solr.common.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.solr.cluster.api.SimpleMap;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;

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
 * A NamedList provides fast access by element number, but not by name.
 * </p>
 * <p>
 * When a NamedList is serialized, order is considered more important than access
 * by key, so ResponseWriters that output to a format such as JSON will normally
 * choose a data structure that allows order to be easily preserved in various
 * clients (i.e. not a straight map).
 * If access by key is more important for serialization, see {@link SimpleOrderedMap},
 * or simply use a regular {@link Map}
 * </p>
 *
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class NamedList<T> implements Cloneable, Serializable, Iterable<Map.Entry<String,T>> , MapWriter, SimpleMap<T> {

  private static final long serialVersionUID = 1957981902839867821L;
  protected final List<Object> nvPairs;

  /** Creates an empty instance */
  public NamedList() {
    nvPairs = new ArrayList<>();
  }


  public NamedList(int sz) {
    nvPairs = new ArrayList<>(sz<<1);
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    for (int i = 0; i < nvPairs.size(); i+=2) {
      ew.put((CharSequence) nvPairs.get(i), nvPairs.get(i + 1));
    }
  }

  /**
   * Creates a NamedList instance containing the "name,value" pairs contained in the
   * Entry[].
   *
   * <p>
   * Modifying the contents of the Entry[] after calling this constructor may change
   * the NamedList (in future versions of Solr), but this is not guaranteed and should
   * not be relied upon.  To modify the NamedList, refer to {@link #add(String, Object)}
   * or {@link #remove(String)}.
   * </p>
   *
   * @param nameValuePairs the name value pairs
   */
  public NamedList(Map.Entry<String, ? extends T>[] nameValuePairs) {
    nvPairs = nameValueMapToList(nameValuePairs);
  }

  /**
   * Creates a NamedList instance containing the "name,value" pairs contained in the
   * Map.
   *
   * <p>
   * Modifying the contents of the Map after calling this constructor may change
   * the NamedList (in future versions of Solr), but this is not guaranteed and should
   * not be relied upon.  To modify the NamedList, refer to {@link #add(String, Object)}
   * or {@link #remove(String)}.
   * </p>
   *
   * @param nameValueMap the name value pairs
   */
  public NamedList(Map<String,? extends T> nameValueMap) {
    if (null == nameValueMap) {
      nvPairs = new ArrayList<>();
    } else {
      nvPairs = new ArrayList<>(nameValueMap.size() << 1);
      for (Map.Entry<String,? extends T> ent : nameValueMap.entrySet()) {
        nvPairs.add(ent.getKey());
        nvPairs.add(ent.getValue());
      }
    }
  }

  /**
   * Creates an instance backed by an explicitly specified list of
   * pairwise names/values.
   *
   * <p>
   * When using this constructor, runtime type safety is only guaranteed if
   * all even numbered elements of the input list are of type "T".
   * </p>
   * <p>
   * This method is package protected and exists solely so SimpleOrderedMap and clone() can utilize it
   * </p>
   * <p>
   * TODO: this method was formerly public, now that it's not we can change the impl details of 
   * this class to be based on a Map.Entry[] 
   * </p>
   * @lucene.internal
   * @see #nameValueMapToList
   */
  NamedList(List<Object> nameValuePairs) {
    nvPairs=nameValuePairs;
  }

  /**
   * Method to serialize Map.Entry&lt;String, ?&gt; to a List in which the even
   * indexed elements (0,2,4. ..etc) are Strings and odd elements (1,3,5,) are of
   * the type "T".
   *
   * <p>
   * NOTE: This a temporary placeholder method until the guts of the class
   * are actually replaced by List&lt;String, ?&gt;.
   * </p>
   *
   * @return Modified List as per the above description
   * @see <a href="https://issues.apache.org/jira/browse/SOLR-912">SOLR-912</a>
   */
  private List<Object> nameValueMapToList(Map.Entry<String, ? extends T>[] nameValuePairs) {
    List<Object> result = new ArrayList<>(nameValuePairs.length << 1);
    for (Map.Entry<String, ?> ent : nameValuePairs) {
      result.add(ent.getKey());
      result.add(ent.getValue());
    }
    return result;
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
  @SuppressWarnings("unchecked")
  public T getVal(int idx) {
    return (T)nvPairs.get((idx << 1) + 1);
  }

  /**
   * Adds a name/value pair to the end of the list.
   */
  public void add(String name, T val) {
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
   *
   * @return the value that used to be at index
   */
  public T setVal(int idx, T val) {
    int index = (idx<<1)+1;
    @SuppressWarnings("unchecked")
    T old = (T)nvPairs.get( index );
    nvPairs.set(index, val);
    return old;
  }

  /**
   * Removes the name/value pair at the specified index.
   *
   * @return the value at the index removed
   */
  public T remove(int idx) {
    int index = (idx<<1);
    nvPairs.remove(index);
    @SuppressWarnings("unchecked")
    T result = (T)nvPairs.remove(index);  // same index, as things shifted in previous remove
    return result;
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
   * <p>
   * NOTE: this runs in linear time (it scans starting at the
   * beginning of the list until it finds the first pair with
   * the specified name).
   *
   * @return null if not found or if the value stored was null.
   * @see #indexOf
   * @see #get(String,int)
   *
   */
  public T get(String name) {
    return get(name,0);
  }

  /**
   * Gets the value for the first instance of the specified name
   * found starting at the specified index.
   * <p>
   * NOTE: this runs in linear time (it scans starting at the
   * specified position until it finds the first pair with
   * the specified name).
   *
   * @return null if not found or if the value stored was null.
   * @see #indexOf
   */
  public T get(String name, int start) {
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

  /**
   * Gets the values for the the specified name
   *
   * @param name Name
   * @return List of values
   */
  public List<T> getAll(String name) {
    List<T> result = new ArrayList<>();
    int sz = size();
    for (int i = 0; i < sz; i++) {
      String n = getName(i);
      if (Objects.equals(name, n)) {
        result.add(getVal(i));
      }
    }
    return result;
  }
  
  /**
   * Removes all values matching the specified name
   *
   * @param name Name
   */
  private void killAll(String name) {
    int sz = size();
    // Go through the list backwards, removing matches as found.
    for (int i = sz - 1; i >= 0; i--) {
      String n = getName(i);
      if (Objects.equals(name, n)) {
        remove(i);
      }
    }
  }
  
  /**
   * Recursively parses the NamedList structure to arrive at a specific element.
   * As you descend the NamedList tree, the last element can be any type,
   * including NamedList, but the previous elements MUST be NamedList objects
   * themselves. A null value is returned if the indicated hierarchy doesn't
   * exist, but NamedList allows null values so that could be the actual value
   * at the end of the path.
   * 
   * This method is particularly useful for parsing the response from Solr's
   * /admin/mbeans handler, but it also works for any complex structure.
   * 
   * Explicitly casting the return value is recommended. An even safer option is
   * to accept the return value as an object and then check its type.
   * 
   * Usage examples:
   * 
   * String coreName = (String) response.findRecursive
   * ("solr-mbeans", "CORE", "core", "stats", "coreName");
   * long numDoc = (long) response.findRecursive
   * ("solr-mbeans", "CORE", "searcher", "stats", "numDocs");
   * 
   * @param args
   *          One or more strings specifying the tree to navigate.
   * @return the last entry in the given path hierarchy, null if not found.
   */
  public Object findRecursive(String... args) {
    NamedList<?> currentList = null;
    Object value = null;
    for (int i = 0; i < args.length; i++) {
      String key = args[i];
      /*
       * The first time through the loop, the current list is null, so we assign
       * it to this list. Then we retrieve the first key from this list and
       * assign it to value.
       * 
       * On the next loop, we check whether the retrieved value is a NamedList.
       * If it is, then we drop down to that NamedList, grab the value of the
       * next key, and start the loop over. If it is not a NamedList, then we
       * assign the value to null and break out of the loop.
       * 
       * Assigning the value to null and then breaking out of the loop seems
       * like the wrong thing to do, but there's a very simple reason that it
       * works: If we have reached the last key, then the loop ends naturally
       * after we retrieve the value, and that code is never executed.
       */
      if (currentList == null) {
        currentList = this;
      } else {
        if (value instanceof NamedList) {
          currentList = (NamedList<?>) value;
        } else {
          value = null;
          break;
        }
      }
      /*
       * We do not need to do a null check on currentList for the following
       * assignment. The instanceof check above will fail if the current list is
       * null, and if that happens, the loop will end before this point.
       */
      value = currentList.get(key, 0);
    }
    return value;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
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

  public NamedList<T> getImmutableCopy() {
    NamedList<T> copy = clone();
    return new NamedList<>( Collections.unmodifiableList(copy.nvPairs));
  }

  public Map<String,T> asShallowMap() {
    return asShallowMap(false);
  }
  public Map<String,T> asShallowMap(boolean allowDps) {
    return new Map<String, T>() {
      @Override
      public int size() {
        return NamedList.this.size();
      }

      @Override
      public boolean isEmpty() {
        return size() == 0;
      }

      public boolean containsKey(Object  key) {
        return NamedList.this.get((String) key) != null ;
      }

      @Override
      public boolean containsValue(Object value) {
        return false;
      }

      @Override
      public T get(Object key) {
        return  NamedList.this.get((String) key);
      }

      @Override
      public T put(String  key, T value) {
        if (allowDps) {
          NamedList.this.add(key, value);
          return null;
        }
        int idx = NamedList.this.indexOf(key, 0);
        if (idx == -1) {
          NamedList.this.add(key, value);
        } else {
          NamedList.this.setVal(idx, value);
        }
        return null;
      }

      @Override
      public T remove(Object key) {
        return  NamedList.this.remove((String) key);
      }

      @Override
      @SuppressWarnings({"unchecked"})
      public void putAll(Map m) {
        boolean isEmpty = isEmpty();
        for (Object o : m.entrySet()) {
          @SuppressWarnings({"rawtypes"})
          Map.Entry e = (Entry) o;
          if (isEmpty) {// we know that there are no duplicates
            add((String) e.getKey(), (T) e.getValue());
          } else {
            put(e.getKey() == null ? null : e.getKey().toString(), (T) e.getValue());
          }
        }
      }

      @Override
      public void clear() {
        NamedList.this.clear();
      }

      @Override
      @SuppressWarnings({"unchecked"})
      public Set<String> keySet() {
        //TODO implement more efficiently
        return  NamedList.this.asMap(1).keySet();
      }

      @Override
      @SuppressWarnings({"unchecked", "rawtypes"})
      public Collection values() {
        //TODO implement more efficiently
        return  NamedList.this.asMap(1).values();
      }

      @Override
      public Set<Entry<String,T>> entrySet() {
        //TODO implement more efficiently
        return NamedList.this.asMap(1).entrySet();
      }

      @Override
      public void forEach(BiConsumer action) {
        NamedList.this.forEachEntry(action);
      }
    };
  }

  public Map asMap(int maxDepth) {
    LinkedHashMap result = new LinkedHashMap();
    for(int i=0;i<size();i++){
      Object val = getVal(i);
      if (val instanceof NamedList && maxDepth> 0) {
        //the maxDepth check is to avoid stack overflow due to infinite recursion
        val = ((NamedList) val).asMap(maxDepth-1);
      }
      Object old = result.put(getName(i), val);
      if(old!=null){
        if (old instanceof List) {
          List list = (List) old;
          list.add(val);
          result.put(getName(i),old);
        } else {
          ArrayList l = new ArrayList();
          l.add(old);
          l.add(val);
          result.put(getName(i), l);
        }
      }
    }
    return result;
  }
  /**
   * Create SolrParams from NamedList.  Values must be {@code String[]} or {@code List}
   * (with toString()-appropriate entries), or otherwise have a toString()-appropriate value.
   * Nulls are retained as such in arrays/lists but otherwise will NPE.
   */
  public SolrParams toSolrParams() {
    HashMap<String,String[]> map = new HashMap<>();
    for (int i=0; i<this.size(); i++) {
      String name = this.getName(i);
      Object val = this.getVal(i);
      if (val instanceof String[]) {
        MultiMapSolrParams.addParam(name, (String[]) val, map);
      } else if (val instanceof List) {
        List l = (List) val;
        String[] s = new String[l.size()];
        for (int j = 0; j < l.size(); j++) {
          s[j] = l.get(j) == null ? null : l.get(j).toString();
        }
        MultiMapSolrParams.addParam(name, s, map);
      } else {
        //TODO: we NPE if val is null; yet we support val members above. A bug?
        MultiMapSolrParams.addParam(name, val.toString(), map);
      }
    }
    // always use MultiMap for easier processing further down the chain
    return new MultiMapSolrParams(map);
  }

  /**
   * 
   * Helper class implementing Map.Entry&lt;String, T&gt; to store the key-value
   * relationship in NamedList (the keys of which are String-s)
   */
  public static final class NamedListEntry<T> implements Map.Entry<String,T> {
    
    public NamedListEntry() {

    }

    public NamedListEntry(String _key, T _value) {
      key = _key;
      value = _value;
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public T getValue() {
      return value;
    }

    @Override
    public T setValue(T _value) {
      T oldValue = value;
      value = _value;
      return oldValue;
    }

    private String key;

    private T value;
  }

  /**
   * Iterates over the Map and sequentially adds its key/value pairs
   */
  public boolean addAll(Map<String,T> args) {
    for (Map.Entry<String, T> entry : args.entrySet() ) {
      add(entry.getKey(), entry.getValue());
    }
    return args.size()>0;
  }

  /** Appends the elements of the given NamedList to this one. */
  public boolean addAll(NamedList<T> nl) {
    nvPairs.addAll(nl.nvPairs);
    return nl.size()>0;
  }

  /**
   * Makes a <i>shallow copy</i> of the named list.
   */
  @Override
  public NamedList<T> clone() {
    ArrayList<Object> newList = new ArrayList<>(nvPairs.size());
    newList.addAll(nvPairs);
    return new NamedList<>(newList);
  }

  //----------------------------------------------------------------------------
  // Iterable interface
  //----------------------------------------------------------------------------

  /**
   * Support the Iterable interface
   */
  @Override
  public Iterator<Map.Entry<String,T>> iterator() {

    final NamedList<T> list = this;

    Iterator<Map.Entry<String,T>> iter = new Iterator<Map.Entry<String,T>>() {

      int idx = 0;

      @Override
      public boolean hasNext() {
        return idx < list.size();
      }

      @Override
      public Map.Entry<String,T> next() {
        final int index = idx++;
        Map.Entry<String,T> nv = new Map.Entry<String,T>() {
          @Override
          public String getKey() {
            return list.getName( index );
          }

          @Override
          public T getValue() {
            return list.getVal( index );
          }

          @Override
          public String toString() {
            return getKey()+"="+getValue();
          }

          @Override
          public T setValue(T value) {
            return list.setVal(index, value);
          }
        };
        return nv;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
    return iter;
  }

  /**
   * NOTE: this runs in linear time (it scans starting at the
   * beginning of the list until it finds the first pair with
   * the specified name).
   */
  public T remove(String name) {
    int idx = indexOf(name, 0);
    if(idx != -1) return remove(idx);
    return null;
  }

  /**
   * Removes and returns all values for the specified name.  Returns null if
   * no matches found.  This method will return all matching objects,
   * regardless of data type.  If you are parsing Solr config options, the
   * {@link #removeConfigArgs(String)} or {@link #removeBooleanArg(String)}
   * methods will probably work better.
   *
   * @param name Name
   * @return List of values
   */
  public List<T> removeAll(String name) {
    List<T> result = getAll(name);
    if (result.size() > 0 ) {
      killAll(name);
      return result;
    }
    return null;
  }

  /**
   * Used for getting a boolean argument from a NamedList object.  If the name
   * is not present, returns null.  If there is more than one value with that
   * name, or if the value found is not a Boolean or a String, throws an
   * exception.  If there is only one value present and it is a Boolean or a
   * String, the value is removed and returned as a Boolean. If an exception
   * is thrown, the NamedList is not modified. See {@link #removeAll(String)}
   * and {@link #removeConfigArgs(String)} for additional ways of gathering
   * configuration information from a NamedList.
   * 
   * @param name
   *          The key to look up in the NamedList.
   * @return The boolean value found.
   * @throws SolrException
   *           If multiple values are found for the name or the value found is
   *           not a Boolean or a String.
   */
  public Boolean removeBooleanArg(final String name) {
    Boolean bool = getBooleanArg(name);
    if (null != bool) {
      remove(name);
    }
    return bool;
  }

  /**
   * Used for getting a boolean argument from a NamedList object.  If the name
   * is not present, returns null.  If there is more than one value with that
   * name, or if the value found is not a Boolean or a String, throws an
   * exception.  If there is only one value present and it is a Boolean or a
   * String, the value is returned as a Boolean.  The NamedList is not
   * modified. See {@link #remove(String)}, {@link #removeAll(String)}
   * and {@link #removeConfigArgs(String)} for additional ways of gathering
   * configuration information from a NamedList.
   *
   * @param name The key to look up in the NamedList.
   * @return The boolean value found.
   * @throws SolrException
   *           If multiple values are found for the name or the value found is
   *           not a Boolean or a String.
   */
  public Boolean getBooleanArg(final String name) {
    Boolean bool;
    List<T> values = getAll(name);
    if (0 == values.size()) {
      return null;
    }
    if (values.size() > 1) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Only one '" + name + "' is allowed");
    }
    Object o = get(name);
    if (o instanceof Boolean) {
      bool = (Boolean)o;
    } else if (o instanceof CharSequence) {
      bool = Boolean.parseBoolean(o.toString());
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "'" + name + "' must have type Boolean or CharSequence; found " + o.getClass());
    }
    return bool;
  }

  /**
   * Used for getting one or many arguments from NamedList objects that hold
   * configuration parameters. Finds all entries in the NamedList that match
   * the given name. If they are all strings or arrays of strings, remove them
   * from the NamedList and return the individual elements as a {@link Collection}.
   * Parameter order will be preserved if the returned collection is handled as
   * an {@link ArrayList}. Throws SolrException if any of the values associated
   * with the name are not strings or arrays of strings.  If exception is
   * thrown, the NamedList is not modified.  Returns an empty collection if no
   * matches found.  If you need to remove and retrieve all matching items from
   * the NamedList regardless of data type, use {@link #removeAll(String)} instead.
   * The {@link #removeBooleanArg(String)} method can be used for retrieving a
   * boolean argument.
   * 
   * @param name
   *          The key to look up in the NamedList.
   * @return A collection of the values found.
   * @throws SolrException
   *           If values are found for the input key that are not strings or
   *           arrays of strings.
   */
  public Collection<String> removeConfigArgs(final String name)
      throws SolrException {
    List<T> objects = getAll(name);
    List<String> collection = new ArrayList<>(size() / 2);
    final String err = "init arg '" + name + "' must be a string "
        + "(ie: 'str'), or an array (ie: 'arr') containing strings; found: ";
    
    for (Object o : objects) {
      if (o instanceof String) {
        collection.add((String) o);
        continue;
      }
      
      // If it's an array, convert to List (which is a Collection).
      if (o instanceof Object[]) {
        o = Arrays.asList((Object[]) o);
      }
      
      // If it's a Collection, collect each value.
      if (o instanceof Collection) {
        for (Object item : (Collection) o) {
          if (!(item instanceof String)) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, err + item.getClass());
          }
          collection.add((String) item);
        }
        continue;
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, err + o.getClass());
    }
    
    if (collection.size() > 0) {
      killAll(name);
    }
    
    return collection;
  }
  
  public void clear() {
    nvPairs.clear();
  }

  @Override
  public int hashCode() {
    return nvPairs.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof NamedList)) return false;
    NamedList<?> nl = (NamedList<?>) obj;
    return this.nvPairs.equals(nl.nvPairs);
  }


  @Override
  public void abortableForEach(BiFunction<String, ? super T, Boolean> fun) {
    int sz = size();
    for (int i = 0; i < sz; i++) {
      if(!fun.apply(getName(i), getVal(i))) break;
    }
  }

  @Override
  public void abortableForEachKey(Function<String, Boolean> fun) {
    int sz = size();
    for (int i = 0; i < sz; i++) {
      if(!fun.apply(getName(i))) break;
    }
  }

  @Override
  public void forEachKey(Consumer<String> fun) {
    int sz = size();
    for (int i = 0; i < sz; i++) {
      fun.accept(getName(i));
    }
  }
  public void forEach(BiConsumer<String, ? super T> action) {
    int sz = size();
    for (int i = 0; i < sz; i++) {
      action.accept(getName(i), getVal(i));
    }
  }

  @Override
  public int _size() {
    return size();
  }

  @Override
  public void forEachEntry(BiConsumer<String, ? super T> fun) {
    forEach(fun);
  }
}
