package org.apache.solr.common.util;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ConcurrentNamedList<T> extends NamedList<T> {


  public ConcurrentNamedList() {
    super();
  }


  public ConcurrentNamedList(int sz) {
    super(sz);
  }

  /**
   * The total number of name/value pairs
   */
  public synchronized  int size() {
    return super.size();
  }

  /**
   * The name of the pair at the specified List index
   *
   * @return null if no name exists
   */
  public synchronized  String getName(int idx) {
    return super.getName(idx);
  }

  /**
   * The value of the pair at the specified List index
   *
   * @return may be null
   */
  @SuppressWarnings("unchecked")
  public synchronized  T getVal(int idx) {
    return super.getVal(idx);
  }

  /**
   * Adds a name/value pair to the end of the list.
   */
  public synchronized  void add(String name, T val) {
    super.add(name, val);
  }

  /**
   * Modifies the name of the pair at the specified index.
   */
  public synchronized  void setName(int idx, String name) {
    super.setName(idx, name);
  }

  /**
   * Modifies the value of the pair at the specified index.
   *
   * @return the value that used to be at index
   */
  public synchronized  T setVal(int idx, T val) {
    return super.setVal(idx, val);
  }

  /**
   * Removes the name/value pair at the specified index.
   *
   * @return the value at the index removed
   */
  public synchronized  T remove(int idx) {
    return super.remove(idx);
  }

  /**
   * Scans the list sequentially beginning at the specified index and
   * returns the index of the first pair with the specified name.
   *
   * @param name  name to look for, may be null
   * @param start index to begin searching from
   * @return The index of the first matching pair, -1 if no match
   */
  public synchronized  int indexOf(String name, int start) {
    return super.indexOf(name, start);
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
   * @see #get(String, int)
   */
  public synchronized  T get(String name) {
    return super.get(name);
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
  public synchronized  T get(String name, int start) {
    return super.get(name, start);
  }

  /**
   * Gets the values for the the specified name
   *
   * @param name Name
   * @return List of values
   */
  public List<T> getAll(String name) {
    return super.getAll(name);
  }

  /**
   * Recursively parses the NamedList structure to arrive at a specific element.
   * As you descend the NamedList tree, the last element can be any type,
   * including NamedList, but the previous elements MUST be NamedList objects
   * themselves. A null value is returned if the indicated hierarchy doesn't
   * exist, but NamedList allows null values so that could be the actual value
   * at the end of the path.
   * <p>
   * This method is particularly useful for parsing the response from Solr's
   * /admin/mbeans handler, but it also works for any complex structure.
   * <p>
   * Explicitly casting the return value is recommended. An even safer option is
   * to accept the return value as an object and then check its type.
   * <p>
   * Usage examples:
   * <p>
   * String coreName = (String) response.findRecursive
   * ("solr-mbeans", "CORE", "core", "stats", "coreName");
   * long numDoc = (long) response.findRecursive
   * ("solr-mbeans", "CORE", "searcher", "stats", "numDocs");
   *
   * @param args One or more strings specifying the tree to navigate.
   * @return the last entry in the given path hierarchy, null if not found.
   */
  public synchronized  Object findRecursive(String... args) {
    return super.findRecursive(args);
  }

  public synchronized NamedList<T> getImmutableCopy() {
    return super.getImmutableCopy();
  }

  public synchronized  Map<String,T> asShallowMap() {
    return super.asShallowMap();
  }

  public synchronized  Map<String,T> asShallowMap(boolean allowDps) {
    return super.asShallowMap(allowDps);
  }

  public Map asMap(int maxDepth) {
    return super.asMap(maxDepth);
  }

  /**
   * Create SolrParams from NamedList.  Values must be {@code String[]} or {@code List}
   * (with toString()-appropriate entries), or otherwise have a toString()-appropriate value.
   * Nulls are retained as such in arrays/lists but otherwise will NPE.
   */
  public synchronized SolrParams toSolrParams() {
    return super.toSolrParams();
  }

  /**
   * Iterates over the Map and sequentially adds its key/value pairs
   */
  public synchronized boolean addAll(Map<String,T> args) {
    return super.addAll(args);
  }

  /**
   * Appends the elements of the given NamedList to this one.
   */
  public synchronized boolean addAll(NamedList<T> nl) {
    return super.addAll(nl);
  }

  /**
   * Makes a <i>shallow copy</i> of the named list.
   */
  @Override
  public synchronized  NamedList<T> clone() {
    return super.clone();
  }

  /**
   * Support the Iterable interface
   */
  public Iterator<Map.Entry<String,T>> iterator() {

    final NamedList<T> list = this;

    Iterator<Map.Entry<String,T>> iter = new Iterator<Map.Entry<String,T>>() {

      int idx = 0;

      @Override
      public synchronized boolean hasNext() {
        return idx < list.size();
      }

      @Override
      public synchronized Map.Entry<String,T> next() {
        final int index = idx++;
        Map.Entry<String,T> nv = new Map.Entry<String,T>() {
          @Override
          public synchronized String getKey() {
            return list.getName(index);
          }

          @Override
          public synchronized T getValue() {
            return list.getVal(index);
          }

          @Override
          public synchronized String toString() {
            return getKey() + "=" + getValue();
          }

          @Override
          public synchronized T setValue(T value) {
            return list.setVal(index, value);
          }
        };
        return nv;
      }

      @Override
      public synchronized void remove() {
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
  public synchronized T remove(String name) {
    return super.remove(name);
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
  public synchronized List<T> removeAll(String name) {
    return super.removeAll(name);
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
   * @param name The key to look up in the NamedList.
   * @return The boolean value found.
   * @throws SolrException If multiple values are found for the name or the value found is
   *                       not a Boolean or a String.
   */
  public synchronized Boolean removeBooleanArg(final String name) {
    return super.removeBooleanArg(name);
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
   * @throws SolrException If multiple values are found for the name or the value found is
   *                       not a Boolean or a String.
   */
  public synchronized Boolean getBooleanArg(final String name) {
    return super.getBooleanArg(name);
  }


  public synchronized void clear() {
    super.clear();
  }

  @Override
  public synchronized int hashCode() {
    return super.hashCode();
  }

  @Override
  public synchronized boolean equals(Object obj) {
    return super.equals(obj);
  }



  public static class ConcurrentSimpleOrderedMap<T> extends NamedList<T> {
    /** Creates an empty instance */
    public ConcurrentSimpleOrderedMap() {
      super();
    }

    public ConcurrentSimpleOrderedMap(int sz) {
      super(sz);
    }

    /**
     * Creates an instance backed by an explicitly specified list of
     * pairwise names/values.
     *
     * <p>
     * TODO: this method was formerly public, now that it's not we can change the impl details of
     * this class to be based on a Map.Entry[]
     * </p>
     *
     * @param nameValuePairs underlying List which should be used to implement a SimpleOrderedMap; modifying this List will affect the SimpleOrderedMap.
     * @lucene.internal
     */
    private ConcurrentSimpleOrderedMap(List<Object> nameValuePairs) {
      super(nameValuePairs);
    }

    public ConcurrentSimpleOrderedMap(Map.Entry<String, T>[] nameValuePairs) {
      super(nameValuePairs);
    }

    @Override
    public synchronized ConcurrentSimpleOrderedMap<T> clone() {
      ArrayList<Object> newList = new ArrayList<>(nvPairs.size());
      newList.addAll(nvPairs);
      return new ConcurrentSimpleOrderedMap<T>(newList);
    }
  }

}
