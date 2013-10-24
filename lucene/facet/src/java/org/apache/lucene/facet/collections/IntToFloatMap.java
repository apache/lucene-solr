package org.apache.lucene.facet.collections;

import java.util.Arrays;

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

/**
 * An Array-based hashtable which maps primitive int to a primitive float.<br>
 * The hashtable is constracted with a given capacity, or 16 as a default. In
 * case there's not enough room for new pairs, the hashtable grows. <br>
 * Capacity is adjusted to a power of 2, and there are 2 * capacity entries for
 * the hash.
 * 
 * The pre allocated arrays (for keys, values) are at length of capacity + 1,
 * when index 0 is used as 'Ground' or 'NULL'.<br>
 * 
 * The arrays are allocated ahead of hash operations, and form an 'empty space'
 * list, to which the key,value pair is allocated.
 * 
 * @lucene.experimental
 */
public class IntToFloatMap {

  public static final float GROUND = Float.NaN;

  /**
   * Implements an IntIterator which iterates over all the allocated indexes.
   */
  private final class IndexIterator implements IntIterator {
    /**
     * The last used baseHashIndex. Needed for "jumping" from one hash entry
     * to another.
     */
    private int baseHashIndex = 0;

    /**
     * The next not-yet-visited index.
     */
    private int index = 0;

    /**
     * Index of the last visited pair. Used in {@link #remove()}.
     */
    private int lastIndex = 0;

    /**
     * Create the Iterator, make <code>index</code> point to the "first"
     * index which is not empty. If such does not exist (eg. the map is
     * empty) it would be zero.
     */
    public IndexIterator() {
      for (baseHashIndex = 0; baseHashIndex < baseHash.length; ++baseHashIndex) {
        index = baseHash[baseHashIndex];
        if (index != 0) {
          break;
        }
      }
    }

    @Override
    public boolean hasNext() {
      return (index != 0);
    }

    @Override
    public int next() {
      // Save the last index visited
      lastIndex = index;

      // next the index
      index = next[index];

      // if the next index points to the 'Ground' it means we're done with
      // the current hash entry and we need to jump to the next one. This
      // is done until all the hash entries had been visited.
      while (index == 0 && ++baseHashIndex < baseHash.length) {
        index = baseHash[baseHashIndex];
      }

      return lastIndex;
    }

    @Override
    public void remove() {
      IntToFloatMap.this.remove(keys[lastIndex]);
    }

  }

  /**
   * Implements an IntIterator, used for iteration over the map's keys.
   */
  private final class KeyIterator implements IntIterator {
    private IntIterator iterator = new IndexIterator();

    KeyIterator() { }
    
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public int next() {
      return keys[iterator.next()];
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }

  /**
   * Implements an Iterator of a generic type T used for iteration over the
   * map's values.
   */
  private final class ValueIterator implements FloatIterator {
    private IntIterator iterator = new IndexIterator();

    ValueIterator() { }
    
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public float next() {
      return values[iterator.next()];
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }

  /**
   * Default capacity - in case no capacity was specified in the constructor
   */
  private static int defaultCapacity = 16;

  /**
   * Holds the base hash entries. if the capacity is 2^N, than the base hash
   * holds 2^(N+1). It can hold
   */
  int[] baseHash;

  /**
   * The current capacity of the map. Always 2^N and never less than 16. We
   * never use the zero index. It is needed to improve performance and is also
   * used as "ground".
   */
  private int capacity;
  /**
   * All objects are being allocated at map creation. Those objects are "free"
   * or empty. Whenever a new pair comes along, a pair is being "allocated" or
   * taken from the free-linked list. as this is just a free list.
   */
  private int firstEmpty;

  /**
   * hashFactor is always (2^(N+1)) - 1. Used for faster hashing.
   */
  private int hashFactor;

  /**
   * This array holds the unique keys
   */
  int[] keys;

  /**
   * In case of collisions, we implement a float linked list of the colliding
   * hash's with the following next[] and prev[]. Those are also used to store
   * the "empty" list.
   */
  int[] next;

  private int prev;

  /**
   * Number of currently objects in the map.
   */
  private int size;

  /**
   * This array holds the values
   */
  float[] values;

  /**
   * Constructs a map with default capacity.
   */
  public IntToFloatMap() {
    this(defaultCapacity);
  }

  /**
   * Constructs a map with given capacity. Capacity is adjusted to a native
   * power of 2, with minimum of 16.
   * 
   * @param capacity
   *            minimum capacity for the map.
   */
  public IntToFloatMap(int capacity) {
    this.capacity = 16;
    // Minimum capacity is 16..
    while (this.capacity < capacity) {
      // Multiply by 2 as long as we're still under the requested capacity
      this.capacity <<= 1;
    }

    // As mentioned, we use the first index (0) as 'Ground', so we need the
    // length of the arrays to be one more than the capacity
    int arrayLength = this.capacity + 1;

    this.values = new float[arrayLength];
    this.keys = new int[arrayLength];
    this.next = new int[arrayLength];

    // Hash entries are twice as big as the capacity.
    int baseHashSize = this.capacity << 1;

    this.baseHash = new int[baseHashSize];

    this.values[0] = GROUND;

    // The has factor is 2^M - 1 which is used as an "AND" hashing operator.
    // {@link #calcBaseHash()}
    this.hashFactor = baseHashSize - 1;

    this.size = 0;

    clear();
  }

  /**
   * Adds a pair to the map. Takes the first empty position from the
   * empty-linked-list's head - {@link #firstEmpty}.
   * 
   * New pairs are always inserted to baseHash, and are followed by the old
   * colliding pair.
   * 
   * @param key
   *            integer which maps the given Object
   * @param v
   *            float value which is being mapped using the given key
   */
  private void prvt_put(int key, float v) {
    // Hash entry to which the new pair would be inserted
    int hashIndex = calcBaseHashIndex(key);

    // 'Allocating' a pair from the "Empty" list.
    int objectIndex = firstEmpty;

    // Setting data
    firstEmpty = next[firstEmpty];
    values[objectIndex] = v;
    keys[objectIndex] = key;

    // Inserting the new pair as the first node in the specific hash entry
    next[objectIndex] = baseHash[hashIndex];
    baseHash[hashIndex] = objectIndex;

    // Announcing a new pair was added!
    ++size;
  }

  /**
   * Calculating the baseHash index using the internal <code>hashFactor</code>
   * .
   */
  protected int calcBaseHashIndex(int key) {
    return key & hashFactor;
  }

  /**
   * Empties the map. Generates the "Empty" space list for later allocation.
   */
  public void clear() {
    // Clears the hash entries
    Arrays.fill(this.baseHash, 0);

    // Set size to zero
    size = 0;

    // Mark all array entries as empty. This is done with
    // <code>firstEmpty</code> pointing to the first valid index (1 as 0 is
    // used as 'Ground').
    firstEmpty = 1;

    // And setting all the <code>next[i]</code> to point at
    // <code>i+1</code>.
    for (int i = 1; i < this.capacity;) {
      next[i] = ++i;
    }

    // Surly, the last one should point to the 'Ground'.
    next[this.capacity] = 0;
  }

  /**
   * Checks if a given key exists in the map.
   * 
   * @param key
   *            that is checked against the map data.
   * @return true if the key exists in the map. false otherwise.
   */
  public boolean containsKey(int key) {
    return find(key) != 0;
  }

  /**
   * Checks if the given value exists in the map.<br>
   * This method iterates over the collection, trying to find an equal object.
   * 
   * @param value
   *            float value that is checked against the map data.
   * @return true if the value exists in the map, false otherwise.
   */
  public boolean containsValue(float value) {
    for (FloatIterator iterator = iterator(); iterator.hasNext();) {
      float d = iterator.next();
      if (d == value) {
        return true;
      }
    }
    return false;
  }

  /**
   * Find the actual index of a given key.
   * 
   * @return index of the key. zero if the key wasn't found.
   */
  protected int find(int key) {
    // Calculate the hash entry.
    int baseHashIndex = calcBaseHashIndex(key);

    // Start from the hash entry.
    int localIndex = baseHash[baseHashIndex];

    // while the index does not point to the 'Ground'
    while (localIndex != 0) {
      // returns the index found in case of of a matching key.
      if (keys[localIndex] == key) {
        return localIndex;
      }
        
      // next the local index
      localIndex = next[localIndex];
    }

    // If we got this far, it could only mean we did not find the key we
    // were asked for. return 'Ground' index.
    return 0;
  }

  /**
   * Find the actual index of a given key with it's baseHashIndex.<br>
   * Some methods use the baseHashIndex. If those call {@link #find} there's
   * no need to re-calculate that hash.
   * 
   * @return the index of the given key, or 0 as 'Ground' if the key wasn't
   *         found.
   */
  private int findForRemove(int key, int baseHashIndex) {
    // Start from the hash entry.
    this.prev = 0;
    int index = baseHash[baseHashIndex];

    // while the index does not point to the 'Ground'
    while (index != 0) {
      // returns the index found in case of of a matching key.
      if (keys[index] == key) {
        return index;
      }

      // next the local index
      prev = index;
      index = next[index];
    }

    // If we got this far, it could only mean we did not find the key we
    // were asked for. return 'Ground' index.
    this.prev = 0;
    return 0;
  }

  /**
   * Returns the value mapped with the given key.
   * 
   * @param key
   *            int who's mapped object we're interested in.
   * @return a float value mapped by the given key. float.NaN if the key wasn't found.
   */
  public float get(int key) {
    return values[find(key)];
  }

  /**
   * Grows the map. Allocates a new map of float the capacity, and
   * fast-insert the old key-value pairs.
   */
  protected void grow() {
    IntToFloatMap that = new IntToFloatMap(
        this.capacity * 2);

    // Iterates fast over the collection. Any valid pair is put into the new
    // map without checking for duplicates or if there's enough space for
    // it.
    for (IndexIterator iterator = new IndexIterator(); iterator.hasNext();) {
      int index = iterator.next();
      that.prvt_put(this.keys[index], this.values[index]);
    }

    // Copy that's data into this.
    this.capacity = that.capacity;
    this.size = that.size;
    this.firstEmpty = that.firstEmpty;
    this.values = that.values;
    this.keys = that.keys;
    this.next = that.next;
    this.baseHash = that.baseHash;
    this.hashFactor = that.hashFactor;
  }

  /**
   * 
   * @return true if the map is empty. false otherwise.
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Returns a new iterator for the mapped float values.
   */
  public FloatIterator iterator() {
    return new ValueIterator();
  }

  /** Returns an iterator on the map keys. */
  public IntIterator keyIterator() {
    return new KeyIterator();
  }

  /**
   * Prints the baseHash array, used for debug purposes.
   */
  @SuppressWarnings("unused")
  private String getBaseHashAsString() {
    return Arrays.toString(this.baseHash);
  }

  /**
   * Inserts the &lt;key,value&gt; pair into the map. If the key already exists,
   * this method updates the mapped value to the given one, returning the old
   * mapped value.
   * 
   * @return the old mapped value, or {@link Float#NaN} if the key didn't exist.
   */
  public float put(int key, float v) {
    // Does key exists?
    int index = find(key);

    // Yes!
    if (index != 0) {
      // Set new data and exit.
      float old = values[index];
      values[index] = v;
      return old;
    }

    // Is there enough room for a new pair?
    if (size == capacity) {
      // No? Than grow up!
      grow();
    }

    // Now that everything is set, the pair can be just put inside with no
    // worries.
    prvt_put(key, v);

    return Float.NaN;
  }

  /**
   * Removes a &lt;key,value&gt; pair from the map and returns the mapped value,
   * or {@link Float#NaN} if the none existed.
   * 
   * @param key used to find the value to remove
   * @return the removed value or {@link Float#NaN} if none existed.
   */
  public float remove(int key) {
    int baseHashIndex = calcBaseHashIndex(key);
    int index = findForRemove(key, baseHashIndex);
    if (index != 0) {
      // If it is the first in the collision list, we should promote its
      // next colliding element.
      if (prev == 0) {
        baseHash[baseHashIndex] = next[index];
      }

      next[prev] = next[index];
      next[index] = firstEmpty;
      firstEmpty = index;
      --size;
      return values[index];
    }

    return Float.NaN;
  }

  /**
   * @return number of pairs currently in the map
   */
  public int size() {
    return this.size;
  }

  /**
   * Translates the mapped pairs' values into an array of Objects
   * 
   * @return a float array of all the values currently in the map.
   */
  public float[] toArray() {
    int j = -1;
    float[] array = new float[size];

    // Iterates over the values, adding them to the array.
    for (FloatIterator iterator = iterator(); iterator.hasNext();) {
      array[++j] = iterator.next();
    }
    return array;
  }

  /**
   * Translates the mapped pairs' values into an array of T
   * 
   * @param a
   *            the array into which the elements of the list are to be
   *            stored. If it is big enough use whatever space we need,
   *            setting the one after the true data as {@link Float#NaN}.
   * 
   * @return an array containing the elements of the list, using the given
   *         parameter if big enough, otherwise allocate an appropriate array
   *         and return it.
   * 
   */
  public float[] toArray(float[] a) {
    int j = 0;
    if (a.length < this.size()) {
      a = new float[this.size()];
    }

    // Iterates over the values, adding them to the array.
    for (FloatIterator iterator = iterator(); iterator.hasNext(); ++j) {
      a[j] = iterator.next();
    }

    if (j < a.length) {
      a[j] = Float.NaN;
    }

    return a;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append('{');
    IntIterator keyIterator = keyIterator();
    while (keyIterator.hasNext()) {
      int key = keyIterator.next();
      sb.append(key);
      sb.append('=');
      sb.append(get(key));
      if (keyIterator.hasNext()) {
        sb.append(',');
        sb.append(' ');
      }
    }
    sb.append('}');
    return sb.toString();
  }
  
  @Override
  public int hashCode() {
    return getClass().hashCode() ^ size();
  }
  
  @Override
  public boolean equals(Object o) {
    IntToFloatMap that = (IntToFloatMap)o;
    if (that.size() != this.size()) {
      return false;
    }
    
    IntIterator it = keyIterator();
    while (it.hasNext()) {
      int key = it.next();
      if (!that.containsKey(key)) {
        return false;
      }

      float v1 = this.get(key);
      float v2 = that.get(key);
      if (Float.compare(v1, v2) != 0) {
        return false;
      }
    }
    return true;
  }
}