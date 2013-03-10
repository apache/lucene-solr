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
 * A Set or primitive int. Implemented as a HashMap of int->int. *
 * 
 * @lucene.experimental
 */
public class IntHashSet {
  
  // TODO (Facet): This is wasteful as the "values" are actually the "keys" and
  // we could spare this amount of space (capacity * sizeof(int)). Perhaps even
  // though it is not OOP, we should re-implement the hash for just that cause.

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
      IntHashSet.this.remove(keys[lastIndex]);
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
   * In case of collisions, we implement a double linked list of the colliding
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
   * Constructs a map with default capacity.
   */
  public IntHashSet() {
    this(defaultCapacity);
  }

  /**
   * Constructs a map with given capacity. Capacity is adjusted to a native
   * power of 2, with minimum of 16.
   * 
   * @param capacity
   *            minimum capacity for the map.
   */
  public IntHashSet(int capacity) {
    this.capacity = 16;
    // Minimum capacity is 16..
    while (this.capacity < capacity) {
      // Multiply by 2 as long as we're still under the requested capacity
      this.capacity <<= 1;
    }

    // As mentioned, we use the first index (0) as 'Ground', so we need the
    // length of the arrays to be one more than the capacity
    int arrayLength = this.capacity + 1;

    this.keys = new int[arrayLength];
    this.next = new int[arrayLength];

    // Hash entries are twice as big as the capacity.
    int baseHashSize = this.capacity << 1;

    this.baseHash = new int[baseHashSize];

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
   *            integer which maps the given value
   */
  private void prvt_add(int key) {
    // Hash entry to which the new pair would be inserted
    int hashIndex = calcBaseHashIndex(key);

    // 'Allocating' a pair from the "Empty" list.
    int objectIndex = firstEmpty;

    // Setting data
    firstEmpty = next[firstEmpty];
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
   * @param value
   *            that is checked against the map data.
   * @return true if the key exists in the map. false otherwise.
   */
  public boolean contains(int value) {
    return find(value) != 0;
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
   * Grows the map. Allocates a new map of double the capacity, and
   * fast-insert the old key-value pairs.
   */
  protected void grow() {
    IntHashSet that = new IntHashSet(this.capacity * 2);

    // Iterates fast over the collection. Any valid pair is put into the new
    // map without checking for duplicates or if there's enough space for
    // it.
    for (IndexIterator iterator = new IndexIterator(); iterator.hasNext();) {
      int index = iterator.next();
      that.prvt_add(this.keys[index]);
    }
    // for (int i = capacity; i > 0; --i) {
    //
    // that._add(this.keys[i]);
    //
    // }

    // Copy that's data into this.
    this.capacity = that.capacity;
    this.size = that.size;
    this.firstEmpty = that.firstEmpty;
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
   * Returns a new iterator for the mapped objects.
   */
  public IntIterator iterator() {
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
   * Add a mapping int key -> int value.
   * <p>
   * If the key was already inside just
   * updating the value it refers to as the given object.
   * <p>
   * Otherwise if the map is full, first {@link #grow()} the map.
   * 
   * @param value
   *            integer which maps the given value
   * @return true always.
   */
  public boolean add(int value) {
    // Does key exists?
    int index = find(value);

    // Yes!
    if (index != 0) {
      return true;
    }

    // Is there enough room for a new pair?
    if (size == capacity) {
      // No? Than grow up!
      grow();
    }

    // Now that everything is set, the pair can be just put inside with no
    // worries.
    prvt_add(value);

    return true;
  }

  /**
   * Remove a pair from the map, specified by it's key.
   * 
   * @param value
   *            specify the value to be removed
   * 
   * @return true if the map was changed (the key was found and removed).
   *         false otherwise.
   */
  public boolean remove(int value) {
    int baseHashIndex = calcBaseHashIndex(value);
    int index = findForRemove(value, baseHashIndex);
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
      return true;
    }

    return false;
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
   * @return an object array of all the values currently in the map.
   */
  public int[] toArray() {
    int j = -1;
    int[] array = new int[size];

    // Iterates over the values, adding them to the array.
    for (IntIterator iterator = iterator(); iterator.hasNext();) {
      array[++j] = iterator.next();
    }
    return array;
  }

  /**
   * Translates the mapped pairs' values into an array of ints
   * 
   * @param a
   *            the array into which the elements of the map are to be stored,
   *            if it is big enough; otherwise, a new array of the same
   *            runtime type is allocated for this purpose.
   * 
   * @return an array containing the values stored in the map
   * 
   */
  public int[] toArray(int[] a) {
    int j = 0;
    if (a.length < size) {
      a = new int[size];
    }
    // Iterates over the values, adding them to the array.
    for (IntIterator iterator = iterator(); j < a.length
        && iterator.hasNext(); ++j) {
      a[j] = iterator.next();
    }
    return a;
  }

  /**
   * I have no idea why would anyone call it - but for debug purposes.<br>
   * Prints the entire map, including the index, key, object, next and prev.
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append('{');
    IntIterator iterator = iterator();
    while (iterator.hasNext()) {
      sb.append(iterator.next());
      if (iterator.hasNext()) {
        sb.append(',');
        sb.append(' ');
      }
    }
    sb.append('}');
    return sb.toString();
  }

  public String toHashString() {
    String string = "\n";
    StringBuffer sb = new StringBuffer();

    for (int i = 0; i < this.baseHash.length; i++) {
      StringBuffer sb2 = new StringBuffer();
      boolean shouldAppend = false;
      sb2.append(i + ".\t");
      for (int index = baseHash[i]; index != 0; index = next[index]) {
        sb2.append(" -> " + keys[index] + "@" + index);
        shouldAppend = true;
      }
      if (shouldAppend) {
        sb.append(sb2);
        sb.append(string);
      }
    }

    return sb.toString();
  }
}