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

package org.apache.lucene.util.hppc;

import static org.apache.lucene.util.BitUtil.nextHighestPowerOfTwo;

import java.util.Arrays;
import java.util.IllegalFormatException;
import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A hash map of <code>int</code> to <code>int</code>, implemented using open addressing with linear
 * probing for collision resolution.
 *
 * <p>Mostly forked and trimmed from com.carrotsearch.hppc.IntIntHashMap
 *
 * <p>github: https://github.com/carrotsearch/hppc release 0.9.0
 */
public class IntIntHashMap implements Iterable<IntIntHashMap.IntIntCursor>, Cloneable {

  public static final int DEFAULT_EXPECTED_ELEMENTS = 4;

  public static final float DEFAULT_LOAD_FACTOR = 0.75f;

  private static final AtomicInteger ITERATION_SEED = new AtomicInteger();

  /** Minimal sane load factor (99 empty slots per 100). */
  public static final float MIN_LOAD_FACTOR = 1 / 100.0f;

  /** Maximum sane load factor (1 empty slot per 100). */
  public static final float MAX_LOAD_FACTOR = 99 / 100.0f;

  /** Minimum hash buffer size. */
  public static final int MIN_HASH_ARRAY_LENGTH = 4;

  /**
   * Maximum array size for hash containers (power-of-two and still allocable in Java, not a
   * negative int).
   */
  public static final int MAX_HASH_ARRAY_LENGTH = 0x80000000 >>> 1;

  /** The array holding keys. */
  public int[] keys;

  /** The array holding values. */
  public int[] values;

  /**
   * The number of stored keys (assigned key slots), excluding the special "empty" key, if any (use
   * {@link #size()} instead).
   *
   * @see #size()
   */
  protected int assigned;

  /** Mask for slot scans in {@link #keys}. */
  protected int mask;

  /** Expand (rehash) {@link #keys} when {@link #assigned} hits this value. */
  protected int resizeAt;

  /** Special treatment for the "empty slot" key marker. */
  protected boolean hasEmptyKey;

  /** The load factor for {@link #keys}. */
  protected double loadFactor;

  /** Seed used to ensure the hash iteration order is different from an iteration to another. */
  protected int iterationSeed;

  /** New instance with sane defaults. */
  public IntIntHashMap() {
    this(DEFAULT_EXPECTED_ELEMENTS);
  }

  /**
   * New instance with sane defaults.
   *
   * @param expectedElements The expected number of elements guaranteed not to cause buffer
   *     expansion (inclusive).
   */
  public IntIntHashMap(int expectedElements) {
    this(expectedElements, DEFAULT_LOAD_FACTOR);
  }

  /**
   * New instance with the provided defaults.
   *
   * @param expectedElements The expected number of elements guaranteed not to cause a rehash
   *     (inclusive).
   * @param loadFactor The load factor for internal buffers. Insane load factors (zero, full
   *     capacity) are rejected by {@link #verifyLoadFactor(double)}.
   */
  public IntIntHashMap(int expectedElements, double loadFactor) {
    this.loadFactor = verifyLoadFactor(loadFactor);
    iterationSeed = ITERATION_SEED.incrementAndGet();
    ensureCapacity(expectedElements);
  }

  /** Create a hash map from all key-value pairs of another container. */
  public IntIntHashMap(Iterable<? extends IntIntCursor> container) {
    this();
    putAll(container);
  }

  public int put(int key, int value) {
    assert assigned < mask + 1;

    final int mask = this.mask;
    if (((key) == 0)) {
      hasEmptyKey = true;
      int previousValue = values[mask + 1];
      values[mask + 1] = value;
      return previousValue;
    } else {
      final int[] keys = this.keys;
      int slot = hashKey(key) & mask;

      int existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          final int previousValue = values[slot];
          values[slot] = value;
          return previousValue;
        }
        slot = (slot + 1) & mask;
      }

      if (assigned == resizeAt) {
        allocateThenInsertThenRehash(slot, key, value);
      } else {
        keys[slot] = key;
        values[slot] = value;
      }

      assigned++;
      return 0;
    }
  }

  public int putAll(Iterable<? extends IntIntCursor> iterable) {
    final int count = size();
    for (IntIntCursor c : iterable) {
      put(c.key, c.value);
    }
    return size() - count;
  }

  /**
   * <a href="http://trove4j.sourceforge.net">Trove</a>-inspired API method. An equivalent of the
   * following code:
   *
   * <pre>
   * if (!map.containsKey(key)) map.put(value);
   * </pre>
   *
   * @param key The key of the value to check.
   * @param value The value to put if <code>key</code> does not exist.
   * @return <code>true</code> if <code>key</code> did not exist and <code>value</code> was placed
   *     in the map.
   */
  public boolean putIfAbsent(int key, int value) {
    int keyIndex = indexOf(key);
    if (!indexExists(keyIndex)) {
      indexInsert(keyIndex, key, value);
      return true;
    } else {
      return false;
    }
  }

  /**
   * If <code>key</code> exists, <code>putValue</code> is inserted into the map, otherwise any
   * existing value is incremented by <code>additionValue</code>.
   *
   * @param key The key of the value to adjust.
   * @param putValue The value to put if <code>key</code> does not exist.
   * @param incrementValue The value to add to the existing value if <code>key</code> exists.
   * @return Returns the current value associated with <code>key</code> (after changes).
   */
  public int putOrAdd(int key, int putValue, int incrementValue) {
    assert assigned < mask + 1;

    int keyIndex = indexOf(key);
    if (indexExists(keyIndex)) {
      putValue = values[keyIndex] + incrementValue;
      indexReplace(keyIndex, putValue);
    } else {
      indexInsert(keyIndex, key, putValue);
    }
    return putValue;
  }

  /**
   * Adds <code>incrementValue</code> to any existing value for the given <code>key</code> or
   * inserts <code>incrementValue</code> if <code>key</code> did not previously exist.
   *
   * @param key The key of the value to adjust.
   * @param incrementValue The value to put or add to the existing value if <code>key</code> exists.
   * @return Returns the current value associated with <code>key</code> (after changes).
   */
  public int addTo(int key, int incrementValue) {
    return putOrAdd(key, incrementValue, incrementValue);
  }

  public int remove(int key) {
    final int mask = this.mask;
    if (((key) == 0)) {
      hasEmptyKey = false;
      int previousValue = values[mask + 1];
      values[mask + 1] = 0;
      return previousValue;
    } else {
      final int[] keys = this.keys;
      int slot = hashKey(key) & mask;

      int existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          final int previousValue = values[slot];
          shiftConflictingKeys(slot);
          return previousValue;
        }
        slot = (slot + 1) & mask;
      }

      return 0;
    }
  }

  public int get(int key) {
    if (((key) == 0)) {
      return hasEmptyKey ? values[mask + 1] : 0;
    } else {
      final int[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      int existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          return values[slot];
        }
        slot = (slot + 1) & mask;
      }

      return 0;
    }
  }

  public int getOrDefault(int key, int defaultValue) {
    if (((key) == 0)) {
      return hasEmptyKey ? values[mask + 1] : defaultValue;
    } else {
      final int[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      int existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          return values[slot];
        }
        slot = (slot + 1) & mask;
      }

      return defaultValue;
    }
  }

  public boolean containsKey(int key) {
    if (((key) == 0)) {
      return hasEmptyKey;
    } else {
      final int[] keys = this.keys;
      final int mask = this.mask;
      int slot = hashKey(key) & mask;

      int existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          return true;
        }
        slot = (slot + 1) & mask;
      }

      return false;
    }
  }

  public int indexOf(int key) {
    final int mask = this.mask;
    if (((key) == 0)) {
      return hasEmptyKey ? mask + 1 : ~(mask + 1);
    } else {
      final int[] keys = this.keys;
      int slot = hashKey(key) & mask;

      int existing;
      while (!((existing = keys[slot]) == 0)) {
        if (((existing) == (key))) {
          return slot;
        }
        slot = (slot + 1) & mask;
      }

      return ~slot;
    }
  }

  public boolean indexExists(int index) {
    assert index < 0 || (index >= 0 && index <= mask) || (index == mask + 1 && hasEmptyKey);

    return index >= 0;
  }

  public int indexGet(int index) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    return values[index];
  }

  public int indexReplace(int index, int newValue) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    int previousValue = values[index];
    values[index] = newValue;
    return previousValue;
  }

  public void indexInsert(int index, int key, int value) {
    assert index < 0 : "The index must not point at an existing key.";

    index = ~index;
    if (((key) == 0)) {
      assert index == mask + 1;
      values[index] = value;
      hasEmptyKey = true;
    } else {
      assert ((keys[index]) == 0);

      if (assigned == resizeAt) {
        allocateThenInsertThenRehash(index, key, value);
      } else {
        keys[index] = key;
        values[index] = value;
      }

      assigned++;
    }
  }

  public int indexRemove(int index) {
    assert index >= 0 : "The index must point at an existing key.";
    assert index <= mask || (index == mask + 1 && hasEmptyKey);

    int previousValue = values[index];
    if (index > mask) {
      hasEmptyKey = false;
      values[index] = 0;
    } else {
      shiftConflictingKeys(index);
    }
    return previousValue;
  }

  public void clear() {
    assigned = 0;
    hasEmptyKey = false;

    Arrays.fill(keys, 0);

    /*  */
  }

  public void release() {
    assigned = 0;
    hasEmptyKey = false;

    keys = null;
    values = null;
    ensureCapacity(DEFAULT_EXPECTED_ELEMENTS);
  }

  public int size() {
    return assigned + (hasEmptyKey ? 1 : 0);
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public int hashCode() {
    int h = hasEmptyKey ? 0xDEADBEEF : 0;
    for (IntIntCursor c : this) {
      h += BitMixer.mix(c.key) + BitMixer.mix(c.value);
    }
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && getClass() == obj.getClass() && equalElements(getClass().cast(obj));
  }

  /** Return true if all keys of some other container exist in this container. */
  protected boolean equalElements(IntIntHashMap other) {
    if (other.size() != size()) {
      return false;
    }

    for (IntIntCursor c : other) {
      int key = c.key;
      if (!containsKey(key) || !((get(key)) == (c.value))) {
        return false;
      }
    }

    return true;
  }

  /**
   * Ensure this container can hold at least the given number of keys (entries) without resizing its
   * buffers.
   *
   * @param expectedElements The total number of keys, inclusive.
   */
  public void ensureCapacity(int expectedElements) {
    if (expectedElements > resizeAt || keys == null) {
      final int[] prevKeys = this.keys;
      final int[] prevValues = this.values;
      allocateBuffers(minBufferSize(expectedElements, loadFactor));
      if (prevKeys != null && !isEmpty()) {
        rehash(prevKeys, prevValues);
      }
    }
  }

  /**
   * Provides the next iteration seed used to build the iteration starting slot and offset
   * increment. This method does not need to be synchronized, what matters is that each thread gets
   * a sequence of varying seeds.
   */
  protected int nextIterationSeed() {
    return iterationSeed = BitMixer.mixPhi(iterationSeed);
  }

  /** An iterator implementation for {@link #iterator}. */
  private final class EntryIterator extends AbstractIterator<IntIntCursor> {
    private final IntIntCursor cursor;
    private final int increment;
    private int index;
    private int slot;

    public EntryIterator() {
      cursor = new IntIntCursor();
      int seed = nextIterationSeed();
      increment = iterationIncrement(seed);
      slot = seed & mask;
    }

    @Override
    protected IntIntCursor fetch() {
      final int mask = IntIntHashMap.this.mask;
      while (index <= mask) {
        int existing;
        index++;
        slot = (slot + increment) & mask;
        if (!((existing = keys[slot]) == 0)) {
          cursor.index = slot;
          cursor.key = existing;
          cursor.value = values[slot];
          return cursor;
        }
      }

      if (index == mask + 1 && hasEmptyKey) {
        cursor.index = index;
        cursor.key = 0;
        cursor.value = values[index++];
        return cursor;
      }

      return done();
    }
  }

  @Override
  public Iterator<IntIntCursor> iterator() {
    return new EntryIterator();
  }

  /** Returns a specialized view of the keys of this associated container. */
  public KeysContainer keys() {
    return new KeysContainer();
  }

  /** A view of the keys inside this hash map. */
  public final class KeysContainer extends IntContainer {
    @Override
    public Iterator<IntCursor> iterator() {
      return new KeysIterator();
    }
  }
  ;

  /** An iterator over the set of assigned keys. */
  private final class KeysIterator extends AbstractIterator<IntCursor> {
    private final IntCursor cursor;
    private final int increment;
    private int index;
    private int slot;

    public KeysIterator() {
      cursor = new IntCursor();
      int seed = nextIterationSeed();
      increment = iterationIncrement(seed);
      slot = seed & mask;
    }

    @Override
    protected IntCursor fetch() {
      final int mask = IntIntHashMap.this.mask;
      while (index <= mask) {
        int existing;
        index++;
        slot = (slot + increment) & mask;
        if (!((existing = keys[slot]) == 0)) {
          cursor.index = slot;
          cursor.value = existing;
          return cursor;
        }
      }

      if (index == mask + 1 && hasEmptyKey) {
        cursor.index = index++;
        cursor.value = 0;
        return cursor;
      }

      return done();
    }
  }

  /** @return Returns a container with all values stored in this map. */
  public IntContainer values() {
    return new ValuesContainer();
  }

  /** A view over the set of values of this map. */
  private final class ValuesContainer extends IntContainer {
    @Override
    public Iterator<IntCursor> iterator() {
      return new ValuesIterator();
    }
  }

  /** IntCursor iterable with size and toArray function implemented */
  public abstract class IntContainer implements Iterable<IntCursor> {

    public int size() {
      return IntIntHashMap.this.size();
    }

    public int[] toArray() {
      int[] array = new int[size()];
      int i = 0;
      for (IntCursor cursor : this) {
        array[i++] = cursor.value;
      }
      return array;
    }
  }

  /** An iterator over the set of assigned values. */
  private final class ValuesIterator extends AbstractIterator<IntCursor> {
    private final IntCursor cursor;
    private final int increment;
    private int index;
    private int slot;

    public ValuesIterator() {
      cursor = new IntCursor();
      int seed = nextIterationSeed();
      increment = iterationIncrement(seed);
      slot = seed & mask;
    }

    @Override
    protected IntCursor fetch() {
      final int mask = IntIntHashMap.this.mask;
      while (index <= mask) {
        index++;
        slot = (slot + increment) & mask;
        if (!((keys[slot]) == 0)) {
          cursor.index = slot;
          cursor.value = values[slot];
          return cursor;
        }
      }

      if (index == mask + 1 && hasEmptyKey) {
        cursor.index = index;
        cursor.value = values[index++];
        return cursor;
      }

      return done();
    }
  }

  /** Simplifies the implementation of iterators a bit. Modeled loosely after Google Guava's API. */
  public abstract static class AbstractIterator<E> implements Iterator<E> {
    private static final int NOT_CACHED = 0;
    private static final int CACHED = 1;
    private static final int AT_END = 2;

    /** Current iterator state. */
    private int state = NOT_CACHED;

    /** The next element to be returned from {@link #next()} if fetched. */
    private E nextElement;

    @Override
    public boolean hasNext() {
      if (state == NOT_CACHED) {
        state = CACHED;
        nextElement = fetch();
      }
      return state == CACHED;
    }

    @Override
    public E next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      state = NOT_CACHED;
      return nextElement;
    }

    /** Default implementation throws {@link UnsupportedOperationException}. */
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    /**
     * Fetch next element. The implementation must return {@link #done()} when all elements have
     * been fetched.
     *
     * @return Returns the next value for the iterator or chain-calls {@link #done()}.
     */
    protected abstract E fetch();

    /**
     * Call when done.
     *
     * @return Returns a unique sentinel value to indicate end-of-iteration.
     */
    protected final E done() {
      state = AT_END;
      return null;
    }
  }

  @Override
  public IntIntHashMap clone() {
    try {
      /*  */
      IntIntHashMap cloned = (IntIntHashMap) super.clone();
      cloned.keys = keys.clone();
      cloned.values = values.clone();
      cloned.hasEmptyKey = hasEmptyKey;
      cloned.iterationSeed = nextIterationSeed();
      return cloned;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  /** Convert the contents of this map to a human-friendly string. */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("[");

    boolean first = true;
    for (IntIntCursor cursor : this) {
      if (!first) {
        buffer.append(", ");
      }
      buffer.append(cursor.key);
      buffer.append("=>");
      buffer.append(cursor.value);
      first = false;
    }
    buffer.append("]");
    return buffer.toString();
  }

  /** Creates a hash map from two index-aligned arrays of key-value pairs. */
  public static IntIntHashMap from(int[] keys, int[] values) {
    if (keys.length != values.length) {
      throw new IllegalArgumentException(
          "Arrays of keys and values must have an identical length.");
    }

    IntIntHashMap map = new IntIntHashMap(keys.length);
    for (int i = 0; i < keys.length; i++) {
      map.put(keys[i], values[i]);
    }

    return map;
  }

  /**
   * Returns a hash code for the given key.
   *
   * <p>The output from this function should evenly distribute keys across the entire integer range.
   */
  protected int hashKey(int key) {
    assert !((key) == 0); // Handled as a special case (empty slot marker).
    return BitMixer.mixPhi(key);
  }

  /**
   * Validate load factor range and return it. Override and suppress if you need insane load
   * factors.
   */
  protected double verifyLoadFactor(double loadFactor) {
    checkLoadFactor(loadFactor, MIN_LOAD_FACTOR, MAX_LOAD_FACTOR);
    return loadFactor;
  }

  /** Rehash from old buffers to new buffers. */
  protected void rehash(int[] fromKeys, int[] fromValues) {
    assert fromKeys.length == fromValues.length && checkPowerOfTwo(fromKeys.length - 1);

    // Rehash all stored key/value pairs into the new buffers.
    final int[] keys = this.keys;
    final int[] values = this.values;
    final int mask = this.mask;
    int existing;

    // Copy the zero element's slot, then rehash everything else.
    int from = fromKeys.length - 1;
    keys[keys.length - 1] = fromKeys[from];
    values[values.length - 1] = fromValues[from];
    while (--from >= 0) {
      if (!((existing = fromKeys[from]) == 0)) {
        int slot = hashKey(existing) & mask;
        while (!((keys[slot]) == 0)) {
          slot = (slot + 1) & mask;
        }
        keys[slot] = existing;
        values[slot] = fromValues[from];
      }
    }
  }

  /**
   * Allocate new internal buffers. This method attempts to allocate and assign internal buffers
   * atomically (either allocations succeed or not).
   */
  protected void allocateBuffers(int arraySize) {
    assert Integer.bitCount(arraySize) == 1;

    // Ensure no change is done if we hit an OOM.
    int[] prevKeys = this.keys;
    int[] prevValues = this.values;
    try {
      int emptyElementSlot = 1;
      this.keys = (new int[arraySize + emptyElementSlot]);
      this.values = (new int[arraySize + emptyElementSlot]);
    } catch (OutOfMemoryError e) {
      this.keys = prevKeys;
      this.values = prevValues;
      throw new BufferAllocationException(
          "Not enough memory to allocate buffers for rehashing: %,d -> %,d",
          e, this.mask + 1, arraySize);
    }

    this.resizeAt = expandAtCount(arraySize, loadFactor);
    this.mask = arraySize - 1;
  }

  /**
   * This method is invoked when there is a new key/ value pair to be inserted into the buffers but
   * there is not enough empty slots to do so.
   *
   * <p>New buffers are allocated. If this succeeds, we know we can proceed with rehashing so we
   * assign the pending element to the previous buffer (possibly violating the invariant of having
   * at least one empty slot) and rehash all keys, substituting new buffers at the end.
   */
  protected void allocateThenInsertThenRehash(int slot, int pendingKey, int pendingValue) {
    assert assigned == resizeAt && ((keys[slot]) == 0) && !((pendingKey) == 0);

    // Try to allocate new buffers first. If we OOM, we leave in a consistent state.
    final int[] prevKeys = this.keys;
    final int[] prevValues = this.values;
    allocateBuffers(nextBufferSize(mask + 1, size(), loadFactor));
    assert this.keys.length > prevKeys.length;

    // We have succeeded at allocating new data so insert the pending key/value at
    // the free slot in the old arrays before rehashing.
    prevKeys[slot] = pendingKey;
    prevValues[slot] = pendingValue;

    // Rehash old keys, including the pending key.
    rehash(prevKeys, prevValues);
  }

  static int nextBufferSize(int arraySize, int elements, double loadFactor) {
    assert checkPowerOfTwo(arraySize);
    if (arraySize == MAX_HASH_ARRAY_LENGTH) {
      throw new BufferAllocationException(
          "Maximum array size exceeded for this load factor (elements: %d, load factor: %f)",
          elements, loadFactor);
    }

    return arraySize << 1;
  }

  static int expandAtCount(int arraySize, double loadFactor) {
    assert checkPowerOfTwo(arraySize);
    // Take care of hash container invariant (there has to be at least one empty slot to ensure
    // the lookup loop finds either the element or an empty slot).
    return Math.min(arraySize - 1, (int) Math.ceil(arraySize * loadFactor));
  }

  static boolean checkPowerOfTwo(int arraySize) {
    // These are internals, we can just assert without retrying.
    assert arraySize > 1;
    assert nextHighestPowerOfTwo(arraySize) == arraySize;
    return true;
  }

  static int minBufferSize(int elements, double loadFactor) {
    if (elements < 0) {
      throw new IllegalArgumentException("Number of elements must be >= 0: " + elements);
    }

    long length = (long) Math.ceil(elements / loadFactor);
    if (length == elements) {
      length++;
    }
    length = Math.max(MIN_HASH_ARRAY_LENGTH, nextHighestPowerOfTwo(length));

    if (length > MAX_HASH_ARRAY_LENGTH) {
      throw new BufferAllocationException(
          "Maximum array size exceeded for this load factor (elements: %d, load factor: %f)",
          elements, loadFactor);
    }

    return (int) length;
  }

  static void checkLoadFactor(
      double loadFactor, double minAllowedInclusive, double maxAllowedInclusive) {
    if (loadFactor < minAllowedInclusive || loadFactor > maxAllowedInclusive) {
      throw new BufferAllocationException(
          "The load factor should be in range [%.2f, %.2f]: %f",
          minAllowedInclusive, maxAllowedInclusive, loadFactor);
    }
  }

  static int iterationIncrement(int seed) {
    return 29 + ((seed & 7) << 1); // Small odd integer.
  }

  /**
   * Shift all the slot-conflicting keys and values allocated to (and including) <code>slot</code>.
   */
  protected void shiftConflictingKeys(int gapSlot) {
    final int[] keys = this.keys;
    final int[] values = this.values;
    final int mask = this.mask;

    // Perform shifts of conflicting keys to fill in the gap.
    int distance = 0;
    while (true) {
      final int slot = (gapSlot + (++distance)) & mask;
      final int existing = keys[slot];
      if (((existing) == 0)) {
        break;
      }

      final int idealSlot = hashKey(existing);
      final int shift = (slot - idealSlot) & mask;
      if (shift >= distance) {
        // Entry at this position was originally at or before the gap slot.
        // Move the conflict-shifted entry to the gap's position and repeat the procedure
        // for any entries to the right of the current position, treating it
        // as the new gap.
        keys[gapSlot] = existing;
        values[gapSlot] = values[slot];
        gapSlot = slot;
        distance = 0;
      }
    }

    // Mark the last found gap slot without a conflict as empty.
    keys[gapSlot] = 0;
    values[gapSlot] = 0;
    assigned--;
  }

  /** Forked from HPPC, holding int index,key and value */
  public final class IntIntCursor {
    /**
     * The current key and value's index in the container this cursor belongs to. The meaning of
     * this index is defined by the container (usually it will be an index in the underlying storage
     * buffer).
     */
    public int index;

    /** The current key. */
    public int key;

    /** The current value. */
    public int value;

    @Override
    public String toString() {
      return "[cursor, index: " + index + ", key: " + key + ", value: " + value + "]";
    }
  }

  /** Forked from HPPC, holding int index and int value */
  public final class IntCursor {
    /**
     * The current value's index in the container this cursor belongs to. The meaning of this index
     * is defined by the container (usually it will be an index in the underlying storage buffer).
     */
    public int index;

    /** The current value. */
    public int value;

    @Override
    public String toString() {
      return "[cursor, index: " + index + ", value: " + value + "]";
    }
  }

  /** BufferAllocationException forked from HPPC */
  @SuppressWarnings("serial")
  public static class BufferAllocationException extends RuntimeException {
    public BufferAllocationException(String message) {
      super(message);
    }

    public BufferAllocationException(String message, Object... args) {
      this(message, null, args);
    }

    public BufferAllocationException(String message, Throwable t, Object... args) {
      super(formatMessage(message, t, args), t);
    }

    private static String formatMessage(String message, Throwable t, Object... args) {
      try {
        return String.format(Locale.ROOT, message, args);
      } catch (IllegalFormatException e) {
        BufferAllocationException substitute =
            new BufferAllocationException(message + " [ILLEGAL FORMAT, ARGS SUPPRESSED]");
        if (t != null) {
          substitute.addSuppressed(t);
        }
        substitute.addSuppressed(e);
        throw substitute;
      }
    }
  }
}
