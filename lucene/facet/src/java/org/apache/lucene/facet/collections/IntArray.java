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
 * A Class wrapper for a grow-able int[] which can be sorted and intersect with
 * other IntArrays.
 * 
 * @lucene.experimental
 */
public class IntArray {

  /**
   * The int[] which holds the data
   */
  private int[] data;

  /**
   * Holds the number of items in the array.
   */
  private int size;

  /**
   * A flag which indicates whether a sort should occur of the array is
   * already sorted.
   */
  private boolean shouldSort;

  /**
   * Construct a default IntArray, size 0 and surly a sort should not occur.
   */
  public IntArray() {
    init(true);
  }

  private void init(boolean realloc) {
    size = 0;
    if (realloc) {
      data = new int[0];
    }
    shouldSort = false;
  }

  /**
   * Intersects the data with a given {@link IntHashSet}.
   * 
   * @param set
   *            A given ArrayHashSetInt which holds the data to be intersected
   *            against
   */
  public void intersect(IntHashSet set) {
    int newSize = 0;
    for (int i = 0; i < size; ++i) {
      if (set.contains(data[i])) {
        data[newSize] = data[i];
        ++newSize;
      }
    }
    this.size = newSize;
  }

  /**
   * Intersects the data with a given IntArray
   * 
   * @param other
   *            A given IntArray which holds the data to be intersected agains
   */
  public void intersect(IntArray other) {
    sort();
    other.sort();

    int myIndex = 0;
    int otherIndex = 0;
    int newSize = 0;
    if (this.size > other.size) {
      while (otherIndex < other.size && myIndex < size) {
        while (otherIndex < other.size
            && other.data[otherIndex] < data[myIndex]) {
          ++otherIndex;
        }
        if (otherIndex == other.size) {
          break;
        }
        while (myIndex < size && other.data[otherIndex] > data[myIndex]) {
          ++myIndex;
        }
        if (other.data[otherIndex] == data[myIndex]) {
          data[newSize++] = data[myIndex];
          ++otherIndex;
          ++myIndex;
        }
      }
    } else {
      while (otherIndex < other.size && myIndex < size) {
        while (myIndex < size && other.data[otherIndex] > data[myIndex]) {
          ++myIndex;
        }
        if (myIndex == size) {
          break;
        }
        while (otherIndex < other.size
            && other.data[otherIndex] < data[myIndex]) {
          ++otherIndex;
        }
        if (other.data[otherIndex] == data[myIndex]) {
          data[newSize++] = data[myIndex];
          ++otherIndex;
          ++myIndex;
        }
      }
    }
    this.size = newSize;
  }

  /**
   * Return the size of the Array. Not the allocated size, but the number of
   * values actually set.
   * 
   * @return the (filled) size of the array
   */
  public int size() {
    return size;
  }

  /**
   * Adds a value to the array.
   * 
   * @param value
   *            value to be added
   */
  public void addToArray(int value) {
    if (size == data.length) {
      int[] newArray = new int[2 * size + 1];
      System.arraycopy(data, 0, newArray, 0, size);
      data = newArray;
    }
    data[size] = value;
    ++size;
    shouldSort = true;
  }

  /**
   * Equals method. Checking the sizes, than the values from the last index to
   * the first (Statistically for random should be the same but for our
   * specific use would find differences faster).
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IntArray)) {
      return false;
    }

    IntArray array = (IntArray) o;
    if (array.size != size) {
      return false;
    }

    sort();
    array.sort();

    boolean equal = true;

    for (int i = size; i > 0 && equal;) {
      --i;
      equal = (array.data[i] == this.data[i]);
    }

    return equal;
  }

  /**
   * Sorts the data. If it is needed.
   */
  public void sort() {
    if (shouldSort) {
      shouldSort = false;
      Arrays.sort(data, 0, size);
    }
  }

  /**
   * Calculates a hash-code for HashTables
   */
  @Override
  public int hashCode() {
    int hash = 0;
    for (int i = 0; i < size; ++i) {
      hash = data[i] ^ (hash * 31);
    }
    return hash;
  }

  /**
   * Get an element from a specific index.
   * 
   * @param i
   *            index of which element should be retrieved.
   */
  public int get(int i) {
    if (i >= size) {
      throw new ArrayIndexOutOfBoundsException(i);
    }
    return this.data[i];
  }

  public void set(int idx, int value) {
    if (idx >= size) {
      throw new ArrayIndexOutOfBoundsException(idx);
    }
    this.data[idx] = value;
  }

  /**
   * toString or not toString. That is the question!
   */
  @Override
  public String toString() {
    String s = "(" + size + ") ";
    for (int i = 0; i < size; ++i) {
      s += "" + data[i] + ", ";
    }
    return s;
  }

  /**
   * Clear the IntArray (set all elements to zero).
   * @param resize - if resize is true, then clear actually allocates
   * a new array of size 0, essentially 'clearing' the array and freeing
   * memory.
   */
  public void clear(boolean resize) {
    init(resize);
  }

}