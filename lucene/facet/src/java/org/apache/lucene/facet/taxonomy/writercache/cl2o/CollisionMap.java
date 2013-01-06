package org.apache.lucene.facet.taxonomy.writercache.cl2o;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.facet.taxonomy.CategoryPath;

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
 * HashMap to store colliding labels. See {@link CompactLabelToOrdinal} for
 * details.
 * 
 * @lucene.experimental
 */
public class CollisionMap {

  private int capacity;
  private float loadFactor;
  private int size;
  private int threshold;

  static class Entry {
    int offset;
    int cid;
    Entry next;
    int hash;

    Entry(int offset, int cid, int h, Entry e) {
      this.offset = offset;
      this.cid = cid;
      this.next = e;
      this.hash = h;
    }
  }

  private CharBlockArray labelRepository;

  private Entry[] entries;

  CollisionMap(CharBlockArray labelRepository) {
    this(16 * 1024, 0.75f, labelRepository);
  }

  CollisionMap(int initialCapacity, CharBlockArray labelRepository) {
    this(initialCapacity, 0.75f, labelRepository);
  }

  private CollisionMap(int initialCapacity, float loadFactor, CharBlockArray labelRepository) {
    this.labelRepository = labelRepository;
    this.loadFactor = loadFactor;
    this.capacity = CompactLabelToOrdinal.determineCapacity(2, initialCapacity);

    this.entries = new Entry[this.capacity];
    this.threshold = (int) (this.capacity * this.loadFactor);
  }

  public int size() {
    return this.size;
  }

  public int capacity() {
    return this.capacity;
  }

  private void grow() {
    int newCapacity = this.capacity * 2;
    Entry[] newEntries = new Entry[newCapacity];
    Entry[] src = this.entries;

    for (int j = 0; j < src.length; j++) {
      Entry e = src[j];
      if (e != null) {
        src[j] = null;
        do {
          Entry next = e.next;
          int hash = e.hash;
          int i = indexFor(hash, newCapacity);  
          e.next = newEntries[i];
          newEntries[i] = e;
          e = next;
        } while (e != null);
      }
    }

    this.capacity = newCapacity;
    this.entries = newEntries;
    this.threshold = (int) (this.capacity * this.loadFactor);
  }

  public int get(CategoryPath label, int hash) {
    int bucketIndex = indexFor(hash, this.capacity);
    Entry e = this.entries[bucketIndex];

    while (e != null && !(hash == e.hash && CategoryPathUtils.equalsToSerialized(label, labelRepository, e.offset))) { 
      e = e.next;
    }
    if (e == null) {
      return LabelToOrdinal.INVALID_ORDINAL;
    }

    return e.cid;
  }

  public int addLabel(CategoryPath label, int hash, int cid) {
    int bucketIndex = indexFor(hash, this.capacity);
    for (Entry e = this.entries[bucketIndex]; e != null; e = e.next) {
      if (e.hash == hash && CategoryPathUtils.equalsToSerialized(label, labelRepository, e.offset)) {
        return e.cid;
      }
    }

    // new string; add to label repository
    int offset = labelRepository.length();
    CategoryPathUtils.serialize(label, labelRepository);
    addEntry(offset, cid, hash, bucketIndex);
    return cid;
  }

  /**
   * This method does not check if the same value is already in the map because
   * we pass in an char-array offset, so so we now that we're in resize-mode
   * here.
   */
  public void addLabelOffset(int hash, int offset, int cid) {
    int bucketIndex = indexFor(hash, this.capacity);
    addEntry(offset, cid, hash, bucketIndex);
  }

  private void addEntry(int offset, int cid, int hash, int bucketIndex) {
    Entry e = this.entries[bucketIndex];
    this.entries[bucketIndex] = new Entry(offset, cid, hash, e);
    if (this.size++ >= this.threshold) {
      grow();
    }
  }

  Iterator<CollisionMap.Entry> entryIterator() {
    return new EntryIterator(entries, size);
  }

  /**
   * Returns index for hash code h. 
   */
  static int indexFor(int h, int length) {
    return h & (length - 1);
  }

  /**
   * Returns an estimate of the memory usage of this CollisionMap.
   * @return The approximate number of bytes used by this structure.
   */
  int getMemoryUsage() {
    int memoryUsage = 0;
    if (this.entries != null) {
      for (Entry e : this.entries) {
        if (e != null) {
          memoryUsage += (4 * 4);
          for (Entry ee = e.next; ee != null; ee = ee.next) {
            memoryUsage += (4 * 4);
          }
        }
      }
    }
    return memoryUsage;
  }

  private class EntryIterator implements Iterator<Entry> {
    Entry next;    // next entry to return
    int index;        // current slot 
    Entry[] ents;
    
    EntryIterator(Entry[] entries, int size) {
      this.ents = entries;
      Entry[] t = entries;
      int i = t.length;
      Entry n = null;
      if (size != 0) { // advance to first entry
        while (i > 0 && (n = t[--i]) == null) {
          // advance
        }
      }
      this.next = n;
      this.index = i;
    }

    @Override
    public boolean hasNext() {
      return this.next != null;
    }

    @Override
    public Entry next() { 
      Entry e = this.next;
      if (e == null) throw new NoSuchElementException();

      Entry n = e.next;
      Entry[] t = ents;
      int i = this.index;
      while (n == null && i > 0) {
        n = t[--i];
      }
      this.index = i;
      this.next = n;
      return  e;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

}
