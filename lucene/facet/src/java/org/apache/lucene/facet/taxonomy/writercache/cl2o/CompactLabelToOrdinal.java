package org.apache.lucene.facet.taxonomy.writercache.cl2o;

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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.facet.taxonomy.CategoryPath;

/**
 * This is a very efficient LabelToOrdinal implementation that uses a
 * CharBlockArray to store all labels and a configurable number of HashArrays to
 * reference the labels.
 * <p>
 * Since the HashArrays don't handle collisions, a {@link CollisionMap} is used
 * to store the colliding labels.
 * <p>
 * This data structure grows by adding a new HashArray whenever the number of
 * collisions in the {@link CollisionMap} exceeds {@code loadFactor} * 
 * {@link #getMaxOrdinal()}. Growing also includes reinserting all colliding
 * labels into the HashArrays to possibly reduce the number of collisions.
 * 
 * For setting the {@code loadFactor} see 
 * {@link #CompactLabelToOrdinal(int, float, int)}. 
 * 
 * <p>
 * This data structure has a much lower memory footprint (~30%) compared to a
 * Java HashMap&lt;String, Integer&gt;. It also only uses a small fraction of objects
 * a HashMap would use, thus limiting the GC overhead. Ingestion speed was also
 * ~50% faster compared to a HashMap for 3M unique labels.
 * 
 * @lucene.experimental
 */
public class CompactLabelToOrdinal extends LabelToOrdinal {

  public static final float DefaultLoadFactor = 0.15f;

  static final char TERMINATOR_CHAR = 0xffff;
  private static final int COLLISION = -5;

  private HashArray[] hashArrays;
  private CollisionMap collisionMap;
  private CharBlockArray labelRepository;

  private int capacity;
  private int threshold;
  private float loadFactor;

  public int sizeOfMap() {
    return this.collisionMap.size();
  }

  private CompactLabelToOrdinal() {
  }

  public CompactLabelToOrdinal(int initialCapacity, float loadFactor,
                                int numHashArrays) {

    this.hashArrays = new HashArray[numHashArrays];

    this.capacity = determineCapacity((int) Math.pow(2, numHashArrays),
        initialCapacity);
    init();
    this.collisionMap = new CollisionMap(this.labelRepository);

    this.counter = 0;
    this.loadFactor = loadFactor;

    this.threshold = (int) (this.loadFactor * this.capacity);
  }

  static int determineCapacity(int minCapacity, int initialCapacity) {
    int capacity = minCapacity;
    while (capacity < initialCapacity) {
      capacity <<= 1;
    }
    return capacity;
  }

  private void init() {
    labelRepository = new CharBlockArray();
    CategoryPathUtils.serialize(CategoryPath.EMPTY, labelRepository);

    int c = this.capacity;
    for (int i = 0; i < this.hashArrays.length; i++) {
      this.hashArrays[i] = new HashArray(c);
      c /= 2;
    }
  }

  @Override
  public void addLabel(CategoryPath label, int ordinal) {
    if (collisionMap.size() > threshold) {
      grow();
    }

    int hash = CompactLabelToOrdinal.stringHashCode(label);
    for (int i = 0; i < this.hashArrays.length; i++) {
      if (addLabel(this.hashArrays[i], label, hash, ordinal)) {
        return;
      }
    }

    int prevVal = collisionMap.addLabel(label, hash, ordinal);
    if (prevVal != ordinal) {
      throw new IllegalArgumentException("Label already exists: " + label.toString('/') + " prev ordinal " + prevVal);
    }
  }

  @Override
  public int getOrdinal(CategoryPath label) {
    if (label == null) {
      return LabelToOrdinal.INVALID_ORDINAL;
    }

    int hash = CompactLabelToOrdinal.stringHashCode(label);
    for (int i = 0; i < this.hashArrays.length; i++) {
      int ord = getOrdinal(this.hashArrays[i], label, hash);
      if (ord != COLLISION) {
        return ord;
      }
    }

    return this.collisionMap.get(label, hash);
  }

  private void grow() {
    HashArray temp = this.hashArrays[this.hashArrays.length - 1];

    for (int i = this.hashArrays.length - 1; i > 0; i--) {
      this.hashArrays[i] = this.hashArrays[i - 1];
    }

    this.capacity *= 2;
    this.hashArrays[0] = new HashArray(this.capacity);

    for (int i = 1; i < this.hashArrays.length; i++) {
      int[] sourceOffsetArray = this.hashArrays[i].offsets;
      int[] sourceCidsArray = this.hashArrays[i].cids;

      for (int k = 0; k < sourceOffsetArray.length; k++) {

        for (int j = 0; j < i && sourceOffsetArray[k] != 0; j++) {
          int[] targetOffsetArray = this.hashArrays[j].offsets;
          int[] targetCidsArray = this.hashArrays[j].cids;

          int newIndex = indexFor(stringHashCode(
              this.labelRepository, sourceOffsetArray[k]),
              targetOffsetArray.length);
          if (targetOffsetArray[newIndex] == 0) {
            targetOffsetArray[newIndex] = sourceOffsetArray[k];
            targetCidsArray[newIndex] = sourceCidsArray[k];
            sourceOffsetArray[k] = 0;
          }
        }
      }
    }

    for (int i = 0; i < temp.offsets.length; i++) {
      int offset = temp.offsets[i];
      if (offset > 0) {
        int hash = stringHashCode(this.labelRepository, offset);
        addLabelOffset(hash, temp.cids[i], offset);
      }
    }

    CollisionMap oldCollisionMap = this.collisionMap;
    this.collisionMap = new CollisionMap(oldCollisionMap.capacity(),
        this.labelRepository);
    this.threshold = (int) (this.capacity * this.loadFactor);

    Iterator<CollisionMap.Entry> it = oldCollisionMap.entryIterator();
    while (it.hasNext()) {
      CollisionMap.Entry e = it.next();
      addLabelOffset(stringHashCode(this.labelRepository, e.offset),
          e.cid, e.offset);
    }
  }

  private boolean addLabel(HashArray a, CategoryPath label, int hash, int ordinal) {
    int index = CompactLabelToOrdinal.indexFor(hash, a.offsets.length);
    int offset = a.offsets[index];

    if (offset == 0) {
      a.offsets[index] = this.labelRepository.length();
      CategoryPathUtils.serialize(label, labelRepository);
      a.cids[index] = ordinal;
      return true;
    }

    return false;
  }

  private void addLabelOffset(int hash, int cid, int knownOffset) {
    for (int i = 0; i < this.hashArrays.length; i++) {
      if (addLabelOffsetToHashArray(this.hashArrays[i], hash, cid,
          knownOffset)) {
        return;
      }
    }

    this.collisionMap.addLabelOffset(hash, knownOffset, cid);

    if (this.collisionMap.size() > this.threshold) {
      grow();
    }
  }

  private boolean addLabelOffsetToHashArray(HashArray a, int hash, int ordinal,
                                            int knownOffset) {

    int index = CompactLabelToOrdinal.indexFor(hash, a.offsets.length);
    int offset = a.offsets[index];

    if (offset == 0) {
      a.offsets[index] = knownOffset;
      a.cids[index] = ordinal;
      return true;
    }

    return false;
  }

  private int getOrdinal(HashArray a, CategoryPath label, int hash) {
    if (label == null) {
      return LabelToOrdinal.INVALID_ORDINAL;
    }

    int index = indexFor(hash, a.offsets.length);
    int offset = a.offsets[index];
    if (offset == 0) {
      return LabelToOrdinal.INVALID_ORDINAL;
    }

    if (CategoryPathUtils.equalsToSerialized(label, labelRepository, offset)) {
      return a.cids[index];
    }

    return COLLISION;
  }

  /** Returns index for hash code h. */
  static int indexFor(int h, int length) {
    return h & (length - 1);
  }

  // static int stringHashCode(String label) {
  // int len = label.length();
  // int hash = 0;
  // int i;
  // for (i = 0; i < len; ++i)
  // hash = 33 * hash + label.charAt(i);
  //
  // hash = hash ^ ((hash >>> 20) ^ (hash >>> 12));
  // hash = hash ^ (hash >>> 7) ^ (hash >>> 4);
  //
  // return hash;
  //
  // }

  static int stringHashCode(CategoryPath label) {
    int hash = label.hashCode();

    hash = hash ^ ((hash >>> 20) ^ (hash >>> 12));
    hash = hash ^ (hash >>> 7) ^ (hash >>> 4);

    return hash;

  }

  static int stringHashCode(CharBlockArray labelRepository, int offset) {
    int hash = CategoryPathUtils.hashCodeOfSerialized(labelRepository, offset);
    hash = hash ^ ((hash >>> 20) ^ (hash >>> 12));
    hash = hash ^ (hash >>> 7) ^ (hash >>> 4);
    return hash;
  }

  // public static boolean equals(CharSequence label, CharBlockArray array,
  // int offset) {
  // // CONTINUE HERE
  // int len = label.length();
  // int bi = array.blockIndex(offset);
  // CharBlockArray.Block b = array.blocks.get(bi);
  // int index = array.indexInBlock(offset);
  //
  // for (int i = 0; i < len; i++) {
  // if (label.charAt(i) != b.chars[index]) {
  // return false;
  // }
  // index++;
  // if (index == b.length) {
  // b = array.blocks.get(++bi);
  // index = 0;
  // }
  // }
  //
  // return b.chars[index] == TerminatorChar;
  // }

  /**
   * Returns an estimate of the amount of memory used by this table. Called only in
   * this package. Memory is consumed mainly by three structures: the hash arrays,
   * label repository and collision map.
   */
  int getMemoryUsage() {
    int memoryUsage = 0;
    if (this.hashArrays != null) {
      // HashArray capacity is instance-specific.
      for (HashArray ha : this.hashArrays) {
        // Each has 2 capacity-length arrays of ints.
        memoryUsage += ( ha.capacity * 2 * 4 ) + 4;
      }
    }
    if (this.labelRepository != null) {
      // All blocks are the same size.
      int blockSize = this.labelRepository.blockSize;
      // Each block has room for blockSize UTF-16 chars.
      int actualBlockSize = ( blockSize * 2 ) + 4;
      memoryUsage += this.labelRepository.blocks.size() * actualBlockSize; 
      memoryUsage += 8;   // Two int values for array as a whole.
    }
    if (this.collisionMap != null) {
      memoryUsage += this.collisionMap.getMemoryUsage();
    }
    return memoryUsage;
  }

  /**
   * Opens the file and reloads the CompactLabelToOrdinal. The file it expects
   * is generated from the {@link #flush(File)} command.
   */
  static CompactLabelToOrdinal open(File file, float loadFactor,
                                    int numHashArrays) throws IOException {
    /**
     * Part of the file is the labelRepository, which needs to be rehashed
     * and label offsets re-added to the object. I am unsure as to why we
     * can't just store these off in the file as well, but in keeping with
     * the spirit of the original code, I did it this way. (ssuppe)
     */
    CompactLabelToOrdinal l2o = new CompactLabelToOrdinal();
    l2o.loadFactor = loadFactor;
    l2o.hashArrays = new HashArray[numHashArrays];

    DataInputStream dis = null;
    try {
      dis = new DataInputStream(new BufferedInputStream(
          new FileInputStream(file)));

      // TaxiReader needs to load the "counter" or occupancy (L2O) to know
      // the next unique facet. we used to load the delimiter too, but
      // never used it.
      l2o.counter = dis.readInt();

      l2o.capacity = determineCapacity((int) Math.pow(2,
          l2o.hashArrays.length), l2o.counter);
      l2o.init();

      // now read the chars
      l2o.labelRepository = CharBlockArray.open(dis);

      l2o.collisionMap = new CollisionMap(l2o.labelRepository);

      // Calculate hash on the fly based on how CategoryPath hashes
      // itself. Maybe in the future we can call some static based methods
      // in CategoryPath so that this doesn't break again? I don't like
      // having code in two different places...
      int cid = 0;
      // Skip the initial offset, it's the CategoryPath(0,0), which isn't
      // a hashed value.
      int offset = 1;
      int lastStartOffset = offset;
      // This loop really relies on a well-formed input (assumes pretty blindly
      // that array offsets will work).  Since the initial file is machine 
      // generated, I think this should be OK.
      while (offset < l2o.labelRepository.length()) {
        // identical code to CategoryPath.hashFromSerialized. since we need to
        // advance offset, we cannot call the method directly. perhaps if we
        // could pass a mutable Integer or something...
        int length = (short) l2o.labelRepository.charAt(offset++);
        int hash = length;
        if (length != 0) {
          for (int i = 0; i < length; i++) {
            int len = (short) l2o.labelRepository.charAt(offset++);
            hash = hash * 31 + l2o.labelRepository.subSequence(offset, offset + len).hashCode();
            offset += len;
          }
        }
        // Now that we've hashed the components of the label, do the
        // final part of the hash algorithm.
        hash = hash ^ ((hash >>> 20) ^ (hash >>> 12));
        hash = hash ^ (hash >>> 7) ^ (hash >>> 4);
        // Add the label, and let's keep going
        l2o.addLabelOffset(hash, cid, lastStartOffset);
        cid++;
        lastStartOffset = offset;
      }

    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Invalid file format. Cannot deserialize.");
    } finally {
      if (dis != null) {
        dis.close();
      }
    }

    l2o.threshold = (int) (l2o.loadFactor * l2o.capacity);
    return l2o;

  }

  void flush(File file) throws IOException {
    FileOutputStream fos = new FileOutputStream(file);

    try {
      BufferedOutputStream os = new BufferedOutputStream(fos);

      DataOutputStream dos = new DataOutputStream(os);
      dos.writeInt(this.counter);

      // write the labelRepository
      this.labelRepository.flush(dos);

      // Closes the data output stream
      dos.close();

    } finally {
      fos.close();
    }
  }

  private static final class HashArray {
    int[] offsets;
    int[] cids;

    int capacity;

    HashArray(int c) {
      this.capacity = c;
      this.offsets = new int[this.capacity];
      this.cids = new int[this.capacity];
    }
  }
}
