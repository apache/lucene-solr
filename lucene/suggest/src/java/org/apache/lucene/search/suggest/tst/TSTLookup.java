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
package org.apache.lucene.search.suggest.tst;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.SortedInputIterator;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.UnicodeUtil;

/**
 * Suggest implementation based on a 
 * <a href="http://en.wikipedia.org/wiki/Ternary_search_tree">Ternary Search Tree</a>
 * 
 * @see TSTAutocomplete
 */
public class TSTLookup extends Lookup {
  TernaryTreeNode root = new TernaryTreeNode();
  TSTAutocomplete autocomplete = new TSTAutocomplete();

  /** Number of entries the lookup was built with */
  private long count = 0;
  
  /** 
   * Creates a new TSTLookup with an empty Ternary Search Tree.
   * @see #build(InputIterator)
   */
  public TSTLookup() {}

  @Override
  public void build(InputIterator iterator) throws IOException {
    if (iterator.hasPayloads()) {
      throw new IllegalArgumentException("this suggester doesn't support payloads");
    }
    if (iterator.hasContexts()) {
      throw new IllegalArgumentException("this suggester doesn't support contexts");
    }
    root = new TernaryTreeNode();

    // make sure it's sorted and the comparator uses UTF16 sort order
    iterator = new SortedInputIterator(iterator, BytesRef.getUTF8SortedAsUTF16Comparator());
    count = 0;
    ArrayList<String> tokens = new ArrayList<>();
    ArrayList<Number> vals = new ArrayList<>();
    BytesRef spare;
    CharsRefBuilder charsSpare = new CharsRefBuilder();
    while ((spare = iterator.next()) != null) {
      charsSpare.copyUTF8Bytes(spare);
      tokens.add(charsSpare.toString());
      vals.add(Long.valueOf(iterator.weight()));
      count++;
    }
    autocomplete.balancedTree(tokens.toArray(), vals.toArray(), 0, tokens.size() - 1, root);
  }

  /** 
   * Adds a new node if <code>key</code> already exists,
   * otherwise replaces its value.
   * <p>
   * This method always returns true.
   */
  public boolean add(CharSequence key, Object value) {
    autocomplete.insert(root, key, value, 0);
    // XXX we don't know if a new node was created
    return true;
  }

  /**
   * Returns the value for the specified key, or null
   * if the key does not exist.
   */
  public Object get(CharSequence key) {
    List<TernaryTreeNode> list = autocomplete.prefixCompletion(root, key, 0);
    if (list == null || list.isEmpty()) {
      return null;
    }
    for (TernaryTreeNode n : list) {
      if (charSeqEquals(n.token, key)) {
        return n.val;
      }
    }
    return null;
  }
  
  private static boolean charSeqEquals(CharSequence left, CharSequence right) {
    int len = left.length();
    if (len != right.length()) {
      return false;
    }
    for (int i = 0; i < len; i++) {
      if (left.charAt(i) != right.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num) {
    if (contexts != null) {
      throw new IllegalArgumentException("this suggester doesn't support contexts");
    }
    List<TernaryTreeNode> list = autocomplete.prefixCompletion(root, key, 0);
    List<LookupResult> res = new ArrayList<>();
    if (list == null || list.size() == 0) {
      return res;
    }
    int maxCnt = Math.min(num, list.size());
    if (onlyMorePopular) {
      LookupPriorityQueue queue = new LookupPriorityQueue(num);
      
      for (TernaryTreeNode ttn : list) {
        queue.insertWithOverflow(new LookupResult(ttn.token, ((Number)ttn.val).longValue()));
      }
      for (LookupResult lr : queue.getResults()) {
        res.add(lr);
      }
    } else {
      for (int i = 0; i < maxCnt; i++) {
        TernaryTreeNode ttn = list.get(i);
        res.add(new LookupResult(ttn.token, ((Number)ttn.val).longValue()));
      }
    }
    return res;
  }
  
  private static final byte LO_KID = 0x01;
  private static final byte EQ_KID = 0x02;
  private static final byte HI_KID = 0x04;
  private static final byte HAS_TOKEN = 0x08;
  private static final byte HAS_VALUE = 0x10;

  // pre-order traversal
  private void readRecursively(DataInput in, TernaryTreeNode node) throws IOException {
    node.splitchar = in.readString().charAt(0);
    byte mask = in.readByte();
    if ((mask & HAS_TOKEN) != 0) {
      node.token = in.readString();
    }
    if ((mask & HAS_VALUE) != 0) {
      node.val = Long.valueOf(in.readLong());
    }
    if ((mask & LO_KID) != 0) {
      node.loKid = new TernaryTreeNode();
      readRecursively(in, node.loKid);
    }
    if ((mask & EQ_KID) != 0) {
      node.eqKid = new TernaryTreeNode();
      readRecursively(in, node.eqKid);
    }
    if ((mask & HI_KID) != 0) {
      node.hiKid = new TernaryTreeNode();
      readRecursively(in, node.hiKid);
    }
  }

  // pre-order traversal
  private void writeRecursively(DataOutput out, TernaryTreeNode node) throws IOException {
    // write out the current node
    out.writeString(new String(new char[] {node.splitchar}, 0, 1));
    // prepare a mask of kids
    byte mask = 0;
    if (node.eqKid != null) mask |= EQ_KID;
    if (node.loKid != null) mask |= LO_KID;
    if (node.hiKid != null) mask |= HI_KID;
    if (node.token != null) mask |= HAS_TOKEN;
    if (node.val != null) mask |= HAS_VALUE;
    out.writeByte(mask);
    if (node.token != null) out.writeString(node.token);
    if (node.val != null) out.writeLong(((Number)node.val).longValue());
    // recurse and write kids
    if (node.loKid != null) {
      writeRecursively(out, node.loKid);
    }
    if (node.eqKid != null) {
      writeRecursively(out, node.eqKid);
    }
    if (node.hiKid != null) {
      writeRecursively(out, node.hiKid);
    }
  }

  @Override
  public synchronized boolean store(DataOutput output) throws IOException {
    output.writeVLong(count);
    writeRecursively(output, root);
    return true;
  }

  @Override
  public synchronized boolean load(DataInput input) throws IOException {
    count = input.readVLong();
    root = new TernaryTreeNode();
    readRecursively(input, root);
    return true;
  }

  /** Returns byte size of the underlying TST */
  @Override
  public long ramBytesUsed() {
    long mem = RamUsageEstimator.shallowSizeOf(this);
    if (root != null) {
      mem += root.sizeInBytes();
    }
    return mem;
  }
  
  @Override
  public long getCount() {
    return count;
  }
}
