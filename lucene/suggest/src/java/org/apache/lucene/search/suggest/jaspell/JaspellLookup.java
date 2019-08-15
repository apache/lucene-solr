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
package org.apache.lucene.search.suggest.jaspell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;

/**
 * Suggest implementation based on 
 * <a href="http://jaspell.sourceforge.net/">JaSpell</a>.
 * 
 * @see JaspellTernarySearchTrie
 * @deprecated Migrate to one of the newer suggesters which are much more RAM efficient.
 */
@Deprecated
@SuppressWarnings("deprecation")
public class JaspellLookup extends Lookup implements Accountable {
  JaspellTernarySearchTrie trie = new JaspellTernarySearchTrie();
  private boolean usePrefix = true;
  private int editDistance = 2;

  /** Number of entries the lookup was built with */
  private long count = 0;
  
  /** 
   * Creates a new empty trie 
   * @see #build(InputIterator)
   * */
  public JaspellLookup() {}

  @Override
  public void build(InputIterator iterator) throws IOException {
    if (iterator.hasPayloads()) {
      throw new IllegalArgumentException("this suggester doesn't support payloads");
    }
    if (iterator.hasContexts()) {
      throw new IllegalArgumentException("this suggester doesn't support contexts");
    }
    count = 0;
    trie = new JaspellTernarySearchTrie();
    trie.setMatchAlmostDiff(editDistance);
    BytesRef spare;
    final CharsRefBuilder charsSpare = new CharsRefBuilder();

    while ((spare = iterator.next()) != null) {
      final long weight = iterator.weight();
      if (spare.length == 0) {
        continue;
      }
      charsSpare.copyUTF8Bytes(spare);
      trie.put(charsSpare.toString(), Long.valueOf(weight));
      count++;
    }
  }

  /** 
   * Adds a new node if <code>key</code> already exists,
   * otherwise replaces its value.
   * <p>
   * This method always returns false.
   */
  public boolean add(CharSequence key, Object value) {
    trie.put(key, value);
    // XXX
    return false;
  }

  /**
   * Returns the value for the specified key, or null
   * if the key does not exist.
   */
  public Object get(CharSequence key) {
    return trie.get(key);
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num) {
    if (contexts != null) {
      throw new IllegalArgumentException("this suggester doesn't support contexts");
    }
    List<LookupResult> res = new ArrayList<>();
    List<String> list;
    int count = onlyMorePopular ? num * 2 : num;
    if (usePrefix) {
      list = trie.matchPrefix(key, count);
    } else {
      list = trie.matchAlmost(key, count);
    }
    if (list == null || list.size() == 0) {
      return res;
      
    }
    int maxCnt = Math.min(num, list.size());
    if (onlyMorePopular) {
      LookupPriorityQueue queue = new LookupPriorityQueue(num);
      for (String s : list) {
        long freq = ((Number)trie.get(s)).longValue();
        queue.insertWithOverflow(new LookupResult(new CharsRef(s), freq));
      }
      for (LookupResult lr : queue.getResults()) {
        res.add(lr);
      }
    } else {
      for (int i = 0; i < maxCnt; i++) {
        String s = list.get(i);
        long freq = ((Number)trie.get(s)).longValue();
        res.add(new LookupResult(new CharsRef(s), freq));
      }      
    }
    return res;
  }

  private static final byte LO_KID = 0x01;
  private static final byte EQ_KID = 0x02;
  private static final byte HI_KID = 0x04;
  private static final byte HAS_VALUE = 0x08;
 
  private void readRecursively(DataInput in, org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode node) throws IOException {
    node.splitchar = in.readString().charAt(0);
    byte mask = in.readByte();
    if ((mask & HAS_VALUE) != 0) {
      node.data = Long.valueOf(in.readLong());
    }
    if ((mask & LO_KID) != 0) {
      org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode kid = new org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode('\0', node);
      node.relatives[org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode.LOKID] = kid;
      readRecursively(in, kid);
    }
    if ((mask & EQ_KID) != 0) {
      org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode kid = new org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode('\0', node);
      node.relatives[org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode.EQKID] = kid;
      readRecursively(in, kid);
    }
    if ((mask & HI_KID) != 0) {
      org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode kid = new org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode('\0', node);
      node.relatives[org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode.HIKID] = kid;
      readRecursively(in, kid);
    }
  }

  private void writeRecursively(DataOutput out, org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode node) throws IOException {
    if (node == null) {
      return;
    }
    out.writeString(new String(new char[] {node.splitchar}, 0, 1));
    byte mask = 0;
    if (node.relatives[org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode.LOKID] != null) mask |= LO_KID;
    if (node.relatives[org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode.EQKID] != null) mask |= EQ_KID;
    if (node.relatives[org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode.HIKID] != null) mask |= HI_KID;
    if (node.data != null) mask |= HAS_VALUE;
    out.writeByte(mask);
    if (node.data != null) {
      out.writeLong(((Number)node.data).longValue());
    }
    writeRecursively(out, node.relatives[org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode.LOKID]);
    writeRecursively(out, node.relatives[org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode.EQKID]);
    writeRecursively(out, node.relatives[org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode.HIKID]);
  }

  @Override
  public boolean store(DataOutput output) throws IOException {
    output.writeVLong(count);
    org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode root = trie.getRoot();
    if (root == null) { // empty tree
      return false;
    }
    writeRecursively(output, root);
    return true;
  }

  @Override
  public boolean load(DataInput input) throws IOException {
    count = input.readVLong();
    org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode root = new org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode('\0', null);
    readRecursively(input, root);
    trie.setRoot(root);
    return true;
  }

  @Override
  public long ramBytesUsed() {
    return trie.ramBytesUsed();
  }
  
  @Override
  public long getCount() {
    return count;
  }
}
