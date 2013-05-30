package org.apache.lucene.search.suggest.jaspell;

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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.search.spell.TermFreqPayloadIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.UnsortedTermFreqIteratorWrapper;
import org.apache.lucene.search.suggest.jaspell.JaspellTernarySearchTrie.TSTNode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.UnicodeUtil;

/**
 * Suggest implementation based on 
 * <a href="http://jaspell.sourceforge.net/">JaSpell</a>.
 * 
 * @see JaspellTernarySearchTrie
 */
public class JaspellLookup extends Lookup {
  JaspellTernarySearchTrie trie = new JaspellTernarySearchTrie();
  private boolean usePrefix = true;
  private int editDistance = 2;
  
  /** 
   * Creates a new empty trie 
   * @see #build(TermFreqIterator)
   * */
  public JaspellLookup() {}

  @Override
  public void build(TermFreqIterator tfit) throws IOException {
    if (tfit instanceof TermFreqPayloadIterator) {
      throw new IllegalArgumentException("this suggester doesn't support payloads");
    }
    if (tfit.getComparator() != null) {
      // make sure it's unsorted
      // WTF - this could result in yet another sorted iteration....
      tfit = new UnsortedTermFreqIteratorWrapper(tfit);
    }
    trie = new JaspellTernarySearchTrie();
    trie.setMatchAlmostDiff(editDistance);
    BytesRef spare;
    final CharsRef charsSpare = new CharsRef();

    while ((spare = tfit.next()) != null) {
      final long weight = tfit.weight();
      if (spare.length == 0) {
        continue;
      }
      charsSpare.grow(spare.length);
      UnicodeUtil.UTF8toUTF16(spare.bytes, spare.offset, spare.length, charsSpare);
      trie.put(charsSpare.toString(), Long.valueOf(weight));
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
  public List<LookupResult> lookup(CharSequence key, boolean onlyMorePopular, int num) {
    List<LookupResult> res = new ArrayList<LookupResult>();
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
 
  private void readRecursively(DataInputStream in, TSTNode node) throws IOException {
    node.splitchar = in.readChar();
    byte mask = in.readByte();
    if ((mask & HAS_VALUE) != 0) {
      node.data = Long.valueOf(in.readLong());
    }
    if ((mask & LO_KID) != 0) {
      TSTNode kid = trie.new TSTNode('\0', node);
      node.relatives[TSTNode.LOKID] = kid;
      readRecursively(in, kid);
    }
    if ((mask & EQ_KID) != 0) {
      TSTNode kid = trie.new TSTNode('\0', node);
      node.relatives[TSTNode.EQKID] = kid;
      readRecursively(in, kid);
    }
    if ((mask & HI_KID) != 0) {
      TSTNode kid = trie.new TSTNode('\0', node);
      node.relatives[TSTNode.HIKID] = kid;
      readRecursively(in, kid);
    }
  }

  private void writeRecursively(DataOutputStream out, TSTNode node) throws IOException {
    if (node == null) {
      return;
    }
    out.writeChar(node.splitchar);
    byte mask = 0;
    if (node.relatives[TSTNode.LOKID] != null) mask |= LO_KID;
    if (node.relatives[TSTNode.EQKID] != null) mask |= EQ_KID;
    if (node.relatives[TSTNode.HIKID] != null) mask |= HI_KID;
    if (node.data != null) mask |= HAS_VALUE;
    out.writeByte(mask);
    if (node.data != null) {
      out.writeLong(((Number)node.data).longValue());
    }
    writeRecursively(out, node.relatives[TSTNode.LOKID]);
    writeRecursively(out, node.relatives[TSTNode.EQKID]);
    writeRecursively(out, node.relatives[TSTNode.HIKID]);
  }

  @Override
  public boolean store(OutputStream output) throws IOException {
    TSTNode root = trie.getRoot();
    if (root == null) { // empty tree
      return false;
    }
    DataOutputStream out = new DataOutputStream(output);
    try {
      writeRecursively(out, root);
      out.flush();
    } finally {
      IOUtils.close(out);
    }
    return true;
  }

  @Override
  public boolean load(InputStream input) throws IOException {
    DataInputStream in = new DataInputStream(input);
    TSTNode root = trie.new TSTNode('\0', null);
    try {
      readRecursively(in, root);
      trie.setRoot(root);
    } finally {
      IOUtils.close(in);
    }
    return true;
  }
}
