package org.apache.lucene.search.suggest.tst;

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
import org.apache.lucene.search.suggest.SortedTermFreqIteratorWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
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
  
  /** 
   * Creates a new TSTLookup with an empty Ternary Search Tree.
   * @see #build(TermFreqIterator)
   */
  public TSTLookup() {}

  @Override
  public void build(TermFreqIterator tfit) throws IOException {
    if (tfit instanceof TermFreqPayloadIterator) {
      throw new IllegalArgumentException("this suggester doesn't support payloads");
    }
    root = new TernaryTreeNode();
    // buffer first
    if (tfit.getComparator() != BytesRef.getUTF8SortedAsUTF16Comparator()) {
      // make sure it's sorted and the comparator uses UTF16 sort order
      tfit = new SortedTermFreqIteratorWrapper(tfit, BytesRef.getUTF8SortedAsUTF16Comparator());
    }

    ArrayList<String> tokens = new ArrayList<String>();
    ArrayList<Number> vals = new ArrayList<Number>();
    BytesRef spare;
    CharsRef charsSpare = new CharsRef();
    while ((spare = tfit.next()) != null) {
      charsSpare.grow(spare.length);
      UnicodeUtil.UTF8toUTF16(spare.bytes, spare.offset, spare.length, charsSpare);
      tokens.add(charsSpare.toString());
      vals.add(Long.valueOf(tfit.weight()));
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
  public List<LookupResult> lookup(CharSequence key, boolean onlyMorePopular, int num) {
    List<TernaryTreeNode> list = autocomplete.prefixCompletion(root, key, 0);
    List<LookupResult> res = new ArrayList<LookupResult>();
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
  private void readRecursively(DataInputStream in, TernaryTreeNode node) throws IOException {
    node.splitchar = in.readChar();
    byte mask = in.readByte();
    if ((mask & HAS_TOKEN) != 0) {
      node.token = in.readUTF();
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
  private void writeRecursively(DataOutputStream out, TernaryTreeNode node) throws IOException {
    // write out the current node
    out.writeChar(node.splitchar);
    // prepare a mask of kids
    byte mask = 0;
    if (node.eqKid != null) mask |= EQ_KID;
    if (node.loKid != null) mask |= LO_KID;
    if (node.hiKid != null) mask |= HI_KID;
    if (node.token != null) mask |= HAS_TOKEN;
    if (node.val != null) mask |= HAS_VALUE;
    out.writeByte(mask);
    if (node.token != null) out.writeUTF(node.token);
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
  public synchronized boolean store(OutputStream output) throws IOException {
    DataOutputStream out = new DataOutputStream(output);
    try {
      writeRecursively(out, root);
      out.flush();
    } finally {
      IOUtils.close(output);
    }
    return true;
  }

  @Override
  public synchronized boolean load(InputStream input) throws IOException {
    DataInputStream in = new DataInputStream(input);
    root = new TernaryTreeNode();
    try {
      readRecursively(in, root);
    } finally {
      IOUtils.close(in);
    }
    return true;
  }
  
}
