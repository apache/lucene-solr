package org.apache.solr.spelling.suggest.jaspell;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.spelling.suggest.Lookup;
import org.apache.solr.spelling.suggest.UnsortedTermFreqIteratorWrapper;
import org.apache.solr.spelling.suggest.jaspell.JaspellTernarySearchTrie.TSTNode;
import org.apache.solr.util.SortedIterator;
import org.apache.solr.util.TermFreqIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaspellLookup extends Lookup {
  private static final Logger LOG = LoggerFactory.getLogger(JaspellLookup.class);
  JaspellTernarySearchTrie trie = new JaspellTernarySearchTrie();
  private boolean usePrefix = true;
  private int editDistance = 2;

  @Override
  public void init(NamedList config, SolrCore core) {
    LOG.info("init: " + config);
  }

  @Override
  public void build(TermFreqIterator tfit) throws IOException {
    if (tfit instanceof SortedIterator) {
      // make sure it's unsorted
      tfit = new UnsortedTermFreqIteratorWrapper(tfit);
    }
    trie = new JaspellTernarySearchTrie();
    trie.setMatchAlmostDiff(editDistance);
    while (tfit.hasNext()) {
      String key = tfit.next();
      float freq = tfit.freq();
      if (key.length() == 0) {
        continue;
      }
      trie.put(key, new Float(freq));
    }
  }

  @Override
  public boolean add(String key, Object value) {
    trie.put(key, value);
    // XXX
    return false;
  }

  @Override
  public Object get(String key) {
    return trie.get(key);
  }

  @Override
  public List<LookupResult> lookup(String key, boolean onlyMorePopular, int num) {
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
        float freq = (Float)trie.get(s);
        queue.insertWithOverflow(new LookupResult(s, freq));
      }
      for (LookupResult lr : queue.getResults()) {
        res.add(lr);
      }
    } else {
      for (int i = 0; i < maxCnt; i++) {
        String s = list.get(i);
        float freq = (Float)trie.get(s);
        res.add(new LookupResult(s, freq));
      }      
    }
    return res;
  }

  public static final String FILENAME = "jaspell.dat";
  private static final byte LO_KID = 0x01;
  private static final byte EQ_KID = 0x02;
  private static final byte HI_KID = 0x04;
  private static final byte HAS_VALUE = 0x08;
 
  
  @Override
  public boolean load(File storeDir) throws IOException {
    File data = new File(storeDir, FILENAME);
    if (!data.exists() || !data.canRead()) {
      return false;
    }
    DataInputStream in = new DataInputStream(new FileInputStream(data));
    TSTNode root = trie.new TSTNode('\0', null);
    try {
      readRecursively(in, root);
      trie.setRoot(root);
    } finally {
      in.close();
    }
    return true;
  }
  
  private void readRecursively(DataInputStream in, TSTNode node) throws IOException {
    node.splitchar = in.readChar();
    byte mask = in.readByte();
    if ((mask & HAS_VALUE) != 0) {
      node.data = new Float(in.readFloat());
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

  @Override
  public boolean store(File storeDir) throws IOException {
    if (!storeDir.exists() || !storeDir.isDirectory() || !storeDir.canWrite()) {
      return false;
    }
    TSTNode root = trie.getRoot();
    if (root == null) { // empty tree
      return false;
    }
    File data = new File(storeDir, FILENAME);
    DataOutputStream out = new DataOutputStream(new FileOutputStream(data));
    try {
      writeRecursively(out, root);
      out.flush();
    } finally {
      out.close();
    }
    return true;
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
      out.writeFloat((Float)node.data);
    }
    writeRecursively(out, node.relatives[TSTNode.LOKID]);
    writeRecursively(out, node.relatives[TSTNode.EQKID]);
    writeRecursively(out, node.relatives[TSTNode.HIKID]);
  }
}
