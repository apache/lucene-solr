package org.apache.solr.spelling.suggest.tst;

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
import org.apache.solr.spelling.suggest.SortedTermFreqIteratorWrapper;
import org.apache.solr.util.SortedIterator;
import org.apache.solr.util.TermFreqIterator;

public class TSTLookup extends Lookup {
  TernaryTreeNode root = new TernaryTreeNode();
  TSTAutocomplete autocomplete = new TSTAutocomplete();

  @Override
  public void init(NamedList config, SolrCore core) {
  }

  @Override
  public void build(TermFreqIterator tfit) throws IOException {
    root = new TernaryTreeNode();
    // buffer first
    if (!(tfit instanceof SortedIterator)) {
      // make sure it's sorted
      tfit = new SortedTermFreqIteratorWrapper(tfit);
    }

    ArrayList<String> tokens = new ArrayList<String>();
    ArrayList<Float> vals = new ArrayList<Float>();
    while (tfit.hasNext()) {
      tokens.add(tfit.next());
      vals.add(new Float(tfit.freq()));
    }
    autocomplete.balancedTree(tokens.toArray(), vals.toArray(), 0, tokens.size() - 1, root);
  }

  @Override
  public boolean add(String key, Object value) {
    autocomplete.insert(root, key, value, 0);
    // XXX we don't know if a new node was created
    return true;
  }

  @Override
  public Object get(String key) {
    List<TernaryTreeNode> list = autocomplete.prefixCompletion(root, key, 0);
    if (list == null || list.isEmpty()) {
      return null;
    }
    for (TernaryTreeNode n : list) {
      if (n.token.equals(key)) {
        return n.val;
      }
    }
    return null;
  }

  @Override
  public List<LookupResult> lookup(String key, boolean onlyMorePopular, int num) {
    List<TernaryTreeNode> list = autocomplete.prefixCompletion(root, key, 0);
    List<LookupResult> res = new ArrayList<LookupResult>();
    if (list == null || list.size() == 0) {
      return res;
    }
    int maxCnt = Math.min(num, list.size());
    if (onlyMorePopular) {
      LookupPriorityQueue queue = new LookupPriorityQueue(num);
      for (TernaryTreeNode ttn : list) {
        queue.insertWithOverflow(new LookupResult(ttn.token, (Float)ttn.val));
      }
      for (LookupResult lr : queue.getResults()) {
        res.add(lr);
      }
    } else {
      for (int i = 0; i < maxCnt; i++) {
        TernaryTreeNode ttn = list.get(i);
        res.add(new LookupResult(ttn.token, (Float)ttn.val));
      }
    }
    return res;
  }
  
  public static final String FILENAME = "tst.dat";
  
  private static final byte LO_KID = 0x01;
  private static final byte EQ_KID = 0x02;
  private static final byte HI_KID = 0x04;
  private static final byte HAS_TOKEN = 0x08;
  private static final byte HAS_VALUE = 0x10;

  @Override
  public synchronized boolean load(File storeDir) throws IOException {
    File data = new File(storeDir, FILENAME);
    if (!data.exists() || !data.canRead()) {
      return false;
    }
    DataInputStream in = new DataInputStream(new FileInputStream(data));
    root = new TernaryTreeNode();
    try {
      readRecursively(in, root);
    } finally {
      in.close();
    }
    return true;
  }
  
  // pre-order traversal
  private void readRecursively(DataInputStream in, TernaryTreeNode node) throws IOException {
    node.splitchar = in.readChar();
    byte mask = in.readByte();
    if ((mask & HAS_TOKEN) != 0) {
      node.token = in.readUTF();
    }
    if ((mask & HAS_VALUE) != 0) {
      node.val = new Float(in.readFloat());
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

  @Override
  public synchronized boolean store(File storeDir) throws IOException {
    if (!storeDir.exists() || !storeDir.isDirectory() || !storeDir.canWrite()) {
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
    if (node.val != null) out.writeFloat((Float)node.val);
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
}
