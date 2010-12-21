package org.apache.solr.spelling.suggest.tst;

import java.io.File;
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
  TernaryTreeNode root;
  TSTAutocomplete autocomplete;

  @Override
  public void init(NamedList config, SolrCore core) {
  }

  @Override
  public void build(TermFreqIterator tfit) throws IOException {
    root = new TernaryTreeNode();
    autocomplete = new TSTAutocomplete();
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
    throw new UnsupportedOperationException("get() is not supported here");
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

  @Override
  public boolean load(File storeDir) throws IOException {
    return false;
  }

  @Override
  public boolean store(File storeDir) throws IOException {
    return false;
  }

}
