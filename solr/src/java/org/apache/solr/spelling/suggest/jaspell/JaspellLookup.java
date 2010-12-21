package org.apache.solr.spelling.suggest.jaspell;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.spelling.suggest.Lookup;
import org.apache.solr.spelling.suggest.UnsortedTermFreqIteratorWrapper;
import org.apache.solr.util.SortedIterator;
import org.apache.solr.util.TermFreqIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaspellLookup extends Lookup {
  private static final Logger LOG = LoggerFactory.getLogger(JaspellLookup.class);
  JaspellTernarySearchTrie trie;
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

  @Override
  public boolean load(File storeDir) throws IOException {
    return false;
  }

  @Override
  public boolean store(File storeDir) throws IOException {
    return false;    
  }

}
