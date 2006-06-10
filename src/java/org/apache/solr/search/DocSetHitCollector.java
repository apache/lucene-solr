package org.apache.solr.search;

import org.apache.lucene.search.HitCollector;
import org.apache.solr.util.OpenBitSet;
import org.apache.solr.core.SolrConfig;

/**
 * @author yonik
 * @version $Id$
 */

final class DocSetHitCollector extends HitCollector {

  static float HASHSET_INVERSE_LOAD_FACTOR = 1.0f / SolrConfig.config.getFloat("//HashDocSet/@loadFactor",0.75f);
  static int HASHDOCSET_MAXSIZE= SolrConfig.config.getInt("//HashDocSet/@maxSize",-1);

  int pos=0;
  OpenBitSet bits;
  final int maxDoc;

  // in case there aren't that many hits, we may not want a very sparse
  // bit array.  Optimistically collect the first few docs in an array
  // in case there are only a few.
  final int[] scratch = new int[HASHDOCSET_MAXSIZE];

  // todo - could pass in bitset and an operation also...
  DocSetHitCollector(int maxDoc) {
    this.maxDoc = maxDoc;
  }

  public void collect(int doc, float score) {
    // optimistically collect the first docs in an array
    // in case the total number will be small enough to represent
    // as a HashDocSet() instead...
    // Storing in this array will be quicker to convert
    // than scanning through a potentially huge bit vector.
    // FUTURE: when search methods all start returning docs in order, maybe
    // we could have a ListDocSet() and use the collected array directly.
    if (pos < scratch.length) {
      scratch[pos]=doc;
    } else {
      // this conditional could be removed if BitSet was preallocated, but that
      // would take up more memory, and add more GC time...
      if (bits==null) bits = new OpenBitSet(maxDoc);
      bits.fastSet(doc);
    }

    pos++;
  }

  public DocSet getDocSet() {
    if (pos<=scratch.length) {
      return new HashDocSet(scratch,0,pos,HASHSET_INVERSE_LOAD_FACTOR);
    } else {
      // set the bits for ids that were collected in the array
      for (int i=0; i<scratch.length; i++) bits.fastSet(scratch[i]);
      return new BitDocSet(bits,pos);
    }
  }
}
