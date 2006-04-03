package org.apache.solr.search;

import org.apache.lucene.search.HitCollector;

import java.util.BitSet;

/**
 * @author yonik
 * @version $Id$
 */

final class DocSetHitCollector extends HitCollector {
  int pos=0;
  BitSet bits;
  final int maxDoc;

  // in case there aren't that many hits, we may not want a very sparse
  // bit array.  Optimistically collect the first few docs in an array
  // in case there are only a few.
  static final int ARRAY_COLLECT_SZ=HashDocSet.MAX_SIZE;
  final int[] scratch = ARRAY_COLLECT_SZ>0 ? new int[ARRAY_COLLECT_SZ] : null;

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
    if (pos < ARRAY_COLLECT_SZ) {
      scratch[pos]=doc;
    } else {
      // this conditional could be removed if BitSet was preallocated, but that
      // would take up more memory, and add more GC time...
      if (bits==null) bits = new BitSet(maxDoc);
      bits.set(doc);
    }

    pos++;
  }

  public DocSet getDocSet() {
    if (pos<=ARRAY_COLLECT_SZ) {
      return new HashDocSet(scratch,0,pos);
    } else {
      // set the bits for ids that were collected in the array
      for (int i=0; i<ARRAY_COLLECT_SZ; i++) bits.set(scratch[i]);
      return new BitDocSet(bits,pos);
    }
  }
}
