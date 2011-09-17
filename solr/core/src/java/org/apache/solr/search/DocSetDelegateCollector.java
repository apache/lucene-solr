package org.apache.solr.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;

/**
 *
 */
public class DocSetDelegateCollector extends DocSetCollector {
  final Collector collector;

  public DocSetDelegateCollector(int smallSetSize, int maxDoc, Collector collector) {
    super(smallSetSize, maxDoc);
    this.collector = collector;
  }

  @Override
  public void collect(int doc) throws IOException {
    collector.collect(doc);

    doc += base;
    // optimistically collect the first docs in an array
    // in case the total number will be small enough to represent
    // as a small set like SortedIntDocSet instead...
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

  @Override
  public DocSet getDocSet() {
    if (pos<=scratch.length) {
      // assumes docs were collected in sorted order!
      return new SortedIntDocSet(scratch, pos);
    } else {
      // set the bits for ids that were collected in the array
      for (int i=0; i<scratch.length; i++) bits.fastSet(scratch[i]);
      return new BitDocSet(bits,pos);
    }
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    collector.setScorer(scorer);
  }

  @Override
  public void setNextReader(IndexReader.AtomicReaderContext context) throws IOException {
    collector.setNextReader(context);
    this.base = context.docBase;
  }
}
