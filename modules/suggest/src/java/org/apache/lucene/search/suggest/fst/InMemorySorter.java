package org.apache.lucene.search.suggest.fst;

import java.util.*;

import org.apache.lucene.util.BytesRef;

/**
 * An {@link BytesRefSorter} that keeps all the entries in memory.
 */
public final class InMemorySorter implements BytesRefSorter {
  // TODO: use a single byte[] to back up all entries?
  private final ArrayList<BytesRef> refs = new ArrayList<BytesRef>();
  
  private boolean closed = false;

  @Override
  public void add(BytesRef utf8) {
    if (closed) throw new IllegalStateException();
    refs.add(BytesRef.deepCopyOf(utf8));
  }

  @Override
  public Iterator<BytesRef> iterator() {
    closed = true;
    Collections.sort(refs, BytesRef.getUTF8SortedAsUnicodeComparator());
    return Collections.unmodifiableCollection(refs).iterator();
  }
}
