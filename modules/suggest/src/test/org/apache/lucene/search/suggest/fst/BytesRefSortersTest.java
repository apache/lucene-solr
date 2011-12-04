package org.apache.lucene.search.suggest.fst;

import java.util.Iterator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class BytesRefSortersTest extends LuceneTestCase {
  @Test
  public void testExternalRefSorter() throws Exception {
    check(new ExternalRefSorter(new Sort()));
  }

  @Test
  public void testInMemorySorter() throws Exception {
    check(new InMemorySorter());
  }

  private void check(BytesRefSorter sorter) throws Exception {
    for (int i = 0; i < 100; i++) {
      byte [] current = new byte [random.nextInt(256)];
      random.nextBytes(current);
      sorter.add(new BytesRef(current));
    }

    // Create two iterators and check that they're aligned with each other.
    Iterator<BytesRef> i1 = sorter.iterator();
    Iterator<BytesRef> i2 = sorter.iterator();
    
    // Verify sorter contract.
    try {
      sorter.add(new BytesRef(new byte [1]));
      fail("expected contract violation.");
    } catch (IllegalStateException e) {
      // Expected.
    }

    while (i1.hasNext() && i2.hasNext()) {
      assertEquals(i1.next(), i2.next());
    }
    assertEquals(i1.hasNext(), i2.hasNext());
  }  
}
