package org.apache.lucene.index;

import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;

import java.util.stream.IntStream;

/**
 * Unit test for {@link BufferedUpdates}
 */
public class TestBufferedUpdates extends LuceneTestCase {
  /**
   * return a term that maybe duplicated with pre
   */
  private static Term mayDuplicate(int bound) {
    boolean shouldDuplicated = bound > 3 && random().nextBoolean();
    if (shouldDuplicated) {
      return new Term("myField", String.valueOf(random().nextInt(bound)));
    }
    return new Term("myField", String.valueOf(bound));
  }

  public void testRamBytesUsed() {
    BufferedUpdates bu = new BufferedUpdates("seg1");
    assertEquals(bu.ramBytesUsed(), 0L);
    assertFalse(bu.any());
    IntStream.range(0, random().nextInt(atLeast(200))).forEach(id -> {
      int reminder = random().nextInt(3);
      if (reminder == 0) {
        bu.addDocID(id);
      } else if (reminder == 1) {
        bu.addQuery(new TermQuery(mayDuplicate(id)), id);
      } else if (reminder == 2) {
        bu.addTerm((mayDuplicate(id)), id);
      }
    });
    assertTrue("we have added tons of docIds, terms and queries", bu.any());

    long totalUsed = bu.ramBytesUsed();
    assertTrue(totalUsed > 0);

    bu.clearDeletedDocIds();
    assertTrue("only docIds are cleaned, buffer shouldn't be empty", bu.any());
    assertTrue("docIds are cleaned, ram in used should decrease", totalUsed > bu.ramBytesUsed());
    totalUsed = bu.ramBytesUsed();

    bu.clearDeleteTerms();
    assertTrue("only terms and docIds are cleaned, the queries are still in memory", bu.any());
    assertTrue("terms are cleaned, ram in used should decrease", totalUsed > bu.ramBytesUsed());

    bu.clear();
    assertFalse(bu.any());
    assertEquals(bu.ramBytesUsed(), 0L);
  }
}
