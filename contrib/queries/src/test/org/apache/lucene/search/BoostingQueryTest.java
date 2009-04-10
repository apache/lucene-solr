package org.apache.lucene.search;

import org.apache.lucene.index.Term;

import junit.framework.TestCase;

public class BoostingQueryTest extends TestCase {
  public void testBoostingQueryEquals() {
    TermQuery q1 = new TermQuery(new Term("subject:", "java"));
    TermQuery q2 = new TermQuery(new Term("subject:", "java"));
    assertEquals("Two TermQueries with same attributes should be equal", q1, q2);
    BoostingQuery bq1 = new BoostingQuery(q1, q2, 0.1f);
    BoostingQuery bq2 = new BoostingQuery(q1, q2, 0.1f);
    assertEquals("BoostingQuery with same attributes is not equal", bq1, bq2);
  }
}
