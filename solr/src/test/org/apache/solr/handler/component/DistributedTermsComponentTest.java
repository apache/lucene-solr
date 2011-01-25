package org.apache.solr.handler.component;

import org.apache.solr.BaseDistributedSearchTestCase;

/**
 * Test for TermsComponent distributed querying
 *
 * @version $Id$
 * @since solr 1.5
 */
public class DistributedTermsComponentTest extends BaseDistributedSearchTestCase {

  @Override
  public void doTest() throws Exception {
    del("*:*");
    index(id, 18, "b_t", "snake spider shark snail slug seal");
    index(id, 19, "b_t", "snake spider shark snail slug");
    index(id, 20, "b_t", "snake spider shark snail");
    index(id, 21, "b_t", "snake spider shark");
    index(id, 22, "b_t", "snake spider");
    index(id, 23, "b_t", "snake");
    index(id, 24, "b_t", "ant zebra");
    index(id, 25, "b_t", "zebra");
    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);

    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.lower", "s");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "sn", "terms.lower", "sn");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.upper", "sn");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.sort", "index");
    query("qt", "/terms", "shards.qt", "/terms", "terms.limit", 5, "terms", "true", "terms.fl", "b_t", "terms.prefix", "s", "terms.lower", "s", "terms.upper", "sn", "terms.sort", "index");
    query("qt", "/terms", "shards.qt", "/terms", "terms", "true", "terms.fl", "b_t", "terms.sort", "index");
  }
}
