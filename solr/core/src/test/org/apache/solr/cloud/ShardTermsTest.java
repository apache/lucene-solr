package org.apache.solr.cloud;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.cloud.ShardTerms;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ShardTermsTest extends SolrTestCase {
  @Test
  public void testIncreaseTerms() {
    Map<String, Long> map = new HashMap<>();
    map.put("leader", 0L);
    ShardTerms terms = new ShardTerms(map, 0);
    terms = terms.increaseTerms("leader", Collections.singleton("replica"));
    assertEquals(1L, terms.getTerm("leader").longValue());

    map.put("leader", 2L);
    map.put("live-replica", 2L);
    map.put("dead-replica", 1L);
    terms = new ShardTerms(map, 0);
    assertNull(terms.increaseTerms("leader", Collections.singleton("dead-replica")));

    terms = terms.increaseTerms("leader", Collections.singleton("leader"));
    assertEquals(3L, terms.getTerm("live-replica").longValue());
    assertEquals(2L, terms.getTerm("leader").longValue());
    assertEquals(1L, terms.getTerm("dead-replica").longValue());
  }
}
