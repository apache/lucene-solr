package org.apache.solr.search;

import junit.framework.TestCase;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Test for FastLRUCache
 *
 * @version $Id$
 * @see org.apache.solr.search.FastLRUCache
 * @since solr 1.4
 */
public class TestFastLRUCache extends TestCase {
  public void testSimple() throws IOException {
    FastLRUCache sc = new FastLRUCache();
    Map l = new HashMap();
    l.put("size", "100");
    l.put("initialSize", "10");
    l.put("autowarmCount", "25");
    CacheRegenerator cr = new CacheRegenerator() {
      public boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache newCache,
                                    SolrCache oldCache, Object oldKey, Object oldVal) throws IOException {
        newCache.put(oldKey, oldVal);
        return true;
      }
    };
    Object o = sc.init(l, null, cr);
    sc.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      sc.put(i + 1, "" + (i + 1));
    }
    assertEquals("25", sc.get(25));
    assertEquals(null, sc.get(110));
    NamedList nl = sc.getStatistics();
    assertEquals(2L, nl.get("lookups"));
    assertEquals(1L, nl.get("hits"));
    assertEquals(101L, nl.get("inserts"));
    assertEquals(11L, nl.get("evictions"));

    FastLRUCache scNew = new FastLRUCache();
    scNew.init(l, o, cr);
    scNew.warm(null, sc);
    scNew.setState(SolrCache.State.LIVE);
    scNew.put(103, "103");
    assertEquals("90", scNew.get(90));
    assertEquals(null, scNew.get(50));
    nl = scNew.getStatistics();
    assertEquals(2L, nl.get("lookups"));
    assertEquals(1L, nl.get("hits"));
    assertEquals(1L, nl.get("inserts"));
    assertEquals(0L, nl.get("evictions"));

    assertEquals(4L, nl.get("cumulative_lookups"));
    assertEquals(2L, nl.get("cumulative_hits"));
    assertEquals(102L, nl.get("cumulative_inserts"));
    assertEquals(11L, nl.get("cumulative_evictions"));
  }

}
