package org.apache.solr.spelling.suggest.fst;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.spelling.suggest.Lookup.LookupResult;
import org.apache.solr.spelling.suggest.LookupBenchmarkTest;
import org.apache.solr.spelling.suggest.TermFreq;
import org.apache.solr.spelling.suggest.TermFreqArrayIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link FSTLookup}.
 */
public class FSTLookupTest extends LuceneTestCase {
  public static TermFreq tf(String t, float v) {
    return new TermFreq(t, v);
  }

  private FSTLookup lookup;

  @Before
  public void prepare() throws Exception {
    final TermFreq[] keys = new TermFreq[] {
        tf("one", 0.5f),
        tf("oneness", 1),
        tf("onerous", 1),
        tf("onesimus", 1),
        tf("two", 1),
        tf("twofold", 1),
        tf("twonk", 1),
        tf("thrive", 1),
        tf("through", 1),
        tf("threat", 1),
        tf("three", 1),
        tf("foundation", 1),
        tf("fourier", 1),
        tf("four", 1),
        tf("fourty", 1),
        tf("xo", 1),
      };

      lookup = new FSTLookup();
      lookup.build(new TermFreqArrayIterator(keys));
  }

  @Test
  public void testExactMatchHighPriority() throws Exception {
    assertMatchEquals(lookup.lookup("two", true, 1), "two/1.0");
  }

  @Test
  public void testExactMatchLowPriority() throws Exception {
    assertMatchEquals(lookup.lookup("one", true, 2), 
        "one/0.0",
        "oneness/1.0");
  }

  @Test
  public void testMiss() throws Exception {
    assertMatchEquals(lookup.lookup("xyz", true, 1));
  }

  @Test
  public void testAlphabeticWithWeights() throws Exception {
    assertEquals(0, lookup.lookup("xyz", false, 1).size());
  }

  @Test
  public void testFullMatchList() throws Exception {
    assertMatchEquals(lookup.lookup("one", true, Integer.MAX_VALUE),
        "oneness/1.0", 
        "onerous/1.0",
        "onesimus/1.0", 
        "one/0.0");
  }

  @Test
  public void testMultilingualInput() throws Exception {
    List<TermFreq> input = LookupBenchmarkTest.readTop50KWiki();

    lookup = new FSTLookup();
    lookup.build(new TermFreqArrayIterator(input));

    for (TermFreq tf : input) {
      assertTrue("Not found: " + tf.term, lookup.get(tf.term) != null);
      assertEquals(tf.term, lookup.lookup(tf.term, true, 1).get(0).key);
    }
  }

  @Test
  public void testEmptyInput() throws Exception {
    lookup = new FSTLookup();
    lookup.build(new TermFreqArrayIterator(new TermFreq[0]));
    
    assertMatchEquals(lookup.lookup("", true, 10));
  }

  @Test
  public void testRandom() throws Exception {
    List<TermFreq> freqs = Lists.newArrayList();
    Random rnd = random;
    for (int i = 0; i < 5000; i++) {
      freqs.add(new TermFreq("" + rnd.nextLong(), rnd.nextInt(100)));
    }
    lookup = new FSTLookup();
    lookup.build(new TermFreqArrayIterator(freqs.toArray(new TermFreq[freqs.size()])));

    for (TermFreq tf : freqs) {
      final String term = tf.term;
      for (int i = 1; i < term.length(); i++) {
        String prefix = term.substring(0, i);
        for (LookupResult lr : lookup.lookup(prefix, true, 10)) {
          Assert.assertTrue(lr.key.startsWith(prefix));
        }
      }
    }
  }

  private void assertMatchEquals(List<LookupResult> res, String... expected) {
    String [] result = new String [res.size()];
    for (int i = 0; i < res.size(); i++)
      result[i] = res.get(i).toString();
    
    if (!Arrays.equals(expected, result)) {
      int colLen = Math.max(maxLen(expected), maxLen(result));
      
      StringBuilder b = new StringBuilder();
      String format = "%" + colLen + "s  " + "%" + colLen + "s\n"; 
      b.append(String.format(Locale.ENGLISH, format, "Expected", "Result"));
      for (int i = 0; i < Math.max(result.length, expected.length); i++) {
        b.append(String.format(Locale.ENGLISH, format, 
            i < expected.length ? expected[i] : "--", 
            i < result.length ? result[i] : "--"));
      }

      System.err.println(b.toString());
      fail("Expected different output:\n" + b.toString());
    }
  }

  private int maxLen(String[] result) {
    int len = 0;
    for (String s : result)
      len = Math.max(len, s.length());
    return len;
  }
}
