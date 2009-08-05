package org.apache.lucene.util;

import org.apache.lucene.util.LuceneTestCase;


/**
 *
 *
 **/
public class ArrayUtilTest extends LuceneTestCase {

  public void testParseInt() throws Exception {
    int test;
    try {
      test = ArrayUtil.parseInt("".toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }
    try {
      test = ArrayUtil.parseInt("foo".toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }
    try {
      test = ArrayUtil.parseInt(String.valueOf(Long.MAX_VALUE).toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }
    try {
      test = ArrayUtil.parseInt("0.34".toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }

    try {
      test = ArrayUtil.parseInt("1".toCharArray());
      assertTrue(test + " does not equal: " + 1, test == 1);
      test = ArrayUtil.parseInt("-10000".toCharArray());
      assertTrue(test + " does not equal: " + -10000, test == -10000);
      test = ArrayUtil.parseInt("1923".toCharArray());
      assertTrue(test + " does not equal: " + 1923, test == 1923);
      test = ArrayUtil.parseInt("-1".toCharArray());
      assertTrue(test + " does not equal: " + -1, test == -1);
      test = ArrayUtil.parseInt("foo 1923 bar".toCharArray(), 4, 4);
      assertTrue(test + " does not equal: " + 1923, test == 1923);
    } catch (NumberFormatException e) {
      e.printStackTrace();
      assertTrue(false);
    }

  }

}
