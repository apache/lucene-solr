package org.apache.lucene.index;

import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.LuceneTestCase;

public class TestCloseableThreadLocal extends LuceneTestCase {
  public static final String TEST_VALUE = "initvaluetest";
  
  public void testInitValue() {
    InitValueThreadLocal tl = new InitValueThreadLocal();
    String str = (String)tl.get();
    assertEquals(TEST_VALUE, str);
  }
  
  public class InitValueThreadLocal extends CloseableThreadLocal {
    protected Object initialValue() {
      return TEST_VALUE;
    } 
  }
}
