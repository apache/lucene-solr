package org.apache.lucene.analysis;

import junit.framework.TestCase;

import java.io.StringReader;

public class TestISOLatin1AccentFilter extends TestCase {
  public void testU() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(new StringReader("\u00FC"));
    ISOLatin1AccentFilter filter = new ISOLatin1AccentFilter(stream);
    Token token = filter.next();
    assertEquals("u", token.termText);

    assertNull(filter.next());
  }
}
