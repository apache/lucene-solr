package org.apache.lucene.analysis;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import java.io.StringReader;

public class TestISOLatin1AccentFilter extends BaseTokenStreamTestCase {
  public void testU() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader("Des mot clés À LA CHAÎNE À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ĳ Ð Ñ Ò Ó Ô Õ Ö Ø Œ Þ Ù Ú Û Ü Ý Ÿ à á â ã ä å æ ç è é ê ë ì í î ï ĳ ð ñ ò ó ô õ ö ø œ ß þ ù ú û ü ý ÿ ﬁ ﬂ"));
    ISOLatin1AccentFilter filter = new ISOLatin1AccentFilter(stream);
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    assertTermEquals("Des", filter, termAtt);
    assertTermEquals("mot", filter, termAtt);
    assertTermEquals("cles", filter, termAtt);
    assertTermEquals("A", filter, termAtt);
    assertTermEquals("LA", filter, termAtt);
    assertTermEquals("CHAINE", filter, termAtt);
    assertTermEquals("A", filter, termAtt);
    assertTermEquals("A", filter, termAtt);
    assertTermEquals("A", filter, termAtt);
    assertTermEquals("A", filter, termAtt);
    assertTermEquals("A", filter, termAtt);
    assertTermEquals("A", filter, termAtt);
    assertTermEquals("AE", filter, termAtt);
    assertTermEquals("C", filter, termAtt);
    assertTermEquals("E", filter, termAtt);
    assertTermEquals("E", filter, termAtt);
    assertTermEquals("E", filter, termAtt);
    assertTermEquals("E", filter, termAtt);
    assertTermEquals("I", filter, termAtt);
    assertTermEquals("I", filter, termAtt);
    assertTermEquals("I", filter, termAtt);
    assertTermEquals("I", filter, termAtt);
    assertTermEquals("IJ", filter, termAtt);
    assertTermEquals("D", filter, termAtt);
    assertTermEquals("N", filter, termAtt);
    assertTermEquals("O", filter, termAtt);
    assertTermEquals("O", filter, termAtt);
    assertTermEquals("O", filter, termAtt);
    assertTermEquals("O", filter, termAtt);
    assertTermEquals("O", filter, termAtt);
    assertTermEquals("O", filter, termAtt);
    assertTermEquals("OE", filter, termAtt);
    assertTermEquals("TH", filter, termAtt);
    assertTermEquals("U", filter, termAtt);
    assertTermEquals("U", filter, termAtt);
    assertTermEquals("U", filter, termAtt);
    assertTermEquals("U", filter, termAtt);
    assertTermEquals("Y", filter, termAtt);
    assertTermEquals("Y", filter, termAtt);
    assertTermEquals("a", filter, termAtt);
    assertTermEquals("a", filter, termAtt);
    assertTermEquals("a", filter, termAtt);
    assertTermEquals("a", filter, termAtt);
    assertTermEquals("a", filter, termAtt);
    assertTermEquals("a", filter, termAtt);
    assertTermEquals("ae", filter, termAtt);
    assertTermEquals("c", filter, termAtt);
    assertTermEquals("e", filter, termAtt);
    assertTermEquals("e", filter, termAtt);
    assertTermEquals("e", filter, termAtt);
    assertTermEquals("e", filter, termAtt);
    assertTermEquals("i", filter, termAtt);
    assertTermEquals("i", filter, termAtt);
    assertTermEquals("i", filter, termAtt);
    assertTermEquals("i", filter, termAtt);
    assertTermEquals("ij", filter, termAtt);
    assertTermEquals("d", filter, termAtt);
    assertTermEquals("n", filter, termAtt);
    assertTermEquals("o", filter, termAtt);
    assertTermEquals("o", filter, termAtt);
    assertTermEquals("o", filter, termAtt);
    assertTermEquals("o", filter, termAtt);
    assertTermEquals("o", filter, termAtt);
    assertTermEquals("o", filter, termAtt);
    assertTermEquals("oe", filter, termAtt);
    assertTermEquals("ss", filter, termAtt);
    assertTermEquals("th", filter, termAtt);
    assertTermEquals("u", filter, termAtt);
    assertTermEquals("u", filter, termAtt);
    assertTermEquals("u", filter, termAtt);
    assertTermEquals("u", filter, termAtt);
    assertTermEquals("y", filter, termAtt);
    assertTermEquals("y", filter, termAtt);
    assertTermEquals("fi", filter, termAtt);
    assertTermEquals("fl", filter, termAtt);
    assertFalse(filter.incrementToken());
  }
  
  void assertTermEquals(String expected, TokenStream stream, CharTermAttribute termAtt) throws Exception {
    assertTrue(stream.incrementToken());
    assertEquals(expected, termAtt.toString());
  }
}
