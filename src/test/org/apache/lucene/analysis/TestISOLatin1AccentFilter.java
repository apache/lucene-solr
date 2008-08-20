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

import org.apache.lucene.util.LuceneTestCase;

import java.io.StringReader;

public class TestISOLatin1AccentFilter extends LuceneTestCase {
  public void testU() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(new StringReader("Des mot clés À LA CHAÎNE À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ĳ Ð Ñ Ò Ó Ô Õ Ö Ø Œ Þ Ù Ú Û Ü Ý Ÿ à á â ã ä å æ ç è é ê ë ì í î ï ĳ ð ñ ò ó ô õ ö ø œ ß þ ù ú û ü ý ÿ ﬁ ﬂ"));
    ISOLatin1AccentFilter filter = new ISOLatin1AccentFilter(stream);
    final Token reusableToken = new Token();
    assertEquals("Des", filter.next(reusableToken).term());
    assertEquals("mot", filter.next(reusableToken).term());
    assertEquals("cles", filter.next(reusableToken).term());
    assertEquals("A", filter.next(reusableToken).term());
    assertEquals("LA", filter.next(reusableToken).term());
    assertEquals("CHAINE", filter.next(reusableToken).term());
    assertEquals("A", filter.next(reusableToken).term());
    assertEquals("A", filter.next(reusableToken).term());
    assertEquals("A", filter.next(reusableToken).term());
    assertEquals("A", filter.next(reusableToken).term());
    assertEquals("A", filter.next(reusableToken).term());
    assertEquals("A", filter.next(reusableToken).term());
    assertEquals("AE", filter.next(reusableToken).term());
    assertEquals("C", filter.next(reusableToken).term());
    assertEquals("E", filter.next(reusableToken).term());
    assertEquals("E", filter.next(reusableToken).term());
    assertEquals("E", filter.next(reusableToken).term());
    assertEquals("E", filter.next(reusableToken).term());
    assertEquals("I", filter.next(reusableToken).term());
    assertEquals("I", filter.next(reusableToken).term());
    assertEquals("I", filter.next(reusableToken).term());
    assertEquals("I", filter.next(reusableToken).term());
    assertEquals("IJ", filter.next(reusableToken).term());
    assertEquals("D", filter.next(reusableToken).term());
    assertEquals("N", filter.next(reusableToken).term());
    assertEquals("O", filter.next(reusableToken).term());
    assertEquals("O", filter.next(reusableToken).term());
    assertEquals("O", filter.next(reusableToken).term());
    assertEquals("O", filter.next(reusableToken).term());
    assertEquals("O", filter.next(reusableToken).term());
    assertEquals("O", filter.next(reusableToken).term());
    assertEquals("OE", filter.next(reusableToken).term());
    assertEquals("TH", filter.next(reusableToken).term());
    assertEquals("U", filter.next(reusableToken).term());
    assertEquals("U", filter.next(reusableToken).term());
    assertEquals("U", filter.next(reusableToken).term());
    assertEquals("U", filter.next(reusableToken).term());
    assertEquals("Y", filter.next(reusableToken).term());
    assertEquals("Y", filter.next(reusableToken).term());
    assertEquals("a", filter.next(reusableToken).term());
    assertEquals("a", filter.next(reusableToken).term());
    assertEquals("a", filter.next(reusableToken).term());
    assertEquals("a", filter.next(reusableToken).term());
    assertEquals("a", filter.next(reusableToken).term());
    assertEquals("a", filter.next(reusableToken).term());
    assertEquals("ae", filter.next(reusableToken).term());
    assertEquals("c", filter.next(reusableToken).term());
    assertEquals("e", filter.next(reusableToken).term());
    assertEquals("e", filter.next(reusableToken).term());
    assertEquals("e", filter.next(reusableToken).term());
    assertEquals("e", filter.next(reusableToken).term());
    assertEquals("i", filter.next(reusableToken).term());
    assertEquals("i", filter.next(reusableToken).term());
    assertEquals("i", filter.next(reusableToken).term());
    assertEquals("i", filter.next(reusableToken).term());
    assertEquals("ij", filter.next(reusableToken).term());
    assertEquals("d", filter.next(reusableToken).term());
    assertEquals("n", filter.next(reusableToken).term());
    assertEquals("o", filter.next(reusableToken).term());
    assertEquals("o", filter.next(reusableToken).term());
    assertEquals("o", filter.next(reusableToken).term());
    assertEquals("o", filter.next(reusableToken).term());
    assertEquals("o", filter.next(reusableToken).term());
    assertEquals("o", filter.next(reusableToken).term());
    assertEquals("oe", filter.next(reusableToken).term());
    assertEquals("ss", filter.next(reusableToken).term());
    assertEquals("th", filter.next(reusableToken).term());
    assertEquals("u", filter.next(reusableToken).term());
    assertEquals("u", filter.next(reusableToken).term());
    assertEquals("u", filter.next(reusableToken).term());
    assertEquals("u", filter.next(reusableToken).term());
    assertEquals("y", filter.next(reusableToken).term());
    assertEquals("y", filter.next(reusableToken).term());
    assertEquals("fi", filter.next(reusableToken).term());
    assertEquals("fl", filter.next(reusableToken).term());
    assertNull(filter.next(reusableToken));
  }
}
