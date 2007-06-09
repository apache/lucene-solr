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

import junit.framework.TestCase;

import java.io.StringReader;

public class TestISOLatin1AccentFilter extends TestCase {
  public void testU() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(new StringReader("Des mot clés À LA CHAÎNE À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ð Ñ Ò Ó Ô Õ Ö Ø Œ Þ Ù Ú Û Ü Ý Ÿ à á â ã ä å æ ç è é ê ë ì í î ï ð ñ ò ó ô õ ö ø œ ß þ ù ú û ü ý ÿ"));
    ISOLatin1AccentFilter filter = new ISOLatin1AccentFilter(stream);
    assertEquals("Des", filter.next().termText());
    assertEquals("mot", filter.next().termText());
    assertEquals("cles", filter.next().termText());
    assertEquals("A", filter.next().termText());
    assertEquals("LA", filter.next().termText());
    assertEquals("CHAINE", filter.next().termText());
    assertEquals("A", filter.next().termText());
    assertEquals("A", filter.next().termText());
    assertEquals("A", filter.next().termText());
    assertEquals("A", filter.next().termText());
    assertEquals("A", filter.next().termText());
    assertEquals("A", filter.next().termText());
    assertEquals("AE", filter.next().termText());
    assertEquals("C", filter.next().termText());
    assertEquals("E", filter.next().termText());
    assertEquals("E", filter.next().termText());
    assertEquals("E", filter.next().termText());
    assertEquals("E", filter.next().termText());
    assertEquals("I", filter.next().termText());
    assertEquals("I", filter.next().termText());
    assertEquals("I", filter.next().termText());
    assertEquals("I", filter.next().termText());
    assertEquals("D", filter.next().termText());
    assertEquals("N", filter.next().termText());
    assertEquals("O", filter.next().termText());
    assertEquals("O", filter.next().termText());
    assertEquals("O", filter.next().termText());
    assertEquals("O", filter.next().termText());
    assertEquals("O", filter.next().termText());
    assertEquals("O", filter.next().termText());
    assertEquals("OE", filter.next().termText());
    assertEquals("TH", filter.next().termText());
    assertEquals("U", filter.next().termText());
    assertEquals("U", filter.next().termText());
    assertEquals("U", filter.next().termText());
    assertEquals("U", filter.next().termText());
    assertEquals("Y", filter.next().termText());
    assertEquals("Y", filter.next().termText());
    assertEquals("a", filter.next().termText());
    assertEquals("a", filter.next().termText());
    assertEquals("a", filter.next().termText());
    assertEquals("a", filter.next().termText());
    assertEquals("a", filter.next().termText());
    assertEquals("a", filter.next().termText());
    assertEquals("ae", filter.next().termText());
    assertEquals("c", filter.next().termText());
    assertEquals("e", filter.next().termText());
    assertEquals("e", filter.next().termText());
    assertEquals("e", filter.next().termText());
    assertEquals("e", filter.next().termText());
    assertEquals("i", filter.next().termText());
    assertEquals("i", filter.next().termText());
    assertEquals("i", filter.next().termText());
    assertEquals("i", filter.next().termText());
    assertEquals("d", filter.next().termText());
    assertEquals("n", filter.next().termText());
    assertEquals("o", filter.next().termText());
    assertEquals("o", filter.next().termText());
    assertEquals("o", filter.next().termText());
    assertEquals("o", filter.next().termText());
    assertEquals("o", filter.next().termText());
    assertEquals("o", filter.next().termText());
    assertEquals("oe", filter.next().termText());
    assertEquals("ss", filter.next().termText());
    assertEquals("th", filter.next().termText());
    assertEquals("u", filter.next().termText());
    assertEquals("u", filter.next().termText());
    assertEquals("u", filter.next().termText());
    assertEquals("u", filter.next().termText());
    assertEquals("y", filter.next().termText());
    assertEquals("y", filter.next().termText());
    assertNull(filter.next());
  }
}
