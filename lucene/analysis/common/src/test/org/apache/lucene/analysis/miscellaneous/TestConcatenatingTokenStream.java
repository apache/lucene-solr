/*
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

package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.AttributeFactory;

public class TestConcatenatingTokenStream extends BaseTokenStreamTestCase {

  public void testBasic() throws IOException {

    AttributeFactory factory = newAttributeFactory();

    final MockTokenizer first = new MockTokenizer(factory, MockTokenizer.WHITESPACE, false);
    first.setReader(new StringReader("first words "));
    final MockTokenizer second = new MockTokenizer(factory, MockTokenizer.WHITESPACE, false);
    second.setReader(new StringReader("second words"));
    final MockTokenizer third = new MockTokenizer(factory, MockTokenizer.WHITESPACE, false);
    third.setReader(new StringReader(" third words"));

    TokenStream ts = new ConcatenatingTokenStream(first, second, new EmptyTokenStream(), third);
    assertTokenStreamContents(ts,
        new String[] { "first", "words", "second", "words", "third", "words" },
        new int[]{ 0, 6, 12, 19, 25, 31 },
        new int[]{ 5, 11, 18, 24, 30, 36 });

    // test re-use
    first.setReader(new StringReader("first words "));
    second.setReader(new StringReader("second words"));
    third.setReader(new StringReader(" third words"));
    assertTokenStreamContents(ts,
        new String[] { "first", "words", "second", "words", "third", "words" },
        new int[]{ 0, 6, 12, 19, 25, 31 },
        new int[]{ 5, 11, 18, 24, 30, 36 },
        new int[]{ 1, 1, 1, 1, 1, 1 });

  }

  public void testOffsetGaps() throws IOException {
    CannedTokenStream cts1 = new CannedTokenStream(2, 10,
        new Token("a", 0, 1), new Token("b", 2, 3));
    CannedTokenStream cts2 = new CannedTokenStream(2, 10,
        new Token("c", 0, 1), new Token("d", 2, 3));

    TokenStream ts = new ConcatenatingTokenStream(cts1, cts2);
    assertTokenStreamContents(ts,
        new String[] { "a", "b", "c", "d" },
        new int[]{      0,   2,   10,  12 },
        new int[]{      1,   3,   11,  13 },
        null,
        new int[]{      1,   1,   3,   1 },
        null, 20, 2, null, false, null
        );
  }

  public void testInconsistentAttributes() throws IOException {

    AttributeFactory factory = newAttributeFactory();

    final MockTokenizer first = new MockTokenizer(factory, MockTokenizer.WHITESPACE, false);
    first.setReader(new StringReader("first words "));
    first.addAttribute(PayloadAttribute.class);
    final MockTokenizer second = new MockTokenizer(factory, MockTokenizer.WHITESPACE, false);
    second.setReader(new StringReader("second words"));
    second.addAttribute(FlagsAttribute.class);

    TokenStream ts = new ConcatenatingTokenStream(first, second);
    assertTrue(ts.hasAttribute(FlagsAttribute.class));
    assertTrue(ts.hasAttribute(PayloadAttribute.class));

    assertTokenStreamContents(ts,
        new String[] { "first", "words", "second", "words" },
        new int[]{ 0, 6, 12, 19, },
        new int[]{ 5, 11, 18, 24, });

  }

  public void testInconsistentAttributeFactories() throws IOException {

    final MockTokenizer first = new MockTokenizer(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, MockTokenizer.WHITESPACE, true);
    final MockTokenizer second = new MockTokenizer(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, MockTokenizer.WHITESPACE, true);

    expectThrows(IllegalArgumentException.class, () -> new ConcatenatingTokenStream(first, second));

  }

}
