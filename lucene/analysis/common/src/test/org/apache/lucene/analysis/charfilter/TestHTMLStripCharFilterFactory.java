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
package org.apache.lucene.analysis.charfilter;


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

/**
 * Simple tests to ensure this factory is working
 */
public class TestHTMLStripCharFilterFactory extends BaseTokenStreamFactoryTestCase {


  public void testNothingChanged() throws Exception {
    //                             11111111112
    //                   012345678901234567890
    final String text = "this is only a test.";
    Reader cs = charFilterFactory("HTMLStrip", "escapedTags", "a, Title").create(new StringReader(text));
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "this", "is", "only", "a", "test." },
        new int[] { 0, 5,  8, 13, 15 },
        new int[] { 4, 7, 12, 14, 20 });
  }

  public void testNoEscapedTags() throws Exception {
    //                             11111111112222222222333333333344
    //                   012345678901234567890123456789012345678901
    final String text = "<u>this</u> is <b>only</b> a <I>test</I>.";
    Reader cs = charFilterFactory("HTMLStrip").create(new StringReader(text));
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "this", "is", "only", "a", "test." },
        new int[] {  3, 12, 18, 27, 32 },
        new int[] { 11, 14, 26, 28, 41 });
  }

  public void testEscapedTags() throws Exception {
    //                             11111111112222222222333333333344
    //                   012345678901234567890123456789012345678901
    final String text = "<u>this</u> is <b>only</b> a <I>test</I>.";
    Reader cs = charFilterFactory("HTMLStrip", "escapedTags", "U i").create(new StringReader(text));
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "<u>this</u>", "is", "only", "a", "<I>test</I>." },
        new int[] {  0, 12, 18, 27, 29 },
        new int[] { 11, 14, 26, 28, 41 });
  }

  public void testSeparatorOnlyEscapedTags() throws Exception {
    //                             11111111112222222222333333333344
    //                   012345678901234567890123456789012345678901
    final String text = "<u>this</u> is <b>only</b> a <I>test</I>.";
    Reader cs = charFilterFactory("HTMLStrip", "escapedTags", ",, , ").create(new StringReader(text));
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "this", "is", "only", "a", "test." },
        new int[] {  3, 12, 18, 27, 32 },
        new int[] { 11, 14, 26, 28, 41 });
  }

  public void testEmptyEscapedTags() throws Exception {
    //                             11111111112222222222333333333344
    //                   012345678901234567890123456789012345678901
    final String text = "<u>this</u> is <b>only</b> a <I>test</I>.";
    Reader cs = charFilterFactory("HTMLStrip", "escapedTags", "").create(new StringReader(text));
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "this", "is", "only", "a", "test." },
        new int[] {  3, 12, 18, 27, 32 },
        new int[] { 11, 14, 26, 28, 41 });
  }

  public void testSingleEscapedTag() throws Exception {
    //                             11111111112222222222333333333344
    //                   012345678901234567890123456789012345678901
    final String text = "<u>this</u> is <b>only</b> a <I>test</I>.";
    Reader cs = charFilterFactory("HTMLStrip", "escapedTags", ", B\r\n\t").create(new StringReader(text));
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "this", "is", "<b>only</b>", "a", "test." },
        new int[] {  3, 12, 15, 27, 32 },
        new int[] { 11, 14, 26, 28, 41 });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      charFilterFactory("HTMLStrip", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
