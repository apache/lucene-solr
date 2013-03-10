package org.apache.lucene.analysis.miscellaneous;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.snowball.SnowballFilter;

import java.io.IOException;
import java.io.StringReader;

public class TestKeywordRepeatFilter extends BaseTokenStreamTestCase {

  public void testBasic() throws IOException {
    TokenStream ts = new RemoveDuplicatesTokenFilter(new SnowballFilter(new KeywordRepeatFilter(
        new MockTokenizer(new StringReader("the birds are flying"), MockTokenizer.WHITESPACE, false)), "English"));
    assertTokenStreamContents(ts, new String[] { "the", "birds", "bird", "are", "flying", "fli"}, new int[] {1,1,0,1,1,0});
  }


  public void testComposition() throws IOException {
    TokenStream ts = new RemoveDuplicatesTokenFilter(new SnowballFilter(new KeywordRepeatFilter(new KeywordRepeatFilter(
        new MockTokenizer(new StringReader("the birds are flying"), MockTokenizer.WHITESPACE, false))), "English"));
    assertTokenStreamContents(ts, new String[] { "the", "birds", "bird", "are", "flying", "fli"}, new int[] {1,1,0,1,1,0});
  }



}
