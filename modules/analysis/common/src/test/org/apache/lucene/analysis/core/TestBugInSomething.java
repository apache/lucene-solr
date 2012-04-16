package org.apache.lucene.analysis.core;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockCharFilter;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.util.CharArraySet;

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

public class TestBugInSomething extends BaseTokenStreamTestCase {
  public void test() throws Exception {
    final CharArraySet cas = new CharArraySet(TEST_VERSION_CURRENT, 3, false);
    cas.add("jjp");
    cas.add("wlmwoknt");
    cas.add("tcgyreo");
    
    final NormalizeCharMap map = new NormalizeCharMap();
    map.add("mtqlpi", "");
    map.add("mwoknt", "jjp");
    map.add("tcgyreo", "zpfpajyws");
    map.add("", "eethksv");
    
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer t = new MockTokenizer(new TestRandomChains.CheckThatYouDidntReadAnythingReaderWrapper(reader), MockTokenFilter.ENGLISH_STOPSET, false, -65);
        TokenFilter f = new CommonGramsFilter(TEST_VERSION_CURRENT, t, cas);
        return new TokenStreamComponents(t, f);
      }

      @Override
      protected Reader initReader(Reader reader) {
        reader = new MockCharFilter(reader, 0);
        reader = new MappingCharFilter(map, reader);
        return reader;
      }
    };
    checkAnalysisConsistency(random(), a, false, "wmgddzunizdomqyj");
  }
}
