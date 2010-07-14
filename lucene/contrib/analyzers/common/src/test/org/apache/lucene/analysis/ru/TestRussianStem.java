package org.apache.lucene.analysis.ru;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.util.LuceneTestCase;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import static org.apache.lucene.analysis.VocabularyAssert.*;

/**
 * @deprecated Remove this test class (and its datafiles!) in Lucene 4.0
 */
@Deprecated
public class TestRussianStem extends LuceneTestCase {
  public void testStem() throws IOException {
    Analyzer a = new ReusableAnalyzerBase() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName,
          Reader reader) {
        Tokenizer t = new KeywordTokenizer(reader);
        return new TokenStreamComponents(t, new RussianStemFilter(t));
      }
    };
    InputStream voc = getClass().getResourceAsStream("wordsUTF8.txt");
    InputStream out = getClass().getResourceAsStream("stemsUTF8.txt");
    assertVocabulary(a, voc, out);
    voc.close();
    out.close();
  }
}
