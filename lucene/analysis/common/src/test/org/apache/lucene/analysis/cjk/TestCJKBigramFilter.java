package org.apache.lucene.analysis.cjk;

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

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class TestCJKBigramFilter extends BaseTokenStreamTestCase {
  Analyzer analyzer = new Analyzer() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer t = new StandardTokenizer(TEST_VERSION_CURRENT, reader);
      return new TokenStreamComponents(t, new CJKBigramFilter(t));
    }
  };
  
  public void testHuge() throws Exception {
    assertAnalyzesTo(analyzer, "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた"
     + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた"
     + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた" + "多くの学生が試験に落ちた",
       new String[] { 
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた", "た多",
        "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた"
       }    
    );
  }
  
  public void testHanOnly() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer t = new StandardTokenizer(TEST_VERSION_CURRENT, reader);
        return new TokenStreamComponents(t, new CJKBigramFilter(t, CJKBigramFilter.HAN));
      }
    };
    assertAnalyzesTo(a, "多くの学生が試験に落ちた。",
        new String[] { "多", "く", "の",  "学生", "が",  "試験", "に",  "落", "ち", "た" });
  }
}
