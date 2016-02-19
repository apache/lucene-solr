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
package org.apache.lucene.analysis.cjk;


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

/**
 * Simple tests to ensure the CJK bigram factory is working.
 */
public class TestCJKBigramFilterFactory extends BaseTokenStreamFactoryTestCase {
  public void testDefaults() throws Exception {
    Reader reader = new StringReader("多くの学生が試験に落ちた。");
    TokenStream stream = tokenizerFactory("standard").create();
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("CJKBigram").create(stream);
    assertTokenStreamContents(stream,
        new String[] { "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた" });
  }
  
  public void testHanOnly() throws Exception {
    Reader reader = new StringReader("多くの学生が試験に落ちた。");
    TokenStream stream = tokenizerFactory("standard").create();
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("CJKBigram", 
        "hiragana", "false").create(stream);
    assertTokenStreamContents(stream,
        new String[] { "多", "く", "の",  "学生", "が",  "試験", "に",  "落", "ち", "た" });
  }
  
  public void testHanOnlyUnigrams() throws Exception {
    Reader reader = new StringReader("多くの学生が試験に落ちた。");
    TokenStream stream = tokenizerFactory("standard").create();
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("CJKBigram", 
        "hiragana", "false", 
        "outputUnigrams", "true").create(stream);
    assertTokenStreamContents(stream,
        new String[] { "多", "く", "の",  "学", "学生", "生", "が",  "試", "試験", "験", "に",  "落", "ち", "た" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {      
      tokenFilterFactory("CJKBigram", "bogusArg", "bogusValue");
    });
  }
}
