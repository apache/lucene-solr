package org.apache.lucene.analysis.cn.smart;

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
import java.io.StringReader;
import java.util.HashMap;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;

/** 
 * Tests for {@link SmartChineseSentenceTokenizerFactory} and 
 * {@link SmartChineseWordTokenFilterFactory}
 */
public class TestSmartChineseFactories extends BaseTokenStreamTestCase {
  /** Test showing the behavior with whitespace */
  public void testSimple() throws Exception {
    Reader reader = new StringReader("我购买了道具和服装。");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    SmartChineseWordTokenFilterFactory factory = new SmartChineseWordTokenFilterFactory(new HashMap<String,String>());
    stream = factory.create(stream);
    // TODO: fix smart chinese to not emit punctuation tokens
    // at the moment: you have to clean up with WDF, or use the stoplist, etc
    assertTokenStreamContents(stream, 
       new String[] { "我", "购买", "了", "道具", "和", "服装", "," });
  }
  
  /** Test showing the behavior with whitespace */
  public void testTokenizer() throws Exception {
    Reader reader = new StringReader("我购买了道具和服装。我购买了道具和服装。");
    SmartChineseSentenceTokenizerFactory tokenizerFactory = new SmartChineseSentenceTokenizerFactory(new HashMap<String,String>());
    TokenStream stream = tokenizerFactory.create(reader);
    SmartChineseWordTokenFilterFactory factory = new SmartChineseWordTokenFilterFactory(new HashMap<String,String>());
    stream = factory.create(stream);
    // TODO: fix smart chinese to not emit punctuation tokens
    // at the moment: you have to clean up with WDF, or use the stoplist, etc
    assertTokenStreamContents(stream, 
       new String[] { "我", "购买", "了", "道具", "和", "服装", ",", 
        "我", "购买", "了", "道具", "和", "服装", ","
        });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      new SmartChineseSentenceTokenizerFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      new SmartChineseWordTokenFilterFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
