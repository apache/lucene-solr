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
package org.apache.lucene.analysis.cn.smart;


import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.TokenizerFactory;

/** 
 * Tests for {@link HMMChineseTokenizerFactory}
 */
public class TestHMMChineseTokenizerFactory extends BaseTokenStreamTestCase {
  
  /** Test showing the behavior */
  public void testSimple() throws Exception {
    Reader reader = new StringReader("我购买了道具和服装。");
    TokenizerFactory factory = new HMMChineseTokenizerFactory(new HashMap<String,String>());
    Tokenizer tokenizer = factory.create(newAttributeFactory());
    tokenizer.setReader(reader);
    // TODO: fix smart chinese to not emit punctuation tokens
    // at the moment: you have to clean up with WDF, or use the stoplist, etc
    assertTokenStreamContents(tokenizer, 
       new String[] { "我", "购买", "了", "道具", "和", "服装", "," });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      new HMMChineseTokenizerFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
