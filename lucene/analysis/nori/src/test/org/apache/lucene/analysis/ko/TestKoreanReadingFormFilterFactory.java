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
package org.apache.lucene.analysis.ko;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Simple tests for {@link KoreanReadingFormFilterFactory}
 */
public class TestKoreanReadingFormFilterFactory extends BaseTokenStreamTestCase {
  public void testReadings() throws IOException {
    KoreanTokenizerFactory tokenizerFactory = new KoreanTokenizerFactory(new HashMap<>());
    tokenizerFactory.inform(new StringMockResourceLoader(""));
    Tokenizer tokenStream = tokenizerFactory.create();
    tokenStream.setReader(new StringReader("丞相"));
    KoreanReadingFormFilterFactory filterFactory = new KoreanReadingFormFilterFactory(new HashMap<>());
    assertTokenStreamContents(filterFactory.create(tokenStream),
        new String[] { "승상" }
    );
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () ->
        new KoreanReadingFormFilterFactory(new HashMap<String, String>() {{
          put("bogusArg", "bogusValue");
        }})
    );
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
