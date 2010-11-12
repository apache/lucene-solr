package org.apache.solr.analysis;

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

import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

/** basic tests for {@link ICUTransformFilterFactory} */
public class TestICUTransformFilterFactory extends BaseTokenTestCase {
  
  /** ensure the transform is working */
  public void test() throws Exception {
    Reader reader = new StringReader("簡化字");
    ICUTransformFilterFactory factory = new ICUTransformFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("id", "Traditional-Simplified");
    factory.init(args);
    Tokenizer tokenizer = new WhitespaceTokenizer(DEFAULT_VERSION, reader);
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "简化字" });
  }
  
  /** test forward and reverse direction */
  public void testDirection() throws Exception {
    // forward
    Reader reader = new StringReader("Российская Федерация");
    ICUTransformFilterFactory factory = new ICUTransformFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("id", "Cyrillic-Latin");
    factory.init(args);
    Tokenizer tokenizer = new WhitespaceTokenizer(DEFAULT_VERSION, reader);
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "Rossijskaâ",  "Federaciâ" });
    
    // backward (invokes Latin-Cyrillic)
    reader = new StringReader("Rossijskaâ Federaciâ");
    args.put("direction", "reverse");
    factory.init(args);
    tokenizer = new WhitespaceTokenizer(DEFAULT_VERSION, reader);
    stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "Российская", "Федерация" });
  }
}
