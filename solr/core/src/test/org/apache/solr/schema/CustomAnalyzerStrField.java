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
package org.apache.solr.schema;

import java.util.HashMap;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.util.LuceneTestCase;

import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.handler.admin.LukeRequestHandlerTest; // jdoc

/**
 * A Test only custom FieldType that specifies null for various params when constructing 
 * TokenizerChain instances to ensure that they are still well behaved.
 *
 * @see LukeRequestHandlerTest#testNullFactories
 */
public class CustomAnalyzerStrField extends StrField {
  private final Analyzer indexAnalyzer;
  private final Analyzer queryAnalyzer;

  public CustomAnalyzerStrField() {
    Random r = LuceneTestCase.random();

    // two arg constructor
    Analyzer a2 = new TokenizerChain
      (new KeywordTokenizerFactory(new HashMap<>()),
       r.nextBoolean() ? null : new TokenFilterFactory[0]);
    
    // three arg constructor
    Analyzer a3 = new TokenizerChain
      (r.nextBoolean() ? null : new CharFilterFactory[0],
       new KeywordTokenizerFactory(new HashMap<>()),
       r.nextBoolean() ? null : new TokenFilterFactory[0]);

    if (r.nextBoolean()) {
      indexAnalyzer = a2;
      queryAnalyzer = a3;
    } else {
      queryAnalyzer = a2;
      indexAnalyzer = a3;
    }
  }

  @Override
  public Analyzer getIndexAnalyzer() {
    return indexAnalyzer;
  }

  @Override
  public Analyzer getQueryAnalyzer() {
    return queryAnalyzer;
  }
}
