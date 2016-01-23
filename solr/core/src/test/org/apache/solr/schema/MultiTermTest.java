package org.apache.solr.schema;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.charfilter.MappingCharFilterFactory;
import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;
import org.apache.lucene.analysis.miscellaneous.TrimFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analysis.TokenizerChain;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiTermTest extends SolrTestCaseJ4 {
  public String getCoreName() {
    return "basic";
  }

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml", "schema-folding.xml");
  }

  @Test
  public void testMultiFound() {
    SchemaField field = h.getCore().getLatestSchema().getField("content_multi");
    Analyzer analyzer = ((TextField)field.getType()).getMultiTermAnalyzer();
    assertTrue(analyzer instanceof TokenizerChain);
    assertTrue(((TokenizerChain) analyzer).getTokenizerFactory() instanceof WhitespaceTokenizerFactory);
    TokenizerChain tc = (TokenizerChain) analyzer;
    for (TokenFilterFactory factory : tc.getTokenFilterFactories()) {
      assertTrue((factory instanceof ASCIIFoldingFilterFactory) || (factory instanceof LowerCaseFilterFactory));
    }

    analyzer = field.getType().getIndexAnalyzer();
    assertTrue(analyzer instanceof TokenizerChain);
    assertTrue(((TokenizerChain) analyzer).getTokenizerFactory() instanceof WhitespaceTokenizerFactory);
    tc = (TokenizerChain) analyzer;
    for (TokenFilterFactory factory : tc.getTokenFilterFactories()) {
      assertTrue((factory instanceof ASCIIFoldingFilterFactory) || (factory instanceof TrimFilterFactory));
    }

    assertTrue(tc.getCharFilterFactories().length == 0);
  }

  @Test
  public void testQueryCopiedToMulti() {
    SchemaField field = h.getCore().getLatestSchema().getField("content_charfilter");
    Analyzer analyzer = ((TextField)field.getType()).getMultiTermAnalyzer();
    assertTrue(analyzer instanceof TokenizerChain);
    assertTrue(((TokenizerChain) analyzer).getTokenizerFactory() instanceof KeywordTokenizerFactory);
    TokenizerChain tc = (TokenizerChain) analyzer;
    for (TokenFilterFactory factory : tc.getTokenFilterFactories()) {
      assertTrue(factory instanceof LowerCaseFilterFactory);
    }

    assertTrue(tc.getCharFilterFactories().length == 1);
    assertTrue(tc.getCharFilterFactories()[0] instanceof MappingCharFilterFactory);
  }

  @Test
  public void testDefaultCopiedToMulti() {
    SchemaField field = h.getCore().getLatestSchema().getField("content_ws");
    Analyzer analyzer = ((TextField)field.getType()).getMultiTermAnalyzer();
    assertTrue(analyzer instanceof TokenizerChain);
    assertTrue(((TokenizerChain) analyzer).getTokenizerFactory() instanceof KeywordTokenizerFactory);
    TokenizerChain tc = (TokenizerChain) analyzer;
    for (TokenFilterFactory factory : tc.getTokenFilterFactories()) {
      assertTrue((factory instanceof ASCIIFoldingFilterFactory) || (factory instanceof LowerCaseFilterFactory));
    }

    assertTrue(tc.getCharFilterFactories().length == 0);

  }
}
