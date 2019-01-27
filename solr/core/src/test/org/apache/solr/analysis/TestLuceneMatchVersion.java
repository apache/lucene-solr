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
package org.apache.solr.analysis;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.util.Version;
import org.junit.BeforeClass;

/**
 * Tests for luceneMatchVersion property for analyzers
 */
public class TestLuceneMatchVersion extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-luceneMatchVersion.xml");
  }
  
  // this must match the solrconfig.xml version for this test
  public static final Version DEFAULT_VERSION =
    SolrConfig.parseLuceneVersionString(System.getProperty("tests.luceneMatchVersion", "LATEST"));

  public void testStandardTokenizerVersions() throws Exception {
    assertEquals(DEFAULT_VERSION, solrConfig.luceneMatchVersion);
    
    final IndexSchema schema = h.getCore().getLatestSchema();
    
    FieldType type = schema.getFieldType("textDefault");
    TokenizerChain ana = (TokenizerChain) type.getIndexAnalyzer();
    assertEquals(DEFAULT_VERSION, (ana.getTokenizerFactory()).getLuceneMatchVersion());
    assertEquals(DEFAULT_VERSION, (ana.getTokenFilterFactories()[2]).getLuceneMatchVersion());

    type = schema.getFieldType("textTurkishAnalyzerDefault");
    Analyzer ana1 = type.getIndexAnalyzer();
    assertTrue(ana1 instanceof TurkishAnalyzer);
    assertEquals(DEFAULT_VERSION, ana1.getVersion());
  }
}
