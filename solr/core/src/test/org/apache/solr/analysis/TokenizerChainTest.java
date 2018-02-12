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

import org.apache.lucene.analysis.Analyzer;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.TextField;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This is a simple test to make sure that the TokenizerChain
 * is correctly normalizing queries.
 *
 */

public class TokenizerChainTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml", "schema.xml");
  }

  @Test
  public void testNormalization() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    Analyzer lowerAscii = ((TextField)schema.getFieldOrNull("lowerascii").getType()).getQueryAnalyzer();
    String normed = lowerAscii.normalize("lowerascii", "FOOBA").utf8ToString();
    assertEquals("fooba", normed);

    lowerAscii = ((TextField)schema.getFieldOrNull("lowerascii").getType()).getMultiTermAnalyzer();
    normed = lowerAscii.normalize("lowerascii", "FOOBA").utf8ToString();
    assertEquals("fooba", normed);

  }
}
