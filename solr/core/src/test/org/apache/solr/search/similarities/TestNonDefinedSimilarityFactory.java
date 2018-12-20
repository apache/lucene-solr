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
package org.apache.solr.search.similarities;

import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarity.LegacyBM25Similarity;
import org.apache.lucene.util.Version;
import org.junit.After;

/**
 * Verifies that the default behavior of the implicit {@link BM25Similarity} 
 * (ie: no similarity configured in schema.xml at all) is consistent with 
 * expectations based on the luceneMatchVersion
 * @see <a href="https://issues.apache.org/jira/browse/SOLR-5561">SOLR-5561</a>
 * @see <a href="https://issues.apache.org/jira/browse/SOLR-8057">SOLR-8057</a>
 * @see <a href="https://issues.apache.org/jira/browse/SOLR-13025">SOLR-13025</a>
 * @see <a href="https://issues.apache.org/jira/browse/LUCENE-8563">LUCENE-8563</a>
 */
public class TestNonDefinedSimilarityFactory extends BaseSimilarityTestCase {

  @After
  public void cleanup() throws Exception {
    deleteCore();
  }

  public void testCurrentBM25FromV8() throws Exception {
    // no sys prop set, rely on LATEST
    initCore("solrconfig-basic.xml","schema-tiny.xml");
    BM25Similarity sim = getSimilarity("text", BM25Similarity.class);
    assertEquals(0.75F, sim.getB(), 0.0F);
  }

  public void testLegacyBM25BeforeV8() throws Exception {
    System.setProperty("tests.luceneMatchVersion", Version.LUCENE_7_0_0.toString());
    initCore("solrconfig-basic.xml","schema-tiny.xml");
    System.clearProperty("tests.luceneMatchVersion");
    LegacyBM25Similarity sim = getSimilarity("text", LegacyBM25Similarity.class);
    assertEquals(0.75F, sim.getB(), 0.0F);
    deleteCore();

    System.setProperty("tests.luceneMatchVersion", "5.0.0");
    initCore("solrconfig-basic.xml","schema-tiny.xml");
    System.clearProperty("tests.luceneMatchVersion");
    getSimilarity("text", LegacyBM25Similarity.class);
    deleteCore();

    System.setProperty("tests.luceneMatchVersion", "6.0.0");
    initCore("solrconfig-basic.xml","schema-tiny.xml");
    System.clearProperty("tests.luceneMatchVersion");
    getSimilarity("text", LegacyBM25Similarity.class);
  }
}
