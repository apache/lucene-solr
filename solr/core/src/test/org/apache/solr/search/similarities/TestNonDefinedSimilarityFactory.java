package org.apache.solr.search.similarities;

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

import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Version;
import org.junit.After;

/**
 * Verifies that the default behavior of the implicit {@link DefaultSimilarityFactory} 
 * (ie: no similarity configured in schema.xml at all) is consistnent with 
 * expectations based on the luceneMatchVersion
 * @see <a href="https://issues.apache.org/jira/browse/SOLR-5561">SOLR-5561</a>
 */
public class TestNonDefinedSimilarityFactory extends BaseSimilarityTestCase {

  @After
  public void cleanup() throws Exception {
    deleteCore();
  }

  public void testCurrent() throws Exception {
    // no sys prop set, rely on LUCENE_CURRENT
    initCore("solrconfig-basic.xml","schema-tiny.xml");
    DefaultSimilarity sim = getSimilarity("text", DefaultSimilarity.class);
    assertEquals(true, sim.getDiscountOverlaps());
  }

  public void test47() throws Exception {
    System.setProperty("tests.luceneMatchVersion", Version.LUCENE_47.toString());
    initCore("solrconfig-basic.xml","schema-tiny.xml");
    DefaultSimilarity sim = getSimilarity("text", DefaultSimilarity.class);
    assertEquals(true, sim.getDiscountOverlaps());
  }

  public void test46() throws Exception {
    System.setProperty("tests.luceneMatchVersion", Version.LUCENE_46.toString());
    initCore("solrconfig-basic.xml","schema-tiny.xml");
    DefaultSimilarity sim = getSimilarity("text", DefaultSimilarity.class);
    assertEquals(false, sim.getDiscountOverlaps());
  }

}
