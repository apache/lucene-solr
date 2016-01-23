package org.apache.solr.rest.schema;
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

import org.apache.solr.rest.SolrRestletTestBase;
import org.junit.Test;

public class TestSchemaSimilarityResource extends SolrRestletTestBase {

  /**
   * NOTE: schema used by parent class doesn't define a global sim, so we get the implicit default
   * which causes the FQN of the class to be returned
   * 
   * @see TestClassNameShortening#testShortenedGlobalSimilarityStaysShortened
   */
  @Test
  public void testGetSchemaSimilarity() throws Exception {
    assertQ("/schema/similarity?indent=on&wt=xml",
            "count(/response/lst[@name='similarity']) = 1",
            "/response/lst[@name='similarity']/str[@name='class'][.='org.apache.solr.search.similarities.SchemaSimilarityFactory']");
  }
}

