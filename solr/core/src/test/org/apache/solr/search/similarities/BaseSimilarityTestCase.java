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

import java.io.IOException;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.SolrTestCaseJ4;

public abstract class BaseSimilarityTestCase extends SolrTestCaseJ4 {

  /** returns the similarity in use for the field */
  protected Similarity getSimilarity(String field) {
    Similarity sim = null;
    try {
      sim = h.getCore().withSearcher(IndexSearcher::getSimilarity);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    while (sim instanceof PerFieldSimilarityWrapper) {
      sim = ((PerFieldSimilarityWrapper)sim).get(field);
    }
    return sim;
  }

  /** 
   * Returns the similarity in use for the field, 
   * after asserting that it implements the specified class 
   */
  protected <T extends Similarity> T getSimilarity(String field, 
                                                   Class<T> clazz) {
    Similarity sim = getSimilarity(field);
    assertTrue("Similarity for Field " + field + 
               " does not match expected class: " + clazz.getName(), 
               clazz.isInstance(sim));
    return clazz.cast(sim);
  }
}
