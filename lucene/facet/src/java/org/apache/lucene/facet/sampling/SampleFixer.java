package org.apache.lucene.facet.sampling;

import java.io.IOException;

import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.ScoredDocIDs;

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

/**
 * Fixer of sample facet accumulation results
 * 
 * @lucene.experimental
 */
public interface SampleFixer {
  
  /**
   * Alter the input result, fixing it to account for the sampling. This
   * implementation can compute accurate or estimated counts for the sampled facets. 
   * For example, a faster correction could just multiply by a compensating factor.
   * 
   * @param origDocIds
   *          full set of matching documents.
   * @param fres
   *          sample result to be fixed.
   * @throws IOException If there is a low-level I/O error.
   */
  public void fixResult(ScoredDocIDs origDocIds, FacetResult fres) throws IOException; 
}