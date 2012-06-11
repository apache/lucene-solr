package org.apache.lucene.facet.example;

import java.util.List;

import org.apache.lucene.facet.search.results.FacetResult;

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
 * Result of running an example program.
 * This is a general object for allowing to write a test 
 * that runs an example and verifies its results.
 * 
 * @lucene.experimental
 */
public class ExampleResult {

  private List<FacetResult> facetResults;

  /**
   * @return the facet results
   */
  public List<FacetResult> getFacetResults() {
    return facetResults;
  }

  /**
   * @param facetResults the facet results to set
   */
  public void setFacetResults(List<FacetResult> facetResults) {
    this.facetResults = facetResults;
  }

}
