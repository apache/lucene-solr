package org.apache.lucene.facet.search;

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
 * Result of faceted search.
 * 
 * @lucene.experimental
 */
public class FacetResult {
  
  private final FacetRequest facetRequest;
  private final FacetResultNode rootNode;
  private final int numValidDescendants;
  
  public FacetResult(FacetRequest facetRequest, FacetResultNode rootNode,  int numValidDescendants) {
    this.facetRequest = facetRequest;
    this.rootNode = rootNode;
    this.numValidDescendants = numValidDescendants;
  }
  
  /**
   * Facet result node matching the root of the {@link #getFacetRequest() facet request}.
   * @see #getFacetRequest()
   * @see FacetRequest#categoryPath
   */
  public final FacetResultNode getFacetResultNode() {
    return rootNode;
  }
  
  /**
   * Number of descendants of {@link #getFacetResultNode() root facet result
   * node}, up till the requested depth.
   */
  public final int getNumValidDescendants() {
    return numValidDescendants;
  }
  
  /**
   * Request for which this result was obtained.
   */
  public final FacetRequest getFacetRequest() {
    return this.facetRequest;
  }

  /**
   * String representation of this facet result.
   * Use with caution: might return a very long string.
   * @param prefix prefix for each result line
   * @see #toString()
   */
  public String toString(String prefix) {
    StringBuilder sb = new StringBuilder();
    String nl = "";
    
    // request
    if (this.facetRequest != null) {
      sb.append(nl).append(prefix).append("Request: ").append(
          this.facetRequest.toString());
      nl = "\n";
    }
    
    // total facets
    sb.append(nl).append(prefix).append("Num valid Descendants (up to specified depth): ").append(
        this.numValidDescendants);
    nl = "\n";
    
    // result node
    if (this.rootNode != null) {
      sb.append(nl).append(this.rootNode.toString(prefix + "\t"));
    }
    
    return sb.toString();
  }
  
  @Override
  public String toString() {
    return toString("");
  }
  
}
