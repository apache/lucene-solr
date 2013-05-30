package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.util.CollectionUtil;

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
  
  private static FacetResultNode addIfNotExist(Map<CategoryPath, FacetResultNode> nodes, FacetResultNode node) {
    FacetResultNode n = nodes.get(node.label);
    if (n == null) {
      nodes.put(node.label, node);
      n = node;
    }
    return n;
  }

  /**
   * A utility for merging multiple {@link FacetResult} of the same
   * (hierarchical) dimension into a single {@link FacetResult}, to reconstruct
   * the hierarchy. The results are merged according to the following rules:
   * <ul>
   * <li>If two results share the same dimension (first component in their
   * {@link CategoryPath}), they are merged.
   * <li>If a result is missing ancestors in the other results, e.g. A/B/C but
   * no corresponding A or A/B, these nodes are 'filled' with their label,
   * ordinal and value (obtained from the respective {@link FacetArrays}).
   * <li>If a result does not share a dimension with other results, it is
   * returned as is.
   * </ul>
   * <p>
   * <b>NOTE:</b> the returned results are not guaranteed to be in the same
   * order of the input ones.
   * 
   * @param results
   *          the results to merge
   * @param taxoReader
   *          the {@link TaxonomyReader} to use when creating missing ancestor
   *          nodes
   * @param dimArrays
   *          a mapping from a dimension to the respective {@link FacetArrays}
   *          from which to pull the nodes values
   */
  public static List<FacetResult> mergeHierarchies(List<FacetResult> results, TaxonomyReader taxoReader,
      Map<String, FacetArrays> dimArrays) throws IOException {
    final Map<String, List<FacetResult>> dims = new HashMap<String,List<FacetResult>>();
    for (FacetResult fr : results) {
      String dim = fr.getFacetRequest().categoryPath.components[0];
      List<FacetResult> frs = dims.get(dim);
      if (frs == null) {
        frs = new ArrayList<FacetResult>();
        dims.put(dim, frs);
      }
      frs.add(fr);
    }

    final List<FacetResult> res = new ArrayList<FacetResult>();
    for (List<FacetResult> frs : dims.values()) {
      FacetResult mergedResult = frs.get(0);
      if (frs.size() > 1) {
        CollectionUtil.introSort(frs, new Comparator<FacetResult>() {
          @Override
          public int compare(FacetResult fr1, FacetResult fr2) {
            return fr1.getFacetRequest().categoryPath.compareTo(fr2.getFacetRequest().categoryPath);
          }
        });
        Map<CategoryPath, FacetResultNode> mergedNodes = new HashMap<CategoryPath,FacetResultNode>();
        FacetArrays arrays = dimArrays != null ? dimArrays.get(frs.get(0).getFacetRequest().categoryPath.components[0]) : null;
        for (FacetResult fr : frs) {
          FacetResultNode frn = fr.getFacetResultNode();
          FacetResultNode merged = mergedNodes.get(frn.label);
          if (merged == null) {
            CategoryPath parent = frn.label.subpath(frn.label.length - 1);
            FacetResultNode childNode = frn;
            FacetResultNode parentNode = null;
            while (parent.length > 0 && (parentNode = mergedNodes.get(parent)) == null) {
              int parentOrd = taxoReader.getOrdinal(parent);
              double parentValue = arrays != null ? fr.getFacetRequest().getValueOf(arrays, parentOrd) : -1;
              parentNode = new FacetResultNode(parentOrd, parentValue);
              parentNode.label = parent;
              parentNode.subResults = new ArrayList<FacetResultNode>();
              parentNode.subResults.add(childNode);
              mergedNodes.put(parent, parentNode);
              childNode = parentNode;
              parent = parent.subpath(parent.length - 1);
            }

            // at least one parent was added, so link the final (existing)
            // parent with the child
            if (parent.length > 0) {
              if (!(parentNode.subResults instanceof ArrayList)) {
                parentNode.subResults = new ArrayList<FacetResultNode>(parentNode.subResults);
              }
              parentNode.subResults.add(childNode);
            }

            // for missing FRNs, add new ones with label and value=-1
            // first time encountered this label, add it and all its children to
            // the map.
            mergedNodes.put(frn.label, frn);
            for (FacetResultNode child : frn.subResults) {
              addIfNotExist(mergedNodes, child);
            }
          } else {
            if (!(merged.subResults instanceof ArrayList)) {
              merged.subResults = new ArrayList<FacetResultNode>(merged.subResults);
            }
            for (FacetResultNode sub : frn.subResults) {
              // make sure sub wasn't already added
              sub = addIfNotExist(mergedNodes, sub);
              if (!merged.subResults.contains(sub)) {
                merged.subResults.add(sub);
              }
            }
          }
        }
        
        // find the 'first' node to put on the FacetResult root
        CategoryPath min = null;
        for (CategoryPath cp : mergedNodes.keySet()) {
          if (min == null || cp.compareTo(min) < 0) {
            min = cp;
          }
        }
        FacetRequest dummy = new FacetRequest(min, frs.get(0).getFacetRequest().numResults) {
          @Override
          public double getValueOf(FacetArrays arrays, int idx) {
            throw new UnsupportedOperationException("not supported by this request");
          }
          
          @Override
          public FacetArraysSource getFacetArraysSource() {
            throw new UnsupportedOperationException("not supported by this request");
          }
        };
        mergedResult = new FacetResult(dummy, mergedNodes.get(min), -1);
      }
      res.add(mergedResult);
    }
    return res;
  }

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
