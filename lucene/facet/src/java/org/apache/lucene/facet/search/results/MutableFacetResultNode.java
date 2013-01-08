package org.apache.lucene.facet.search.results;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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
 * Mutable implementation for Result of faceted search for a certain taxonomy node.
 * 
 * @lucene.experimental
 */
public class MutableFacetResultNode implements FacetResultNode {
  
  /**
   * Empty sub results to be returned when there are no results.
   * We never return null, so that code using this can remain simpler. 
   */
  private static final ArrayList<FacetResultNode> EMPTY_SUB_RESULTS = new ArrayList<FacetResultNode>();
  
  private int ordinal;
  private CategoryPath label = null;
  private double value;
  private double residue;
  private List<FacetResultNode> subResults;

  /**
   * Create a Facet Result Node.
   * 
   * @param ordinal
   *          ordinal in the taxonomy of the category of this result.
   * @param value
   *          value this result.
   */
  public MutableFacetResultNode(int ordinal, double value) {
    this(ordinal, value, 0, null, null);
  }

  /**
   * Reset a facet Result Node.
   * <p>
   * Used at the population of facet results, not intended for regular use by
   * applications.
   * 
   * @param ordinal
   *          ordinal in the taxonomy of the category of this result.
   * @param value
   *          value of this result.
   */
  public void reset(int ordinal, double value) {
    this.ordinal = ordinal;
    this.value = value;
    if (subResults != null) {
      subResults.clear();
    }
    label = null;
    residue = 0;
  }

  /**
   * Create a Facet Result Node.
   * 
   * @param ordinal
   *          ordinal in the taxonomy of the category of this result.
   * @param value
   *          value of this result.
   * @param residue
   *          Value of screened out sub results.
   * @param label
   *          label of the category path of this result.
   * @param subResults
   *          - sub results, usually descendants, sometimes child results, of
   *          this result - depending on the request.
   */
  public MutableFacetResultNode(int ordinal, double value, double residue,
      CategoryPath label, List<FacetResultNode> subResults) {
    this.ordinal = ordinal;
    this.value = value;
    this.residue = residue;
    this.label = label;
    this.subResults = subResults;
  }
  
  /**
   * Create a mutable facet result node from another result node
   * @param other other result node to copy from
   * @param takeSubResults set to true to take also sub results of other node
   */
  public MutableFacetResultNode(FacetResultNode other, boolean takeSubResults) {
    this(other.getOrdinal(), other.getValue(), other.getResidue(), other
        .getLabel(), takeSubResults ? resultsToList(other.getSubResults())
        : null);
  }
  
  private static List<FacetResultNode> resultsToList(
      Iterable<? extends FacetResultNode> subResults) {
    if (subResults == null) {
      return null;
    }
    ArrayList<FacetResultNode> res = new ArrayList<FacetResultNode>();
    for (FacetResultNode r : subResults) {
      res.add(r);
    }
    return res;
  }
  
  @Override
  public String toString() {
    return toString("");
  }
  
  /**
   * Number of sub results.
   */
  private int numSubResults() {
    if (subResults == null) {
      return 0;
    }
    return subResults.size();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.facet.search.results2.FacetResultNode#toString(java.lang.
   * String)
   */
  @Override
  public String toString(String prefix) {
    StringBuilder sb = new StringBuilder(prefix);
    
    sb.append("Facet Result Node with ").append(numSubResults()).append(
        " sub result nodes.\n");
    
    // label
    sb.append(prefix).append("Name: ").append(getLabel()).append("\n");
    
    // value
    sb.append(prefix).append("Value: ").append(value).append("\n");
    
    // residue
    sb.append(prefix).append("Residue: ").append(residue).append("\n");
    
    if (subResults != null) {
      int i = 0;
      for (FacetResultNode subRes : subResults) {
        sb.append("\n").append(prefix).append("Subresult #").append(i++)
            .append("\n").append(subRes.toString(prefix + "\t"));
      }
    }
    
    return sb.toString();
  }
  
  @Override
  public final int getOrdinal() {
    return ordinal;
  }
  
  @Override
  public final CategoryPath getLabel() {
    return label;
  }
  
  /**
   * Set the label of the category of this result.
   * @param label the label to set.
   * @see #getLabel()
   */
  public void setLabel(CategoryPath label) {
    this.label = label;
  }
  
  @Override
  public final double getValue() {
    return value;
  }

  /**
   * Set the value of this result.
   * 
   * @param value
   *          the value to set
   * @see #getValue()
   */
  @Override
  public void setValue(double value) {
    this.value = value;
  }
  
  /**
   * increase the value for this result.
   * @param addedValue the value to add
   * @see #getValue()
   */
  public void increaseValue(double addedValue) {
    this.value += addedValue;
  }
  
  @Override
  public final double getResidue() {
    return residue;
  }
  
  /**
   * Set the residue.
   * @param residue the residue to set
   * @see #getResidue()
   */
  public void setResidue(double residue) {
    this.residue = residue;
  }
  
  /**
   * increase the residue for this result.
   * @param addedResidue the residue to add
   * @see #getResidue()
   */
  public void increaseResidue(double addedResidue) {
    this.residue += addedResidue;
  }
  
  @Override
  public final Iterable<? extends FacetResultNode> getSubResults() {
    return subResults != null ? subResults : EMPTY_SUB_RESULTS;
  }

  /**
   * Trim sub results to a given size.
   * <p>
   * Note: Although the {@link #getResidue()} is not guaranteed to be
   * accurate, it is worth fixing it, as possible, by taking under account the
   * trimmed sub-nodes.
   */
  public void trimSubResults(int size) {
    if (subResults == null || subResults.size() == 0) {
      return;
    }

    ArrayList<FacetResultNode> trimmed = new ArrayList<FacetResultNode>(size);
    for (int i = 0; i < subResults.size() && i < size; i++) {
      MutableFacetResultNode trimmedNode = toImpl(subResults.get(i));
      trimmedNode.trimSubResults(size);
      trimmed.add(trimmedNode);
    }
    
    /*
     * If we are trimming, it means Sampling is in effect and the extra
     * (over-sampled) results are being trimmed. Although the residue is not
     * guaranteed to be accurate for Sampling, we try our best to fix it.
     * The node's residue now will take under account the sub-nodes we're
     * trimming.
     */
    for (int i = size; i < subResults.size(); i++) {
      increaseResidue(subResults.get(i).getValue());
    }
    
    subResults = trimmed;
  }
  
  /**
   * Set the sub results.
   * @param subResults the sub-results to set
   */
  public void setSubResults(List<FacetResultNode> subResults) {
    this.subResults = subResults;
  }
  
  /**
   * Append a sub result (as last).
   * @param subRes sub-result to be appended
   */
  public void appendSubResult(FacetResultNode subRes) {
    if (subResults == null) {
      subResults = new ArrayList<FacetResultNode>();
    }
    subResults.add(subRes);
  }
  
  /**
   * Insert sub result (as first).
   * @param subRes sub-result to be inserted
   */
  public void insertSubResult(FacetResultNode subRes) {
    if (subResults == null) {
      subResults = new ArrayList<FacetResultNode>();
    }
    subResults.add(0, subRes);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.facet.search.results.FacetResultNode#getLabel(org.apache.lucene
   * .facet.taxonomy.TaxonomyReader)
   */
  @Override
  public final CategoryPath getLabel(TaxonomyReader taxonomyReader)
      throws IOException {
    if (label == null) {
      label = taxonomyReader.getPath(ordinal);
    }
    return label;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.facet.search.results.FacetResultNode#getNumSubResults()
   */
  @Override
  public final int getNumSubResults() {
    return subResults == null ? 0 : subResults.size();
  }
  
  /**
   * Internal utility: turn a result node into an implementation class
   * with richer API that allows modifying it.
   * <p>
   * In case that input result node is already of an implementation 
   * class only casting is done, but in any case we pay the price
   * of checking "instance of".
   * @param frn facet result node to be turned into an implementation class object 
   */
  public static MutableFacetResultNode toImpl(FacetResultNode frn) {
    if (frn instanceof MutableFacetResultNode) {
      return (MutableFacetResultNode) frn;
    }
    return new MutableFacetResultNode(frn, true);
  }
  
}
