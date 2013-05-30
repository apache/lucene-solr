package org.apache.lucene.facet.search;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.facet.search.FacetRequest.ResultMode;
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
 * Result of faceted search for a certain taxonomy node. This class serves as a
 * bin of different attributes of the result node, such as its {@link #ordinal}
 * as well as {@link #label}. You are not expected to modify those values.
 * 
 * @lucene.experimental
 */
public class FacetResultNode {

  public static final List<FacetResultNode> EMPTY_SUB_RESULTS = Collections.emptyList();
  
  /** The category ordinal of this node. */
  public int ordinal;

  /**
   * The {@link CategoryPath label} of this result. May be {@code null} if not
   * computed, in which case use {@link TaxonomyReader#getPath(int)} to label
   * it.
   * <p>
   * <b>NOTE:</b> by default, all nodes are labeled. Only when
   * {@link FacetRequest#getNumLabel()} &lt;
   * {@link FacetRequest#numResults} there will be unlabeled nodes.
   */
  public CategoryPath label;
  
  /**
   * The value of this result. Its actual type depends on the
   * {@link FacetRequest} used (e.g. in case of {@link CountFacetRequest} it is
   * {@code int}).
   */
  public double value;

  /**
   * The sub-results of this result. If {@link FacetRequest#getResultMode()} is
   * {@link ResultMode#PER_NODE_IN_TREE}, every sub result denotes an immediate
   * child of this node. Otherwise, it is a descendant of any level.
   * <p>
   * <b>NOTE:</b> this member should not be {@code null}. To denote that a
   * result does not have sub results, set it to {@link #EMPTY_SUB_RESULTS} (or
   * don't modify it).
   */
  public List<FacetResultNode> subResults = EMPTY_SUB_RESULTS;

  public FacetResultNode(int ordinal, double value) {
    this.ordinal = ordinal;
    this.value = value;
  }
  
  @Override
  public String toString() {
    return toString("");
  }
  
  /** Returns a String representation of this facet result node. */
  public String toString(String prefix) {
    StringBuilder sb = new StringBuilder(prefix);
    if (label == null) {
      sb.append("not labeled (ordinal=").append(ordinal).append(")");
    } else {
      sb.append(label.toString());
    }
    sb.append(" (").append(Double.toString(value)).append(")");
    for (FacetResultNode sub : subResults) {
      sb.append("\n").append(prefix).append(sub.toString(prefix + "  "));
    }
    return sb.toString();
  }

}
