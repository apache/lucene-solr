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

package org.apache.lucene.luwak.termextractor.querytree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.lucene.luwak.termextractor.QueryTerm;

public class DisjunctionNode extends QueryTree {

  private final List<QueryTree> children;

  private DisjunctionNode(List<QueryTree> children) {
    this.children = new ArrayList<>(children);
    this.children.sort(Comparator.comparingDouble(QueryTree::weight));
  }

  public static QueryTree build(List<QueryTree> children) {
    if (children.size() == 0)
      throw new IllegalArgumentException("Cannot build DisjunctionNode with no children");
    if (children.size() == 1)
      return children.get(0);
    Optional<QueryTree> firstAnyChild = children.stream().filter(QueryTree::isAny).findAny();
    // if any of the children is an ANY node, just return that, otherwise build the disjunction
    return firstAnyChild.orElseGet(() -> new DisjunctionNode(children));
  }

  public static QueryTree build(QueryTree... children) {
    return build(Arrays.asList(children));
  }

  @Override
  public double weight() {
    return children.get(0).weight();
  }

  @Override
  public void collectTerms(Set<QueryTerm> termsList) {
    if (isAny()) {
      termsList.add(new QueryTerm("", "DISJUNCTION WITH ANYTOKEN", QueryTerm.Type.ANY));
      return;
    }
    for (QueryTree child : children) {
      child.collectTerms(termsList);
    }
  }

  @Override
  public boolean isAny() {
    for (QueryTree child : children) {
      if (child.isAny())
        return true;
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Disjunction[");
    sb.append(children.size()).append("]^");
    sb.append(weight()).append(" { ");
    for (QueryTree child : children) {
      sb.append(child.toString()).append(" ");
    }
    return sb.append("}").toString();
  }

  @Override
  public boolean advancePhase(float minWeight) {
    boolean changed = false;
    for (QueryTree child : children) {
      changed |= child.advancePhase(minWeight);
    }
    if (changed == false) {
      return false;
    }
    children.sort(Comparator.comparingDouble(QueryTree::weight));
    return changed;
  }

  @Override
  public void visit(QueryTreeVisitor visitor, int depth) {
    visitor.visit(this, depth);
    for (QueryTree child : children) {
      child.visit(visitor, depth + 1);
    }
  }

}
