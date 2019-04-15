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

package org.apache.lucene.luwak.termextractor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.termextractor.weights.TermWeightor;

public abstract class QueryTree {

  public abstract double weight();

  public abstract void collectTerms(Set<QueryTerm> termsList);

  public abstract boolean advancePhase(float minWeight);

  public abstract boolean isAny();

  public abstract String toString(int depth);

  @Override
  public String toString() {
    return toString(0);
  }

  String space(int width) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < width; i++) {
      sb.append(" ");
    }
    return sb.toString();
  }

  public static QueryTree term(Term term, TermWeightor weightor) {
    QueryTerm queryTerm = new QueryTerm(term);
    return term(queryTerm, weightor);
  }

  public static QueryTree term(QueryTerm queryTerm, TermWeightor weightor) {
    return term(queryTerm, weightor.weigh(queryTerm));
  }

  public static QueryTree term(QueryTerm queryTerm, double weight) {
    return new QueryTree() {
      @Override
      public double weight() {
        return weight;
      }

      @Override
      public void collectTerms(Set<QueryTerm> termsList) {
        termsList.add(queryTerm);
      }

      @Override
      public boolean advancePhase(float minWeight) {
        return false;
      }

      @Override
      public boolean isAny() {
        return false;
      }

      @Override
      public String toString(int depth) {
        return space(depth) + queryTerm + "^" + weight;
      }
    };
  }

  public static QueryTree anyTerm(String reason) {
    return new QueryTree() {
      @Override
      public double weight() {
        return 0;
      }

      @Override
      public void collectTerms(Set<QueryTerm> termsList) {
        termsList.add(new QueryTerm("__any__", reason, QueryTerm.Type.ANY));
      }

      @Override
      public boolean advancePhase(float minWeight) {
        return false;
      }

      @Override
      public boolean isAny() {
        return true;
      }

      @Override
      public String toString(int depth) {
        return space(depth) + "ANY[" + reason + "]";
      }
    };
  }

  public static QueryTree conjunction(List<Function<TermWeightor, QueryTree>> children, TermWeightor weightor) {
    if (children.size() == 0) {
      throw new IllegalArgumentException("Cannot build a conjunction with no children");
    }
    if (children.size() == 1) {
      return children.get(0).apply(weightor);
    }
    List<QueryTree> qt = children.stream()
        .map(f -> f.apply(weightor)).collect(Collectors.toList());
    List<QueryTree> restricted = qt.stream().filter(t -> t.isAny() == false).collect(Collectors.toList());
    if (restricted.size() == 0) {
      // all children are ANY, so just return the first one
      return qt.get(0);
    }
    return new ConjunctionQueryTree(qt);
  }

  static QueryTree conjunction(QueryTree... children) {
    return new ConjunctionQueryTree(Arrays.asList(children));
  }

  public static QueryTree disjunction(List<Function<TermWeightor, QueryTree>> children, TermWeightor weightor) {
    if (children.size() == 0) {
      throw new IllegalArgumentException("Cannot build a disjunction with no children");
    }
    if (children.size() == 1) {
      return children.get(0).apply(weightor);
    }
    List<QueryTree> qt = children.stream()
        .map(f -> f.apply(weightor)).collect(Collectors.toList());
    Optional<QueryTree> firstAnyChild = qt.stream().filter(QueryTree::isAny).findAny();
    // if any of the children is an ANY node, just return that, otherwise build the disjunction
    return firstAnyChild.orElseGet(() -> new DisjunctionQueryTree(qt));
  }

  static QueryTree disjunction(QueryTree... children) {
    return new DisjunctionQueryTree(Arrays.asList(children));
  }

  private static class ConjunctionQueryTree extends QueryTree {

    private static final Comparator<QueryTree> COMPARATOR = Comparator.comparingDouble(QueryTree::weight).reversed();

    final List<QueryTree> children = new ArrayList<>();

    ConjunctionQueryTree(List<QueryTree> children) {
      this.children.addAll(children);
      this.children.sort(COMPARATOR);
    }

    @Override
    public double weight() {
      return children.get(0).weight();
    }

    @Override
    public void collectTerms(Set<QueryTerm> termsList) {
      children.get(0).collectTerms(termsList);
    }

    @Override
    public boolean advancePhase(float minWeight) {
      if (children.get(0).advancePhase(minWeight)) {
        this.children.sort(COMPARATOR);
        return true;
      }
      if (children.size() == 1) {
        return false;
      }
      if (children.get(1).weight() <= minWeight) {
        return false;
      }
      children.remove(0);
      return true;
    }

    @Override
    public boolean isAny() {
      for (QueryTree child : children) {
        if (!child.isAny())
          return false;
      }
      return true;
    }

    @Override
    public String toString(int depth) {
      StringBuilder sb = new StringBuilder(space(depth)).append("Conjunction[")
          .append(children.size())
          .append("]^")
          .append(weight())
          .append("\n");
      for (QueryTree child : children) {
        sb.append(child.toString(depth + 2)).append("\n");
      }
      return sb.toString();
    }
  }

  private static class DisjunctionQueryTree extends QueryTree {

    final List<QueryTree> children = new ArrayList<>();

    private DisjunctionQueryTree(List<QueryTree> children) {
      this.children.addAll(children);
      this.children.sort(Comparator.comparingDouble(QueryTree::weight));
    }

    @Override
    public double weight() {
      return children.get(0).weight();
    }

    @Override
    public void collectTerms(Set<QueryTerm> termsList) {
      for (QueryTree child : children) {
        child.collectTerms(termsList);
      }
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
    public boolean isAny() {
      // if a child is an ANY token, then the disjunction doesn't get built in the first place
      return false;
    }

    @Override
    public String toString(int depth) {
      StringBuilder sb = new StringBuilder(space(depth)).append("Disjunction[");
      sb.append(children.size()).append("]^");
      sb.append(weight()).append("\n");
      for (QueryTree child : children) {
        sb.append(child.toString(depth + 2)).append("\n");
      }
      return sb.toString();
    }
  }
}
