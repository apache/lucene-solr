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

package org.apache.lucene.monitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

/**
 * A representation of a node in a query tree
 *
 * <p>Queries are analyzed and converted into an abstract tree, consisting of conjunction and
 * disjunction nodes, and leaf nodes containing terms.
 *
 * <p>Terms may be collected from a node, which will use the weights of its sub-nodes to determine
 * which paths are followed. The path may be changed by calling {@link #advancePhase(double)}
 */
public abstract class QueryTree {

  /** The weight of this node */
  public abstract double weight();

  /** Collect terms from the most highly-weighted path below this node */
  public abstract void collectTerms(BiConsumer<String, BytesRef> termCollector);

  /**
   * Find the next-most highly-weighted path below this node
   *
   * @param minWeight do not advance if the next path has a weight below this value
   * @return {@code false} if there are no more paths above the minimum weight
   */
  public abstract boolean advancePhase(double minWeight);

  /**
   * Returns a string representation of the node
   *
   * @param depth the current depth of this node in the overall query tree
   */
  public abstract String toString(int depth);

  @Override
  public String toString() {
    return toString(0);
  }

  /** Returns a string of {@code width} spaces */
  protected String space(int width) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < width; i++) {
      sb.append(" ");
    }
    return sb.toString();
  }

  /** Returns a leaf node for a particular term */
  public static QueryTree term(Term term, TermWeightor weightor) {
    return term(term.field(), term.bytes(), weightor.applyAsDouble(term));
  }

  /**
   * Returns a leaf node for a particular term and weight
   *
   * <p>The weight must be greater than 0
   */
  public static QueryTree term(Term term, double weight) {
    return term(term.field(), term.bytes(), weight);
  }

  /**
   * Returns a leaf node for a particular term and weight
   *
   * <p>The weight must be greater than 0
   */
  public static QueryTree term(String field, BytesRef term, double weight) {
    return new QueryTree() {
      @Override
      public double weight() {
        if (weight <= 0) {
          throw new IllegalArgumentException("Term weights must be greater than 0");
        }
        return weight;
      }

      @Override
      public void collectTerms(BiConsumer<String, BytesRef> termCollector) {
        termCollector.accept(field, term);
      }

      @Override
      public boolean advancePhase(double minWeight) {
        return false;
      }

      @Override
      public String toString(int depth) {
        return space(depth) + field + ":" + term.utf8ToString() + "^" + weight;
      }
    };
  }

  /** Returns a leaf node that will match any document */
  public static QueryTree anyTerm(String reason) {
    return new QueryTree() {
      @Override
      public double weight() {
        return 0;
      }

      @Override
      public void collectTerms(BiConsumer<String, BytesRef> termCollector) {
        termCollector.accept(
            TermFilteredPresearcher.ANYTOKEN_FIELD, new BytesRef(TermFilteredPresearcher.ANYTOKEN));
      }

      @Override
      public boolean advancePhase(double minWeight) {
        return false;
      }

      @Override
      public String toString(int depth) {
        return space(depth) + "ANY[" + reason + "]";
      }
    };
  }

  /** Returns a conjunction of a set of child nodes */
  public static QueryTree conjunction(
      List<Function<TermWeightor, QueryTree>> children, TermWeightor weightor) {
    if (children.size() == 0) {
      throw new IllegalArgumentException("Cannot build a conjunction with no children");
    }
    if (children.size() == 1) {
      return children.get(0).apply(weightor);
    }
    List<QueryTree> qt = children.stream().map(f -> f.apply(weightor)).collect(Collectors.toList());
    List<QueryTree> restricted =
        qt.stream().filter(t -> t.weight() > 0).collect(Collectors.toList());
    if (restricted.size() == 0) {
      // all children are ANY, so just return the first one
      return qt.get(0);
    }
    return new ConjunctionQueryTree(qt);
  }

  static QueryTree conjunction(QueryTree... children) {
    return new ConjunctionQueryTree(Arrays.asList(children));
  }

  /** Returns a disjunction of a set of child nodes */
  public static QueryTree disjunction(
      List<Function<TermWeightor, QueryTree>> children, TermWeightor weightor) {
    if (children.size() == 0) {
      throw new IllegalArgumentException("Cannot build a disjunction with no children");
    }
    if (children.size() == 1) {
      return children.get(0).apply(weightor);
    }
    List<QueryTree> qt = children.stream().map(f -> f.apply(weightor)).collect(Collectors.toList());
    Optional<QueryTree> firstAnyChild = qt.stream().filter(q -> q.weight() == 0).findAny();
    // if any of the children is an ANY node, just return that, otherwise build the disjunction
    return firstAnyChild.orElseGet(() -> new DisjunctionQueryTree(qt));
  }

  static QueryTree disjunction(QueryTree... children) {
    return new DisjunctionQueryTree(Arrays.asList(children));
  }

  private static class ConjunctionQueryTree extends QueryTree {

    private static final Comparator<QueryTree> COMPARATOR =
        Comparator.comparingDouble(QueryTree::weight).reversed();

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
    public void collectTerms(BiConsumer<String, BytesRef> termCollector) {
      children.get(0).collectTerms(termCollector);
    }

    @Override
    public boolean advancePhase(double minWeight) {
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
    public String toString(int depth) {
      StringBuilder sb =
          new StringBuilder(space(depth))
              .append("Conjunction[")
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
    public void collectTerms(BiConsumer<String, BytesRef> termCollector) {
      for (QueryTree child : children) {
        child.collectTerms(termCollector);
      }
    }

    @Override
    public boolean advancePhase(double minWeight) {
      boolean changed = false;
      for (QueryTree child : children) {
        changed |= child.advancePhase(minWeight);
      }
      if (changed == false) {
        return false;
      }
      children.sort(Comparator.comparingDouble(QueryTree::weight));
      return true;
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
