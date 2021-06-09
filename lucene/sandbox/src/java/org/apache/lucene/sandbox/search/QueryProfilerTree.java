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

package org.apache.lucene.sandbox.search;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import org.apache.lucene.search.Query;

/**
 * This class tracks the dependency tree for queries (scoring and rewriting) and generates {@link
 * QueryProfilerBreakdown} for each node in the tree.
 */
class QueryProfilerTree {

  private final ArrayList<QueryProfilerBreakdown> breakdowns;
  /** Maps the Query to it's list of children. This is basically the dependency tree */
  private final ArrayList<ArrayList<Integer>> tree;
  /** A list of the original queries, keyed by index position */
  private final ArrayList<Query> queries;
  /** A list of top-level "roots". Each root can have its own tree of profiles */
  private final ArrayList<Integer> roots;
  /** A temporary stack used to record where we are in the dependency tree. */
  private final Deque<Integer> stack;

  private int currentToken = 0;

  /** Rewrite time */
  private long rewriteTime;

  private long rewriteScratch;

  public QueryProfilerTree() {
    breakdowns = new ArrayList<>(10);
    stack = new ArrayDeque<>(10);
    tree = new ArrayList<>(10);
    queries = new ArrayList<>(10);
    roots = new ArrayList<>(10);
  }

  /**
   * Returns a {@link QueryProfilerBreakdown} for a scoring query. Scoring queries (e.g. those that
   * are past the rewrite phase and are now being wrapped by createWeight() ) follow a recursive
   * progression. We can track the dependency tree by a simple stack
   *
   * <p>The only hiccup is that the first scoring query will be identical to the last rewritten
   * query, so we need to take special care to fix that
   *
   * @param query The scoring query we wish to profile
   * @return A ProfileBreakdown for this query
   */
  public QueryProfilerBreakdown getProfileBreakdown(Query query) {
    int token = currentToken;

    boolean stackEmpty = stack.isEmpty();

    // If the stack is empty, we are a new root query
    if (stackEmpty) {

      // We couldn't find a rewritten query to attach to, so just add it as a
      // top-level root. This is just a precaution: it really shouldn't happen.
      // We would only get here if a top-level query that never rewrites for some reason.
      roots.add(token);

      // Increment the token since we are adding a new node, but notably, do not
      // updateParent() because this was added as a root
      currentToken += 1;
      stack.add(token);

      return addDependencyNode(query, token);
    }

    updateParent(token);

    // Increment the token since we are adding a new node
    currentToken += 1;
    stack.add(token);

    return addDependencyNode(query, token);
  }

  /**
   * Helper method to add a new node to the dependency tree.
   *
   * <p>Initializes a new list in the dependency tree, saves the query and generates a new {@link
   * QueryProfilerBreakdown} to track the timings of this query.
   *
   * @param query The query to profile
   * @param token The assigned token for this query
   * @return A {@link QueryProfilerBreakdown} to profile this query
   */
  private QueryProfilerBreakdown addDependencyNode(Query query, int token) {

    // Add a new slot in the dependency tree
    tree.add(new ArrayList<>(5));

    // Save our query for lookup later
    queries.add(query);

    QueryProfilerBreakdown breakdown = createProfileBreakdown();
    breakdowns.add(token, breakdown);
    return breakdown;
  }

  private QueryProfilerBreakdown createProfileBreakdown() {
    return new QueryProfilerBreakdown();
  }

  /** Removes the last (e.g. most recent) value on the stack */
  public void pollLast() {
    stack.pollLast();
  }

  /**
   * After the query has been run and profiled, we need to merge the flat timing map with the
   * dependency graph to build a data structure that mirrors the original query tree
   *
   * @return a hierarchical representation of the profiled query tree
   */
  public List<QueryProfilerResult> getTree() {
    ArrayList<QueryProfilerResult> results = new ArrayList<>(roots.size());
    for (Integer root : roots) {
      results.add(doGetTree(root));
    }
    return results;
  }

  /**
   * Recursive helper to finalize a node in the dependency tree
   *
   * @param token The node we are currently finalizing
   * @return A hierarchical representation of the tree inclusive of children at this level
   */
  private QueryProfilerResult doGetTree(int token) {
    Query query = queries.get(token);
    QueryProfilerBreakdown breakdown = breakdowns.get(token);
    List<Integer> children = tree.get(token);
    List<QueryProfilerResult> childrenProfileResults = Collections.emptyList();

    if (children != null) {
      childrenProfileResults = new ArrayList<>(children.size());
      for (Integer child : children) {
        QueryProfilerResult childNode = doGetTree(child);
        childrenProfileResults.add(childNode);
      }
    }

    // TODO this would be better done bottom-up instead of top-down to avoid
    // calculating the same times over and over...but worth the effort?
    String type = getTypeFromQuery(query);
    String description = getDescriptionFromQuery(query);
    return new QueryProfilerResult(
        type,
        description,
        breakdown.toBreakdownMap(),
        breakdown.toTotalTime(),
        childrenProfileResults);
  }

  private String getTypeFromQuery(Query query) {
    // Anonymous classes won't have a name,
    // we need to get the super class
    if (query.getClass().getSimpleName().isEmpty()) {
      return query.getClass().getSuperclass().getSimpleName();
    }
    return query.getClass().getSimpleName();
  }

  private String getDescriptionFromQuery(Query query) {
    return query.toString();
  }

  /**
   * Internal helper to add a child to the current parent node
   *
   * @param childToken The child to add to the current parent
   */
  private void updateParent(int childToken) {
    Integer parent = stack.peekLast();
    ArrayList<Integer> parentNode = tree.get(parent);
    parentNode.add(childToken);
    tree.set(parent, parentNode);
  }

  /** Begin timing a query for a specific Timing context */
  public void startRewriteTime() {
    assert rewriteScratch == 0;
    rewriteScratch = System.nanoTime();
  }

  /**
   * Halt the timing process and add the elapsed rewriting time. startRewriteTime() must be called
   * for a particular context prior to calling stopAndAddRewriteTime(), otherwise the elapsed time
   * will be negative and nonsensical
   *
   * @return The elapsed time
   */
  public long stopAndAddRewriteTime() {
    long time = Math.max(1, System.nanoTime() - rewriteScratch);
    rewriteTime += time;
    rewriteScratch = 0;
    return time;
  }

  public long getRewriteTime() {
    return rewriteTime;
  }
}
