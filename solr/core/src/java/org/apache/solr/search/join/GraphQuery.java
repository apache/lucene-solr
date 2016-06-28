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
package org.apache.solr.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.DaciukMihovAutomatonBuilder;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.Filter;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * GraphQuery - search for nodes and traverse edges in an index.
 * 
 * Params:
 * fromField = the field that contains the node id
 * toField = the field that contains the edge ids
 * traversalFilter = a query that can be applied for each hop in the graph.
 * maxDepth = the max depth to traverse.  (start nodes is depth=1)
 * onlyLeafNodes = only return documents that have no edge id values.
 * returnRoot = if false, the documents matching the initial query will not be returned.
 *
 * @lucene.experimental
 */
public class GraphQuery extends Query {
  
  /** The inital node matching query */
  private Query q;
  /** the field with the node id */
  private String fromField;
  /** the field containing the edge ids */
  private String toField;
  /** A query to apply while traversing the graph to filter out edges */
  private Query traversalFilter;
  /** The max depth to traverse the graph, -1 means no limit. */
  private int maxDepth = -1;

  /** Use automaton compilation for graph query traversal (experimental + expert use only) */
  private boolean useAutn = true;
  
  /** If this is true, the graph traversal result will only return documents that 
   * do not have a value in the edge field. (Only leaf nodes returned from the graph) */
  private boolean onlyLeafNodes = false;
  
  /** False if documents matching the start query for the graph will be excluded from the final result set.  */
  private boolean returnRoot = true;
  
  /**
   * Create a graph query 
   * q - the starting node query
   * fromField - the field containing the node id
   * toField - the field containing the edge ids
   */
  public GraphQuery(Query q, String fromField, String toField) {
    this(q, fromField, toField, null);
  }
  
  /**
   * Create a graph query with a traversal filter applied while traversing the frontier.
   * q - the starting node query
   * fromField - the field containing the node id
   * toField - the field containing the edge ids
   * traversalFilter - the filter to be applied on each iteration of the frontier.
   */
  public GraphQuery(Query q, String fromField, String toField, Query traversalFilter) {
    this.q = q;
    this.fromField = fromField;
    this.toField = toField;
    this.traversalFilter = traversalFilter;
  }
  
  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    Weight graphWeight = new GraphQueryWeight((SolrIndexSearcher)searcher);
    return graphWeight;
  }
  
  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    sb.append("[[" + q.toString() + "]," + fromField + "=" + toField + "]");
    if (traversalFilter != null) {
      sb.append(" [TraversalFilter: " + traversalFilter.toString() + "]");
    }
    sb.append("[maxDepth=" + maxDepth + "]");
    sb.append("[returnRoot=" + returnRoot + "]");
    sb.append("[onlyLeafNodes=" + onlyLeafNodes + "]");
    sb.append("[useAutn=" + useAutn + "]");
    return sb.toString();
  }
  
  protected class GraphQueryWeight extends Weight {
    
    SolrIndexSearcher fromSearcher;
    private float queryNorm = 1.0F;
    private float queryWeight = 1.0F; 
    private int frontierSize = 0;
    private int currentDepth = -1;
    private Filter filter;
    private DocSet resultSet;
    
    public GraphQueryWeight(SolrIndexSearcher searcher) {
      // Grab the searcher so we can run additional searches.
      super(null);
      this.fromSearcher = searcher;
    }
    
    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      // currently no ranking for graph queries. 
      final Scorer cs = scorer(context);
      final boolean exists = (cs != null && cs.iterator().advance(doc) == doc);
      if (exists) {
        List<Explanation> subs = new ArrayList<Explanation>();
        return Explanation.match(1.0F, "Graph Match", subs);
      } else {
        List<Explanation> subs = new ArrayList<Explanation>();
        return Explanation.noMatch("No Graph Match.", subs);
      }
    }
    
    @Override
    public float getValueForNormalization() throws IOException {
      return 1F;
    }
    
    @Override
    public void normalize(float norm, float topLevelBoost) {
      this.queryWeight = norm * topLevelBoost;
    }
    
    /**
     * This computes the matching doc set for a given graph query
     * 
     * @return DocSet representing the documents in the graph.
     * @throws IOException - if a sub search fails... maybe other cases too! :)
     */
    private DocSet getDocSet() throws IOException {
      // Size that the bit set needs to be.
      int capacity = fromSearcher.getRawReader().maxDoc();
      // The bit set to contain the results that match the query.
      FixedBitSet resultBits = new FixedBitSet(capacity);
      // this holds the result at each level
      BitDocSet fromSet = null;
      // the root docs if we return root is false
      FixedBitSet rootBits = null;
      // the initial query for the frontier for the first query
      Query frontierQuery = q;
      // Find all documents in this graph that are leaf nodes to speed traversal
      DocSet leafNodes = resolveLeafNodes(toField);
      // Start the breadth first graph traversal.
      
      do {
        // Increment how far we have gone in the frontier.
        currentDepth++;
        // if we are at the max level we don't need the graph terms collector.
        // TODO validate that the join case works properly.
        if (maxDepth != -1 && currentDepth >= maxDepth) {
          // if we've reached the max depth, don't worry about collecting edges.
          fromSet = fromSearcher.getDocSetBits(frontierQuery);
          // explicitly the frontier size is zero now so we can break
          frontierSize = 0;
        } else {
          // when we're not at the max depth level, we need to collect edges          
          // Create the graph result collector for this level
          GraphTermsCollector graphResultCollector = new GraphTermsCollector(toField,capacity, resultBits, leafNodes);
          fromSearcher.search(frontierQuery, graphResultCollector);
          fromSet = graphResultCollector.getDocSet();
          // All edge ids on the frontier.
          BytesRefHash collectorTerms = graphResultCollector.getCollectorTerms();
          frontierSize = collectorTerms.size();
          // The resulting doc set from the frontier.
          FrontierQuery fq = buildFrontierQuery(collectorTerms, frontierSize);
          if (fq == null) {
            // in case we get null back, make sure we know we're done at this level.
            frontierSize = 0;
          } else {
            frontierQuery = fq.getQuery();
            frontierSize = fq.getFrontierSize();
          }
        }
        if (currentDepth == 0 && !returnRoot) {
          // grab a copy of the root bits but only if we need it.
          rootBits = fromSet.getBits();
        }
        // Add the bits from this level to the result set.
        resultBits.or(fromSet.getBits());
        // test if we discovered any new edges, if not , we're done.
        if ((maxDepth != -1 && currentDepth >= maxDepth)) {
          break;
        }
      } while (frontierSize > 0);
      // helper bit set operations on the final result set
      if (!returnRoot) {
        resultBits.andNot(rootBits);
      }
      // this is the final resulting filter.
      BitDocSet resultSet = new BitDocSet(resultBits);
      // If we only want to return leaf nodes do that here.
      if (onlyLeafNodes) {
        return resultSet.intersection(leafNodes);
      } else {
        return resultSet;
      }
    }
    
    private DocSet resolveLeafNodes(String field) throws IOException {
      BooleanQuery.Builder leafNodeQuery = new BooleanQuery.Builder();
      WildcardQuery edgeQuery = new WildcardQuery(new Term(field, "*"));
      leafNodeQuery.add(edgeQuery, Occur.MUST_NOT);
      DocSet leafNodes = fromSearcher.getDocSet(leafNodeQuery.build());
      return leafNodes;
    }
    
    /** Build an automaton to represent the frontier query */
    private Automaton buildAutomaton(BytesRefHash termBytesHash) {
      // need top pass a sorted set of terms to the autn builder (maybe a better way to avoid this?)
      final TreeSet<BytesRef> terms = new TreeSet<BytesRef>();
      for (int i = 0 ; i < termBytesHash.size(); i++) {
        BytesRef ref = new BytesRef();
        termBytesHash.get(i, ref);
        terms.add(ref);
      }
      final Automaton a = DaciukMihovAutomatonBuilder.build(terms);
      return a;    
    }
    
    /**
     * This return a query that represents the documents that match the next hop in the query.
     * 
     * collectorTerms - the terms that represent the edge ids for the current frontier.
     * frontierSize - the size of the frontier query (number of unique edges)
     *  
     */
    public FrontierQuery buildFrontierQuery(BytesRefHash collectorTerms, Integer frontierSize) {
      if (collectorTerms == null || collectorTerms.size() == 0) {
        // return null if there are no terms (edges) to traverse.
        return null;
      } else {
        // Create a query
        Query q = null;

        // TODO: see if we should dynamically select this based on the frontier size.
        if (useAutn) {
          // build an automaton based query for the frontier.
          Automaton autn = buildAutomaton(collectorTerms);
          AutomatonQuery autnQuery = new AutomatonQuery(new Term(fromField), autn);
          q = autnQuery;
        } else {
          List<BytesRef> termList = new ArrayList<>(collectorTerms.size());
          for (int i = 0 ; i < collectorTerms.size(); i++) {
            BytesRef ref = new BytesRef();
            collectorTerms.get(i, ref);
            termList.add(ref);
          }
          q = new TermsQuery(fromField, termList);
        }
        
        // If there is a filter to be used while crawling the graph, add that.
        if (traversalFilter != null) {
          BooleanQuery.Builder builder = new BooleanQuery.Builder();
          builder.add(q, Occur.MUST);
          builder.add(traversalFilter, Occur.MUST);
          q = builder.build();
        } 
        // return the new query. 
        FrontierQuery frontier = new FrontierQuery(q, frontierSize);
        return frontier;
      }
    }
    
    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      if (filter == null) {
        resultSet = getDocSet();
        filter = resultSet.getTopFilter();
      }
      DocIdSet readerSet = filter.getDocIdSet(context,context.reader().getLiveDocs());
      // create a scrorer on the result set, if results from right query are empty, use empty iterator.
      return new GraphScorer(this, readerSet == null ? DocIdSetIterator.empty() : readerSet.iterator(), 1);
    }
    
    @Override
    public void extractTerms(Set<Term> terms) {
      // NoOp for now , not used.. / supported
    }
    
  }
  
  private class GraphScorer extends Scorer {
    
    final DocIdSetIterator iter;
    final float score;
    // graph query scorer constructor with iterator
    public GraphScorer(Weight w, DocIdSetIterator iter, float score) throws IOException {
      super(w);
      this.iter = iter==null ? DocIdSet.EMPTY.iterator() : iter;
      this.score = score;
    }
    
    @Override
    public float score() throws IOException {
      // no dynamic scoring now.  
      return score;
    }

    @Override
    public DocIdSetIterator iterator() {
      return iter;
    }

    @Override
    public int docID() {
      // current position of the doc iterator.
      return iter.docID();
    }
    
    @Override
    public int freq() throws IOException {
      return 1;
    }
  }
  
  /**
   * @return The query to be used as a filter for each hop in the graph.
   */
  public Query getTraversalFilter() {
    return traversalFilter;
  }
  
  public void setTraversalFilter(Query traversalFilter) {
    this.traversalFilter = traversalFilter;
  }
  
  public Query getQ() {
    return q;
  }
  
  public void setQ(Query q) {
    this.q = q;
  }
  
  /**
   * @return The field that contains the node id
   */
  public String getFromField() {
    return fromField;
  }
  
  public void setFromField(String fromField) {
    this.fromField = fromField;
  }
  
  /**
   * @return the field that contains the edge id(s)
   */
  public String getToField() {
    return toField;
  }
  
  public void setToField(String toField) {
    this.toField = toField;
  }
  
  /**
   * @return Max depth for traversal,  -1 for infinite!
   */
  public int getMaxDepth() {
    return maxDepth;
  }
  
  public void setMaxDepth(int maxDepth) {
    this.maxDepth = maxDepth;
  }
  
  /**
   * @return If true , an automaton query will be compiled for each new frontier traversal
   * this helps to avoid max boolean clause errors.
   */
  public boolean isUseAutn() {
    return useAutn;
  }
  
  public void setUseAutn(boolean useAutn) {
    this.useAutn = useAutn;
  }
  
  /**
   * @return if true only documents that do not have a value in the edge id field will be returned.
   */
  public boolean isOnlyLeafNodes() {
    return onlyLeafNodes;
  }
  
  public void setOnlyLeafNodes(boolean onlyLeafNodes) {
    this.onlyLeafNodes = onlyLeafNodes;
  }
  
  /**
   * @return if true the documents that matched the rootNodes query will be returned.  o/w they will be removed from the result set.
   */
  public boolean isReturnRoot() {
    return returnRoot;
  }
  
  public void setReturnRoot(boolean returnRoot) {
    this.returnRoot = returnRoot;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + Objects.hashCode(fromField);
    result = prime * result + maxDepth;
    result = prime * result + (onlyLeafNodes ? 1231 : 1237);
    result = prime * result + Objects.hashCode(q);
    result = prime * result + (returnRoot ? 1231 : 1237);
    result = prime * result + Objects.hashCode(toField);
    result = prime * result + Objects.hashCode(traversalFilter);
    result = prime * result + (useAutn ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(GraphQuery other) {
    return Objects.equals(fromField, other.fromField) &&
           maxDepth == other.maxDepth &&
           onlyLeafNodes == other.onlyLeafNodes &&
           returnRoot == other.returnRoot &&
           useAutn == other.useAutn &&
           Objects.equals(q, other.q) &&
           Objects.equals(toField, other.toField) &&
           Objects.equals(traversalFilter, other.traversalFilter);
  }
  
}
