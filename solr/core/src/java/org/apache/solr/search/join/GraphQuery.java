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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.DaciukMihovAutomatonBuilder;
import org.apache.solr.schema.SchemaField;
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
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    Weight graphWeight = new GraphQueryWeight((SolrIndexSearcher)searcher, boost);
    return graphWeight;
  }
  
  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    sb.append("[[").append(q.toString()).append("],").append(fromField).append('=').append(toField).append(']');
    if (traversalFilter != null) {
      sb.append(" [TraversalFilter: ").append(traversalFilter.toString()).append(']');
    }
    sb.append("[maxDepth=").append(maxDepth).append(']');
    sb.append("[returnRoot=").append(returnRoot).append(']');
    sb.append("[onlyLeafNodes=").append(onlyLeafNodes).append(']');
    sb.append("[useAutn=").append(useAutn).append(']');
    return sb.toString();
  }
  
  protected class GraphQueryWeight extends Weight {
    
    final SolrIndexSearcher fromSearcher;
    private int currentDepth = -1;
    private Filter filter;
    private DocSet resultSet;
    SchemaField collectSchemaField;  // the field to collect values from
    SchemaField matchSchemaField;    // the field to match those values

    public GraphQueryWeight(SolrIndexSearcher searcher, float boost) {
      // Grab the searcher so we can run additional searches.
      super(null);
      this.fromSearcher = searcher;
      this.matchSchemaField = searcher.getSchema().getField(fromField);
      this.collectSchemaField = searcher.getSchema().getField(toField);
    }

    GraphQuery getGraphQuery() {
      return GraphQuery.this;
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
      DocSet leafNodes = resolveLeafNodes();
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
          frontierQuery = null;
        } else {
          // when we're not at the max depth level, we need to collect edges          
          // Create the graph result collector for this level
          GraphEdgeCollector graphResultCollector = collectSchemaField.getType().isPointField()
              ? new GraphPointsCollector(collectSchemaField, new BitDocSet(resultBits), leafNodes)
              : new GraphEdgeCollector.GraphTermsCollector(collectSchemaField, new BitDocSet(resultBits), leafNodes);

          fromSet = new BitDocSet(new FixedBitSet(capacity));
          graphResultCollector.setCollectDocs(fromSet.getBits());

          fromSearcher.search(frontierQuery, graphResultCollector);

          frontierQuery = graphResultCollector.getResultQuery(matchSchemaField, isUseAutn());
          // If there is a filter to be used while crawling the graph, add that.
          if (frontierQuery != null && getTraversalFilter() != null) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(frontierQuery, BooleanClause.Occur.MUST);
            builder.add(getTraversalFilter(), BooleanClause.Occur.MUST);
            frontierQuery = builder.build();
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
      } while (frontierQuery != null);
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
    
    private DocSet resolveLeafNodes() throws IOException {
      String field = collectSchemaField.getName();
      BooleanQuery.Builder leafNodeQuery = new BooleanQuery.Builder();
      Query edgeQuery = collectSchemaField.hasDocValues() ? new DocValuesFieldExistsQuery(field) : new WildcardQuery(new Term(field, "*"));
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
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      // NoOp for now , not used.. / supported
    }

  }
  
  private static class GraphScorer extends Scorer {
    
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
    public float getMaxScore(int upTo) throws IOException {
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

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

}
