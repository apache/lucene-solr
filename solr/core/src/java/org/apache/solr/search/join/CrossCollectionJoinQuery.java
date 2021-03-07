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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.UniqueStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.SolrIndexSearcher;

public class CrossCollectionJoinQuery extends Query {

  protected final String query;
  protected final String zkHost;
  protected final String solrUrl;
  protected final String collection;
  protected final String fromField;
  protected final String toField;
  protected final boolean routedByJoinKey;

  protected final long timestamp;
  protected final int ttl;

  protected SolrParams otherParams;
  protected String otherParamsString;

  public CrossCollectionJoinQuery(String query, String zkHost, String solrUrl,
                                  String collection, String fromField, String toField,
                                  boolean routedByJoinKey, int ttl, SolrParams otherParams) {

    this.query = query;
    this.zkHost = zkHost;
    this.solrUrl = solrUrl;
    this.collection = collection;
    this.fromField = fromField;
    this.toField = toField;
    this.routedByJoinKey = routedByJoinKey;

    this.timestamp = System.nanoTime();
    this.ttl = ttl;

    this.otherParams = otherParams;
    // SolrParams doesn't implement equals(), so use this string to compare them
    if (otherParams != null) {
      this.otherParamsString = otherParams.toString();
    }
  }

  private interface JoinKeyCollector {
    void collect(Object value) throws IOException;
    Query computeQuery() throws IOException;
  }

  private class TermsJoinKeyCollector implements JoinKeyCollector {

    FieldType fieldType;
    SolrIndexSearcher searcher;

    TermsEnum termsEnum;
    BytesRefBuilder bytes;
    PostingsEnum postingsEnum;

    FixedBitSet bitSet;

    public TermsJoinKeyCollector(FieldType fieldType, Terms terms, SolrIndexSearcher searcher) throws IOException {
      this.fieldType = fieldType;
      this.searcher = searcher;

      termsEnum = terms.iterator();
      bytes = new BytesRefBuilder();

      bitSet = new FixedBitSet(searcher.maxDoc());
    }

    @Override
    public void collect(Object value) throws IOException {
      fieldType.readableToIndexed((String) value, bytes);
      if (termsEnum.seekExact(bytes.get())) {
        postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
        bitSet.or(postingsEnum);
      }
    }

    @Override
    public Query computeQuery() throws IOException {
      if (searcher.getIndexReader().hasDeletions()) {
        bitSet.and(searcher.getLiveDocSet().getBits());
      }
      return new BitDocSet(bitSet).getTopFilter();
    }
  }

  private class PointJoinKeyCollector extends GraphPointsCollector implements JoinKeyCollector {

    SolrIndexSearcher searcher;

    public PointJoinKeyCollector(SolrIndexSearcher searcher) {
      super(searcher.getSchema().getField(toField), null, null);
      this.searcher = searcher;
    }

    @Override
    public void collect(Object value) throws IOException {
      if (value instanceof Long || value instanceof Integer) {
        set.add(((Number) value).longValue());
      } else {
        throw new UnsupportedOperationException("Unsupported field type for XCJFQuery");
      }
    }

    @Override
    public Query computeQuery() {
      return getResultQuery(searcher.getSchema().getField(toField), false);
    }
  }

  private class CrossCollectionJoinQueryWeight extends ConstantScoreWeight {

    private final SolrIndexSearcher searcher;
    private Weight toWeight;

    public CrossCollectionJoinQueryWeight(SolrIndexSearcher searcher, float score) {
      super(CrossCollectionJoinQuery.this, score);
      this.searcher = searcher;
    }

    private String createHashRangeFq() {
      if (routedByJoinKey) {
        ClusterState clusterState = searcher.getCore().getCoreContainer().getZkController().getClusterState();
        CloudDescriptor desc = searcher.getCore().getCoreDescriptor().getCloudDescriptor();
        Slice slice = clusterState.getCollection(desc.getCollectionName()).getSlicesMap().get(desc.getShardId());
        DocRouter.Range range = slice.getRange();

        // In CompositeIdRouter, the routing prefix only affects the top 16 bits
        int min = range.min & 0xffff0000;
        int max = range.max | 0x0000ffff;

        return String.format(Locale.ROOT, "{!hash_range f=%s l=%d u=%d}", fromField, min, max);
      } else {
        return null;
      }
    }

    private TupleStream createCloudSolrStream(SolrClientCache solrClientCache) throws IOException {
      String streamZkHost;
      if (zkHost != null) {
        streamZkHost = zkHost;
      } else {
        streamZkHost = searcher.getCore().getCoreContainer().getZkController().getZkServerAddress();
      }

      ModifiableSolrParams params = new ModifiableSolrParams(otherParams);
      params.set(CommonParams.Q, query);
      String fq = createHashRangeFq();
      if (fq != null) {
        params.add(CommonParams.FQ, fq);
      }
      params.set(CommonParams.FL, fromField);
      params.set(CommonParams.SORT, fromField + " asc");
      params.set(CommonParams.QT, "/export");
      params.set(CommonParams.WT, CommonParams.JAVABIN);

      StreamContext streamContext = new StreamContext();
      streamContext.setSolrClientCache(solrClientCache);

      TupleStream cloudSolrStream = new CloudSolrStream(streamZkHost, collection, params);
      TupleStream uniqueStream = new UniqueStream(cloudSolrStream, new FieldEqualitor(fromField));
      uniqueStream.setStreamContext(streamContext);
      return uniqueStream;
    }

    private TupleStream createSolrStream() {
      StreamExpression searchExpr = new StreamExpression("search")
              .withParameter(collection)
              .withParameter(new StreamExpressionNamedParameter(CommonParams.Q, query));
      String fq = createHashRangeFq();
      if (fq != null) {
        searchExpr.withParameter(new StreamExpressionNamedParameter(CommonParams.FQ, fq));
      }
      searchExpr.withParameter(new StreamExpressionNamedParameter(CommonParams.FL, fromField))
              .withParameter(new StreamExpressionNamedParameter(CommonParams.SORT, fromField + " asc"))
              .withParameter(new StreamExpressionNamedParameter(CommonParams.QT, "/export"));

      for (Map.Entry<String,String[]> entry : otherParams) {
        for (String value : entry.getValue()) {
          searchExpr.withParameter(new StreamExpressionNamedParameter(entry.getKey(), value));
        }
      }

      StreamExpression uniqueExpr = new StreamExpression("unique");
      uniqueExpr.withParameter(searchExpr)
              .withParameter(new StreamExpressionNamedParameter("over", fromField));

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("expr", uniqueExpr.toString());
      params.set(CommonParams.QT, "/stream");
      params.set(CommonParams.WT, CommonParams.JAVABIN);

      return new SolrStream(solrUrl + "/" + collection, params);
    }

    private Query computeToQuery() throws IOException {
      SolrClientCache solrClientCache = searcher.getCore().getCoreContainer().getSolrClientCache();
      TupleStream solrStream;
      if (zkHost != null || solrUrl == null) {
        solrStream = createCloudSolrStream(solrClientCache);
      } else {
        solrStream = createSolrStream();
      }

      FieldType fieldType = searcher.getSchema().getFieldType(toField);
      JoinKeyCollector collector;
      if (fieldType.isPointField()) {
        collector = new PointJoinKeyCollector(searcher);
      } else {
        Terms terms = searcher.getSlowAtomicReader().terms(toField);
        if (terms == null) {
          return new MatchNoDocsQuery();
        }
        collector = new TermsJoinKeyCollector(fieldType, terms, searcher);
      }

      try {
        solrStream.open();
        while (true) {
          Tuple tuple = solrStream.read();
          if (tuple.EXCEPTION) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, tuple.getException());
          }
          if (tuple.EOF) {
            break;
          }

          Object value = tuple.get(fromField);
          collector.collect(value);
        }
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } finally {
        solrStream.close();
      }

      return collector.computeQuery();
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      if (toWeight == null) {
        float boost = super.score(); // boost==score
        toWeight = searcher.createWeight(computeToQuery(), ScoreMode.COMPLETE_NO_SCORES, boost);
      }
      return toWeight.scorer(context);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new CrossCollectionJoinQueryWeight((SolrIndexSearcher) searcher, boost);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + Objects.hashCode(query);
    result = prime * result + Objects.hashCode(zkHost);
    result = prime * result + Objects.hashCode(solrUrl);
    result = prime * result + Objects.hashCode(collection);
    result = prime * result + Objects.hashCode(fromField);
    result = prime * result + Objects.hashCode(toField);
    result = prime * result + Objects.hashCode(routedByJoinKey);
    result = prime * result + Objects.hashCode(otherParamsString);
    // timestamp and ttl should not be included in hash code
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
            equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(CrossCollectionJoinQuery other) {
    return Objects.equals(query, other.query) &&
            Objects.equals(zkHost, other.zkHost) &&
            Objects.equals(solrUrl, other.solrUrl) &&
            Objects.equals(collection, other.collection) &&
            Objects.equals(fromField, other.fromField) &&
            Objects.equals(toField, other.toField) &&
            Objects.equals(routedByJoinKey, other.routedByJoinKey) &&
            Objects.equals(otherParamsString, other.otherParamsString) &&
            TimeUnit.SECONDS.convert(Math.abs(timestamp - other.timestamp), TimeUnit.NANOSECONDS) < Math.min(ttl, other.ttl);
  }

  @Override
  public String toString(String field) {
    return String.format(Locale.ROOT, "{!xcjf collection=%s from=%s to=%s routed=%b ttl=%d}%s",
            collection, fromField, toField, routedByJoinKey, ttl, query.toString());
  }
}
