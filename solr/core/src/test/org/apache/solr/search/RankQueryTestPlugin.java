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
package org.apache.solr.search;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;


public class RankQueryTestPlugin extends QParserPlugin {


  public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new TestRankQueryParser(query, localParams, params, req);
  }

  static class TestRankQueryParser extends QParser {

    public TestRankQueryParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(query, localParams, params, req);
    }

    public Query parse() throws SyntaxError {

      int mergeStrategy = localParams.getInt("mergeStrategy", 0);
      int collector = localParams.getInt("collector", 0);
      return new TestRankQuery(collector, mergeStrategy);
    }
  }

  static class TestRankQuery extends RankQuery {

    private int mergeStrategy;
    private int collector;
    private Query q;

    public int hashCode() {
      return collector+q.hashCode();
    }

    public boolean equals(Object o) {
      if(o instanceof TestRankQuery) {
        TestRankQuery trq = (TestRankQuery)o;

        return (trq.q.equals(q) && trq.collector == collector) ;
      }

      return false;
    }

    public Weight createWeight(IndexSearcher indexSearcher, ScoreMode scoreMode, float boost) throws IOException{
      return q.createWeight(indexSearcher, scoreMode, boost);
    }

    @Override
    public void visit(QueryVisitor visitor) {

    }

    @Override
    public String toString(String field) {
      return q.toString(field);
    }

    public RankQuery wrap(Query q) {
      this.q = q;
      return this;
    }

    public TestRankQuery(int collector, int mergeStrategy) {
      this.collector = collector;
      this.mergeStrategy = mergeStrategy;
    }

    @SuppressWarnings({"rawtypes"})
    public TopDocsCollector getTopDocsCollector(int len, QueryCommand cmd, IndexSearcher searcher) {
      if(collector == 0)
        return new TestCollector(null);
      else
        return new TestCollector1(null);
    }

    public MergeStrategy getMergeStrategy() {
      if(mergeStrategy == 0)
        return new TestMergeStrategy();
      else
        return new TestMergeStrategy1();
    }
  }

  static class TestMergeStrategy implements MergeStrategy {

    public int getCost() {
      return 1;
    }

    public boolean mergesIds() {
      return true;
    }

    public boolean handlesMergeFields() {
      return false;
    }

    public void handleMergeFields(ResponseBuilder rb, SolrIndexSearcher searcher) {

    }

    @SuppressWarnings({"unchecked"})
    public void merge(ResponseBuilder rb, ShardRequest sreq) {

      // id to shard mapping, to eliminate any accidental dups
      HashMap<Object,String> uniqueDoc = new HashMap<>();


      NamedList<Object> shardInfo = null;
      if(rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
        shardInfo = new SimpleOrderedMap<>();
        rb.rsp.getValues().add(ShardParams.SHARDS_INFO,shardInfo);
      }

      IndexSchema schema = rb.req.getSchema();
      SchemaField uniqueKeyField = schema.getUniqueKeyField();

      long numFound = 0;
      Float maxScore=null;
      boolean partialResults = false;
      List<ShardDoc> shardDocs = new ArrayList<>();

      for (ShardResponse srsp : sreq.responses) {
        SolrDocumentList docs = null;

        if(shardInfo!=null) {
          SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();

          if (srsp.getException() != null) {
            Throwable t = srsp.getException();
            if(t instanceof SolrServerException) {
              t = ((SolrServerException)t).getCause();
            }
            nl.add("error", t.toString() );
            StringWriter trace = new StringWriter();
            t.printStackTrace(new PrintWriter(trace));
            nl.add("trace", trace.toString() );
            if (srsp.getShardAddress() != null) {
              nl.add("shardAddress", srsp.getShardAddress());
            }
          }
          else {
            docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");
            nl.add("numFound", docs.getNumFound());
            nl.add("maxScore", docs.getMaxScore());
            nl.add("shardAddress", srsp.getShardAddress());
          }
          if(srsp.getSolrResponse()!=null) {
            nl.add("time", srsp.getSolrResponse().getElapsedTime());
          }

          shardInfo.add(srsp.getShard(), nl);
        }
        // now that we've added the shard info, let's only proceed if we have no error.
        if (srsp.getException() != null) {
          partialResults = true;
          continue;
        }

        if (docs == null) { // could have been initialized in the shards info block above
          docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");
        }

        NamedList<?> responseHeader = (NamedList<?>)srsp.getSolrResponse().getResponse().get("responseHeader");
        if (responseHeader != null && Boolean.TRUE.equals(responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY))) {
          partialResults = true;
        }

        // calculate global maxScore and numDocsFound
        if (docs.getMaxScore() != null) {
          maxScore = maxScore==null ? docs.getMaxScore() : Math.max(maxScore, docs.getMaxScore());
        }
        numFound += docs.getNumFound();


        for (int i=0; i<docs.size(); i++) {
          SolrDocument doc = docs.get(i);
          Object id = doc.getFieldValue(uniqueKeyField.getName());

          String prevShard = uniqueDoc.put(id, srsp.getShard());
          if (prevShard != null) {
            // duplicate detected
            numFound--;

            // For now, just always use the first encountered since we can't currently
            // remove the previous one added to the priority queue.  If we switched
            // to the Java5 PriorityQueue, this would be easier.
            continue;
            // make which duplicate is used deterministic based on shard
            // if (prevShard.compareTo(srsp.shard) >= 0) {
            //  TODO: remove previous from priority queue
            //  continue;
            // }
          }

          ShardDoc shardDoc = new ShardDoc();
          shardDoc.id = id;
          shardDoc.shard = srsp.getShard();
          shardDoc.orderInShard = i;
          Object scoreObj = doc.getFieldValue("score");
          if (scoreObj != null) {
            if (scoreObj instanceof String) {
              shardDoc.score = Float.parseFloat((String)scoreObj);
            } else {
              shardDoc.score = (Float)scoreObj;
            }
          }
          shardDocs.add(shardDoc);
        } // end for-each-doc-in-response
      } // end for-each-response

      Collections.sort(shardDocs, (o1, o2) -> {
        if (o1.score < o2.score) {
          return 1;
        } else if (o1.score > o2.score) {
          return -1;
        } else {
          return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }
      });

      int resultSize = shardDocs.size();

      Map<Object,ShardDoc> resultIds = new HashMap<>();
      for (int i=0; i<shardDocs.size(); i++) {
        ShardDoc shardDoc = shardDocs.get(i);
        shardDoc.positionInResponse = i;
        // Need the toString() for correlation with other lists that must
        // be strings (like keys in highlighting, explain, etc)
        resultIds.put(shardDoc.id.toString(), shardDoc);
      }

      // Add hits for distributed requests
      // https://issues.apache.org/jira/browse/SOLR-3518
      rb.rsp.addToLog("hits", numFound);

      SolrDocumentList responseDocs = new SolrDocumentList();
      if (maxScore!=null) responseDocs.setMaxScore(maxScore);
      responseDocs.setNumFound(numFound);
      responseDocs.setStart(0);
      // size appropriately
      for (int i=0; i<resultSize; i++) responseDocs.add(null);

      // save these results in a private area so we can access them
      // again when retrieving stored fields.
      // TODO: use ResponseBuilder (w/ comments) or the request context?
      rb.resultIds = resultIds;
      rb.setResponseDocs(responseDocs);

      if (partialResults) {
        rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE );
      }
    }
  }

  static class TestMergeStrategy1 implements MergeStrategy {

    public int getCost() {
      return 1;
    }

    public boolean mergesIds() {
      return true;
    }

    public boolean handlesMergeFields() {
      return true;
    }

    public void handleMergeFields(ResponseBuilder rb, SolrIndexSearcher searcher) throws IOException {
      SolrQueryRequest req = rb.req;
      SolrQueryResponse rsp = rb.rsp;
      // The query cache doesn't currently store sort field values, and SolrIndexSearcher doesn't
      // currently have an option to return sort field values.  Because of this, we
      // take the documents given and re-derive the sort values.
      //
      // TODO: See SOLR-5595
      boolean fsv = req.getParams().getBool(ResponseBuilder.FIELD_SORT_VALUES,false);
      if(fsv){
        NamedList<Object[]> sortVals = new NamedList<>(); // order is important for the sort fields
        IndexReaderContext topReaderContext = searcher.getTopReaderContext();
        List<LeafReaderContext> leaves = topReaderContext.leaves();
        LeafReaderContext currentLeaf = null;
        if (leaves.size()==1) {
          // if there is a single segment, use that subReader and avoid looking up each time
          currentLeaf = leaves.get(0);
          leaves=null;
        }

        DocList docList = rb.getResults().docList;

        // sort ids from lowest to highest so we can access them in order
        int nDocs = docList.size();
        final long[] sortedIds = new long[nDocs];
        final float[] scores = new float[nDocs]; // doc scores, parallel to sortedIds
        DocList docs = rb.getResults().docList;
        DocIterator it = docs.iterator();
        for (int i=0; i<nDocs; i++) {
          sortedIds[i] = (((long)it.nextDoc()) << 32) | i;
          scores[i] = docs.hasScores() ? it.score() : Float.NaN;
        }

        // sort ids and scores together
        new InPlaceMergeSorter() {
          @Override
          protected void swap(int i, int j) {
            long tmpId = sortedIds[i];
            float tmpScore = scores[i];
            sortedIds[i] = sortedIds[j];
            scores[i] = scores[j];
            sortedIds[j] = tmpId;
            scores[j] = tmpScore;
          }

          @Override
          protected int compare(int i, int j) {
            return Long.compare(sortedIds[i], sortedIds[j]);
          }
        }.sort(0, sortedIds.length);

        SortSpec sortSpec = rb.getSortSpec();
        Sort sort = searcher.weightSort(sortSpec.getSort());
        SortField[] sortFields = sort==null ? new SortField[]{SortField.FIELD_SCORE} : sort.getSort();
        List<SchemaField> schemaFields = sortSpec.getSchemaFields();

        for (int fld = 0; fld < schemaFields.size(); fld++) {
          SchemaField schemaField = schemaFields.get(fld);
          FieldType ft = null == schemaField? null : schemaField.getType();
          SortField sortField = sortFields[fld];

          SortField.Type type = sortField.getType();
          // :TODO: would be simpler to always serialize every position of SortField[]
          if (type==SortField.Type.SCORE || type==SortField.Type.DOC) continue;

          FieldComparator<?> comparator = null;
          LeafFieldComparator leafComparator = null;
          Object[] vals = new Object[nDocs];

          int lastIdx = -1;
          int idx = 0;

          for (int i = 0; i < sortedIds.length; ++i) {
            long idAndPos = sortedIds[i];
            float score = scores[i];
            int doc = (int)(idAndPos >>> 32);
            int position = (int)idAndPos;

            if (leaves != null) {
              idx = ReaderUtil.subIndex(doc, leaves);
              currentLeaf = leaves.get(idx);
              if (idx != lastIdx) {
                // we switched segments.  invalidate comparator.
                comparator = null;
              }
            }

            if (comparator == null) {
              comparator = sortField.getComparator(1,0);
              leafComparator = comparator.getLeafComparator(currentLeaf);
            }

            doc -= currentLeaf.docBase;  // adjust for what segment this is in
            leafComparator.setScorer(new ScoreAndDoc(doc, score));
            leafComparator.copy(0, doc);
            Object val = comparator.value(0);
            if (null != ft) val = ft.marshalSortValue(val);
            vals[position] = val;
          }

          sortVals.add(sortField.getField(), vals);
        }

        rsp.add("merge_values", sortVals);
      }
    }

    private static class ScoreAndDoc extends Scorable {

      final int docid;
      final float score;

      ScoreAndDoc(int docid, float score) {
        this.docid = docid;
        this.score = score;
      }

      @Override
      public int docID() {
        return docid;
      }

      @Override
      public float score() {
        return score;
      }
    }

    @SuppressWarnings({"unchecked"})
    public void merge(ResponseBuilder rb, ShardRequest sreq) {

      // id to shard mapping, to eliminate any accidental dups
      HashMap<Object,String> uniqueDoc = new HashMap<>();


      NamedList<Object> shardInfo = null;
      if(rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
        shardInfo = new SimpleOrderedMap<>();
        rb.rsp.getValues().add(ShardParams.SHARDS_INFO,shardInfo);
      }

      IndexSchema schema = rb.req.getSchema();
      SchemaField uniqueKeyField = schema.getUniqueKeyField();

      long numFound = 0;
      Float maxScore=null;
      boolean partialResults = false;
      List<ShardDoc> shardDocs = new ArrayList<>();

      for (ShardResponse srsp : sreq.responses) {
        SolrDocumentList docs = null;

        if(shardInfo!=null) {
          SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();

          if (srsp.getException() != null) {
            Throwable t = srsp.getException();
            if(t instanceof SolrServerException) {
              t = ((SolrServerException)t).getCause();
            }
            nl.add("error", t.toString() );
            StringWriter trace = new StringWriter();
            t.printStackTrace(new PrintWriter(trace));
            nl.add("trace", trace.toString() );
            if (srsp.getShardAddress() != null) {
              nl.add("shardAddress", srsp.getShardAddress());
            }
          }
          else {
            docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");
            nl.add("numFound", docs.getNumFound());
            nl.add("maxScore", docs.getMaxScore());
            nl.add("shardAddress", srsp.getShardAddress());
          }
          if(srsp.getSolrResponse()!=null) {
            nl.add("time", srsp.getSolrResponse().getElapsedTime());
          }

          shardInfo.add(srsp.getShard(), nl);
        }
        // now that we've added the shard info, let's only proceed if we have no error.
        if (srsp.getException() != null) {
          partialResults = true;
          continue;
        }

        if (docs == null) { // could have been initialized in the shards info block above
          docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");
        }

        NamedList<?> responseHeader = (NamedList<?>)srsp.getSolrResponse().getResponse().get("responseHeader");
        if (responseHeader != null && Boolean.TRUE.equals(responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY))) {
          partialResults = true;
        }

        // calculate global maxScore and numDocsFound
        if (docs.getMaxScore() != null) {
          maxScore = maxScore==null ? docs.getMaxScore() : Math.max(maxScore, docs.getMaxScore());
        }
        numFound += docs.getNumFound();

        SortSpec ss = rb.getSortSpec();
        Sort sort = ss.getSort();

        @SuppressWarnings({"rawtypes"})
        NamedList sortFieldValues = (NamedList)(srsp.getSolrResponse().getResponse().get("merge_values"));
        @SuppressWarnings({"rawtypes"})
        NamedList unmarshalledSortFieldValues = unmarshalSortValues(ss, sortFieldValues, schema);
        @SuppressWarnings({"rawtypes"})
        List lst = (List)unmarshalledSortFieldValues.getVal(0);

        for (int i=0; i<docs.size(); i++) {
          SolrDocument doc = docs.get(i);
          Object id = doc.getFieldValue(uniqueKeyField.getName());

          String prevShard = uniqueDoc.put(id, srsp.getShard());
          if (prevShard != null) {
            // duplicate detected
            numFound--;

            // For now, just always use the first encountered since we can't currently
            // remove the previous one added to the priority queue.  If we switched
            // to the Java5 PriorityQueue, this would be easier.
            continue;
            // make which duplicate is used deterministic based on shard
            // if (prevShard.compareTo(srsp.shard) >= 0) {
            //  TODO: remove previous from priority queue
            //  continue;
            // }
          }

          ShardDoc shardDoc = new ShardDoc();
          shardDoc.id = id;
          shardDoc.shard = srsp.getShard();
          shardDoc.orderInShard = i;
          Object scoreObj = lst.get(i);
          if (scoreObj != null) {
            shardDoc.score = ((Integer)scoreObj).floatValue();
          }
          shardDocs.add(shardDoc);
        } // end for-each-doc-in-response
      } // end for-each-response

      Collections.sort(shardDocs, (o1, o2) -> {
        if (o1.score < o2.score) {
          return 1;
        } else if (o1.score > o2.score) {
          return -1;
        } else {
          return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }
      });

      int resultSize = shardDocs.size();

      Map<Object,ShardDoc> resultIds = new HashMap<>();
      for (int i=0; i<shardDocs.size(); i++) {
        ShardDoc shardDoc = shardDocs.get(i);
        shardDoc.positionInResponse = i;
        // Need the toString() for correlation with other lists that must
        // be strings (like keys in highlighting, explain, etc)
        resultIds.put(shardDoc.id.toString(), shardDoc);
      }

      // Add hits for distributed requests
      // https://issues.apache.org/jira/browse/SOLR-3518
      rb.rsp.addToLog("hits", numFound);

      SolrDocumentList responseDocs = new SolrDocumentList();
      if (maxScore!=null) responseDocs.setMaxScore(maxScore);
      responseDocs.setNumFound(numFound);
      responseDocs.setStart(0);
      // size appropriately
      for (int i=0; i<resultSize; i++) responseDocs.add(null);

      // save these results in a private area so we can access them
      // again when retrieving stored fields.
      // TODO: use ResponseBuilder (w/ comments) or the request context?
      rb.resultIds = resultIds;
      rb.setResponseDocs(responseDocs);

      if (partialResults) {
        rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE );
      }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private NamedList unmarshalSortValues(SortSpec sortSpec,
                                          NamedList sortFieldValues,
                                          IndexSchema schema) {
      NamedList unmarshalledSortValsPerField = new NamedList();

      if (0 == sortFieldValues.size()) return unmarshalledSortValsPerField;

      List<SchemaField> schemaFields = sortSpec.getSchemaFields();
      SortField[] sortFields = sortSpec.getSort().getSort();

      int marshalledFieldNum = 0;
      for (int sortFieldNum = 0; sortFieldNum < sortFields.length; sortFieldNum++) {
        final SortField sortField = sortFields[sortFieldNum];
        final SortField.Type type = sortField.getType();

        // :TODO: would be simpler to always serialize every position of SortField[]
        if (type==SortField.Type.SCORE || type==SortField.Type.DOC) continue;

        final String sortFieldName = sortField.getField();
        final String valueFieldName = sortFieldValues.getName(marshalledFieldNum);
        assert sortFieldName.equals(valueFieldName)
            : "sortFieldValues name key does not match expected SortField.getField";

        List sortVals = (List)sortFieldValues.getVal(marshalledFieldNum);

        final SchemaField schemaField = schemaFields.get(sortFieldNum);
        if (null == schemaField) {
          unmarshalledSortValsPerField.add(sortField.getField(), sortVals);
        } else {
          FieldType fieldType = schemaField.getType();
          List unmarshalledSortVals = new ArrayList();
          for (Object sortVal : sortVals) {
            unmarshalledSortVals.add(fieldType.unmarshalSortValue(sortVal));
          }
          unmarshalledSortValsPerField.add(sortField.getField(), unmarshalledSortVals);
        }
        marshalledFieldNum++;
      }
      return unmarshalledSortValsPerField;
    }
  }


  @SuppressWarnings({"rawtypes"})
  static class TestCollector extends TopDocsCollector {

    private List<ScoreDoc> list = new ArrayList<>();

    @SuppressWarnings({"unchecked"})
    public TestCollector(@SuppressWarnings({"rawtypes"})PriorityQueue pq) {
      super(pq);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      final int base = context.docBase;
      final NumericDocValues values = DocValues.getNumeric(context.reader(), "sort_i");
      return new LeafCollector() {
        
        @Override
        public void setScorer(Scorable scorer) throws IOException {}
        
        public void collect(int doc) throws IOException {
          long value;
          if (values.advanceExact(doc)) {
            value = values.longValue();
          } else {
            value = 0;
          }
          list.add(new ScoreDoc(doc+base, (float) value));
        }
      };
    }

    public int topDocsSize() {
      return list.size();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public TopDocs topDocs() {
      Collections.sort(list, new Comparator() {
        public int compare(Object o1, Object o2) {
          ScoreDoc s1 = (ScoreDoc) o1;
          ScoreDoc s2 = (ScoreDoc) o2;
          if (s1.score == s2.score) {
            return 0;
          } else if (s1.score < s2.score) {
            return 1;
          } else {
            return -1;
          }
        }
      });
      ScoreDoc[] scoreDocs = list.toArray(new ScoreDoc[list.size()]);
      return new TopDocs(new TotalHits(list.size(), TotalHits.Relation.EQUAL_TO), scoreDocs);
    }

    public TopDocs topDocs(int start, int len) {
      return topDocs();
    }

    public int getTotalHits() {
      return list.size();
    }
    
    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }
  }

  @SuppressWarnings({"rawtypes"})
  static class TestCollector1 extends TopDocsCollector {

    private List<ScoreDoc> list = new ArrayList<>();

    @SuppressWarnings({"unchecked"})
    public TestCollector1(@SuppressWarnings({"rawtypes"})PriorityQueue pq) {
      super(pq);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      final int base = context.docBase;
      return new LeafCollector() {
        
        Scorable scorer;
        
        @Override
        public void setScorer(Scorable scorer) throws IOException {
          this.scorer = scorer;
        }
        
        public void collect(int doc) throws IOException {
          list.add(new ScoreDoc(doc+base, scorer.score()));
        }
      };
    }

    public int topDocsSize() {
      return list.size();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public TopDocs topDocs() {
      Collections.sort(list, new Comparator() {
        public int compare(Object o1, Object o2) {
          ScoreDoc s1 = (ScoreDoc) o1;
          ScoreDoc s2 = (ScoreDoc) o2;
          if (s1.score == s2.score) {
            return 0;
          } else if (s1.score > s2.score) {
            return 1;
          } else {
            return -1;
          }
        }
      });
      ScoreDoc[] scoreDocs = list.toArray(new ScoreDoc[list.size()]);
      return new TopDocs(new TotalHits(list.size(), TotalHits.Relation.EQUAL_TO), scoreDocs);
    }

    public TopDocs topDocs(int start, int len) {
      return topDocs();
    }

    public int getTotalHits() {
      return list.size();
    }
    
    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }
  }

}
