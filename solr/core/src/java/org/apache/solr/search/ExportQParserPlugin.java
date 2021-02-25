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
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;

public class ExportQParserPlugin extends QParserPlugin {

  public static final String NAME = "xport";

  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
    return new ExportQParser(qstr, localParams, params, request);
  }

  public class ExportQParser extends QParser {
    
    public ExportQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
      super(qstr, localParams, params, request);
    }
    
    public Query parse() throws SyntaxError {
      try {
          return new ExportQuery(localParams, params, req);
        } catch (Exception e) {
          throw new SyntaxError(e.getMessage(), e);
        }
    }
  }

  public class ExportQuery extends RankQuery {
    private Query mainQuery;
    private Object id;

    public RankQuery clone() {
      ExportQuery clone = new ExportQuery();
      clone.id = id;
      return clone;
    }

    public RankQuery wrap(Query mainQuery) {
      this.mainQuery = mainQuery;
      return this;
    }

    public MergeStrategy getMergeStrategy() {
      return null;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException{
      return mainQuery.createWeight(searcher, scoreMode, boost);
    }

    public Query rewrite(IndexReader reader) throws IOException {
      Query q = mainQuery.rewrite(reader);
      if(q == mainQuery) {
        return super.rewrite(reader);
      } else {
        return clone().wrap(q);
      }
    }

    @SuppressWarnings({"rawtypes"})
    public TopDocsCollector getTopDocsCollector(int len,
                                                QueryCommand cmd,
                                                IndexSearcher searcher) throws IOException {
      int leafCount = searcher.getTopReaderContext().leaves().size();
      FixedBitSet[] sets = new FixedBitSet[leafCount];
      return new ExportCollector(sets);
    }

    public int hashCode() {
      return classHash() + 
          31 * id.hashCode() +
          31 * Objects.hash(mainQuery);
    }

    public boolean equals(Object other) {
      return sameClassAs(other) &&
             equalsTo(getClass().cast(other));
    }
    
    private boolean equalsTo(ExportQuery other) {
      return Objects.equals(id, other.id) &&
             Objects.equals(mainQuery, other.mainQuery);
    }

    public String toString(String s) {
      return s;
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    public ExportQuery() {

    }
    
    public ExportQuery(SolrParams localParams, SolrParams params, SolrQueryRequest request) throws IOException {
      id = new Object();
    }
  }
  
  @SuppressWarnings({"rawtypes"})
  private static class ExportCollector extends TopDocsCollector  {

    private FixedBitSet[] sets;

    @SuppressWarnings({"unchecked"})
    public ExportCollector(FixedBitSet[] sets) {
      super(null);
      this.sets = sets;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      final FixedBitSet set = new FixedBitSet(context.reader().maxDoc());
      this.sets[context.ord] = set;
      return new LeafCollector() {
        
        @Override
        public void setScorer(Scorable scorer) throws IOException {}
        
        @Override
        public void collect(int docId) throws IOException{
          ++totalHits;
          set.set(docId);
        }
      };
    }

    private ScoreDoc[] getScoreDocs(int howMany) {
      ScoreDoc[] docs = new ScoreDoc[Math.min(totalHits, howMany)];
      for(int i=0; i<docs.length; i++) {
        docs[i] = new ScoreDoc(i,0);
      }

      return docs;
    }

    @SuppressWarnings({"unchecked"})
    public TopDocs topDocs(int start, int howMany) {

      assert(sets != null);

      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();

      SolrQueryRequest req = null;
      if(info != null && ((req = info.getReq()) != null)) {
        @SuppressWarnings({"rawtypes"})
        Map context = req.getContext();
        context.put("export", sets);
        context.put("totalHits", totalHits);
      }

      ScoreDoc[] scoreDocs = getScoreDocs(howMany);
      assert scoreDocs.length <= totalHits;
      return new TopDocs(new TotalHits(totalHits, totalHitsRelation), scoreDocs);
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

}
