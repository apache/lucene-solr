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

import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.lucene.search.*;
import org.apache.lucene.index.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.common.params.SolrParams;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class ExportQParserPlugin extends QParserPlugin {

  public static final String NAME = "xport";
  
  public void init(NamedList namedList) {
  }
  
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
    
    private int leafCount;
    private Query mainQuery;
    private Object id;

    public Query clone() {
      ExportQuery clone = new ExportQuery();
      clone.id = id;
      clone.leafCount = leafCount;
      clone.mainQuery = mainQuery;
      return clone;
    }

    public RankQuery wrap(Query mainQuery) {
      this.mainQuery = mainQuery;
      return this;
    }

    public MergeStrategy getMergeStrategy() {
      return null;
    }

    public Weight createWeight(IndexSearcher searcher) throws IOException {
      return mainQuery.createWeight(searcher);
    }

    public Query rewrite(IndexReader reader) throws IOException {
      return this.mainQuery.rewrite(reader);
    }

    public void extractTerms(Set<Term> terms) {
      this.mainQuery.extractTerms(terms);
    }

    public TopDocsCollector getTopDocsCollector(int len,
                                                SolrIndexSearcher.QueryCommand cmd,
                                                IndexSearcher searcher) throws IOException {
      FixedBitSet[] sets = new FixedBitSet[this.leafCount];
      return new ExportCollector(sets);
    }

    public int hashCode() {
      return id.hashCode()+((int)getBoost());
    }
    
    public boolean equals(Object o) {
      if(o instanceof ExportQuery) {
        ExportQuery q = (ExportQuery)o;
        return (this.id == q.id && getBoost() == q.getBoost());
      } else {
        return false;
      }
    }
    
    public String toString(String s) {
      return s;
    }

    public ExportQuery() {

    }
    
    public ExportQuery(SolrParams localParams, SolrParams params, SolrQueryRequest request) throws IOException {
      this.leafCount = request.getSearcher().getTopReaderContext().leaves().size();
      id = new Object();
    }
  }
  
  private class ExportCollector extends TopDocsCollector  {

    private FixedBitSet[] sets;
    private FixedBitSet set;

    public ExportCollector(FixedBitSet[] sets) {
      super(null);
      this.sets = sets;
    }
    
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.set = new FixedBitSet(context.reader().maxDoc());
      this.sets[context.ord] = set;

    }
    
    public void collect(int docId) throws IOException{
      ++totalHits;
      set.set(docId);
    }

    private ScoreDoc[] getScoreDocs(int howMany) {
      ScoreDoc[] docs = new ScoreDoc[howMany];
      for(int i=0; i<docs.length; i++) {
        docs[i] = new ScoreDoc(i,0);
      }
      return docs;
    }

    public TopDocs topDocs(int start, int howMany) {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      SolrQueryRequest req = null;
      if(info != null && ((req = info.getReq()) != null)) {
        Map context = req.getContext();
        context.put("export", sets);
        context.put("totalHits", totalHits);

      }
      return new TopDocs(totalHits, getScoreDocs(howMany), 0.0f);
    }

    public void setScorer(Scorer scorer) throws IOException {

    }
    
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }
  }
}