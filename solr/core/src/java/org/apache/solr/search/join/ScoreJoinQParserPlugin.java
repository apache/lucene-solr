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

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.JoinQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.RefCounted;

/**
 * Create a query-time join query with scoring. 
 * It just calls  {@link JoinUtil#createJoinQuery(String, boolean, String, Query, org.apache.lucene.search.IndexSearcher, ScoreMode)}.
 * It runs subordinate query and collects values of "from"  field and scores, then it lookups these collected values in "to" field, and
 * yields aggregated scores.
 * Local parameters are similar to {@link JoinQParserPlugin} <a href="http://wiki.apache.org/solr/Join">{!join}</a>
 * This plugin doesn't have own name, and is called by specifying local parameter <code>{!join score=...}...</code>. 
 * Note: this parser is invoked even if you specify <code>score=none</code>.
 * <br>Example:<code>q={!join from=manu_id_s to=id score=total}foo</code>
 * <ul>
 *  <li>from - "foreign key" field name to collect values while enumerating subordinate query (denoted as <code>foo</code> in example above).
 *             it's better to have this field declared as <code>type="string" docValues="true"</code>.
 *             note: if <a href="http://wiki.apache.org/solr/DocValues">docValues</a> are not enabled for this field, it will work anyway, 
 *             but it costs some memory for {@link UninvertingReader}. 
 *             Also, numeric doc values are not supported until <a href="https://issues.apache.org/jira/browse/LUCENE-5868">LUCENE-5868</a>.
 *             Thus, it only supports {@link DocValuesType#SORTED}, {@link DocValuesType#SORTED_SET}, {@link DocValuesType#BINARY}.  </li>
 *  <li>fromIndex - optional parameter, a core name where subordinate query should run (and <code>from</code> values are collected) rather than current core.
 *             <br>Example:<code>q={!join from=manu_id_s to=id score=total fromIndex=products}foo</code> 
 *             <br>Follow up <a href="https://issues.apache.org/jira/browse/SOLR-7775">SOLR-7775</a> for SolrCloud collections support.</li>
 *  <li>to - "primary key" field name which is searched for values collected from subordinate query. 
 *             it should be declared as <code>indexed="true"</code>. Now it's treated as a single value field.</li>
 *  <li>score - one of {@link ScoreMode}: None,Avg,Total,Max. Lowercase is also accepted.</li>
 * </ul>
 */
public class ScoreJoinQParserPlugin extends QParserPlugin {

  public static final String SCORE = "score";

  static class OtherCoreJoinQuery extends SameCoreJoinQuery {
    private final String fromIndex;
    private final long fromCoreOpenTime;

    public OtherCoreJoinQuery(Query fromQuery, String fromField,
                              String fromIndex, long fromCoreOpenTime, ScoreMode scoreMode,
                              String toField) {
      super(fromQuery, fromField, toField, scoreMode);
      this.fromIndex = fromIndex;
      this.fromCoreOpenTime = fromCoreOpenTime;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();

      CoreContainer container = info.getReq().getCore().getCoreDescriptor().getCoreContainer();

      final SolrCore fromCore = container.getCore(fromIndex);

      if (fromCore == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
      }
      RefCounted<SolrIndexSearcher> fromHolder = null;
      fromHolder = fromCore.getRegisteredSearcher();
      final Query joinQuery;
      try {
        joinQuery = JoinUtil.createJoinQuery(fromField, true,
            toField, fromQuery, fromHolder.get(), scoreMode);
      } finally {
        fromCore.close();
        fromHolder.decref();
      }
      joinQuery.setBoost(getBoost());
      return joinQuery.rewrite(reader);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result
          + (int) (fromCoreOpenTime ^ (fromCoreOpenTime >>> 32));
      result = prime * result
          + ((fromIndex == null) ? 0 : fromIndex.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (getClass() != obj.getClass()) return false;
      OtherCoreJoinQuery other = (OtherCoreJoinQuery) obj;
      if (fromCoreOpenTime != other.fromCoreOpenTime) return false;
      if (fromIndex == null) {
        if (other.fromIndex != null) return false;
      } else if (!fromIndex.equals(other.fromIndex)) return false;
      return true;
    }

    @Override
    public String toString(String field) {
      return "OtherCoreJoinQuery [fromIndex=" + fromIndex
          + ", fromCoreOpenTime=" + fromCoreOpenTime + " extends "
          + super.toString(field) + "]";
    }
  }

  static class SameCoreJoinQuery extends Query {
    protected final Query fromQuery;
    protected final ScoreMode scoreMode;
    protected final String fromField;
    protected final String toField;

    SameCoreJoinQuery(Query fromQuery, String fromField, String toField,
                      ScoreMode scoreMode) {
      this.fromQuery = fromQuery;
      this.scoreMode = scoreMode;
      this.fromField = fromField;
      this.toField = toField;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      final Query jq = JoinUtil.createJoinQuery(fromField, true,
          toField, fromQuery, info.getReq().getSearcher(), scoreMode);
      jq.setBoost(getBoost());
      return jq.rewrite(reader);
    }


    @Override
    public String toString(String field) {
      return "SameCoreJoinQuery [fromQuery=" + fromQuery + ", fromField="
          + fromField + ", toField=" + toField + ", scoreMode=" + scoreMode
          + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result
          + ((fromField == null) ? 0 : fromField.hashCode());
      result = prime * result
          + ((fromQuery == null) ? 0 : fromQuery.hashCode());
      result = prime * result
          + ((scoreMode == null) ? 0 : scoreMode.hashCode());
      result = prime * result + ((toField == null) ? 0 : toField.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (getClass() != obj.getClass()) return false;
      SameCoreJoinQuery other = (SameCoreJoinQuery) obj;
      if (fromField == null) {
        if (other.fromField != null) return false;
      } else if (!fromField.equals(other.fromField)) return false;
      if (fromQuery == null) {
        if (other.fromQuery != null) return false;
      } else if (!fromQuery.equals(other.fromQuery)) return false;
      if (scoreMode != other.scoreMode) return false;
      if (toField == null) {
        if (other.toField != null) return false;
      } else if (!toField.equals(other.toField)) return false;
      return true;
    }
  }

  @Override
  public void init(NamedList args) {
  }


  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        final String fromField = localParams.get("from");
        final String fromIndex = localParams.get("fromIndex");
        final String toField = localParams.get("to");
        final ScoreMode scoreMode = ScoreModeParser.parse(getParam(SCORE));

        final String v = localParams.get(CommonParams.VALUE);

        final Query q = createQuery(fromField, v, fromIndex, toField, scoreMode,
            CommonParams.TRUE.equals(localParams.get("TESTenforceSameCoreAsAnotherOne")));

        return q;
      }

      private Query createQuery(final String fromField, final String fromQueryStr,
                                String fromIndex, final String toField, final ScoreMode scoreMode,
                                boolean byPassShortCircutCheck) throws SyntaxError {

        final String myCore = req.getCore().getCoreDescriptor().getName();

        if (fromIndex != null && (!fromIndex.equals(myCore) || byPassShortCircutCheck)) {
          CoreContainer container = req.getCore().getCoreDescriptor().getCoreContainer();

          final SolrCore fromCore = container.getCore(fromIndex);
          RefCounted<SolrIndexSearcher> fromHolder = null;

          if (fromCore == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
          }

          long fromCoreOpenTime = 0;
          LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(fromCore, params);

          try {
            QParser fromQueryParser = QParser.getParser(fromQueryStr, "lucene", otherReq);
            Query fromQuery = fromQueryParser.getQuery();

            fromHolder = fromCore.getRegisteredSearcher();
            if (fromHolder != null) {
              fromCoreOpenTime = fromHolder.get().getOpenTime();
            }
            return new OtherCoreJoinQuery(fromQuery, fromField, fromIndex, fromCoreOpenTime,
                scoreMode, toField);
          } finally {
            otherReq.close();
            fromCore.close();
            if (fromHolder != null) fromHolder.decref();
          }
        } else {
          QParser fromQueryParser = subQuery(fromQueryStr, null);
          final Query fromQuery = fromQueryParser.getQuery();
          return new SameCoreJoinQuery(fromQuery, fromField, toField, scoreMode);
        }
      }
    };
  }
}


