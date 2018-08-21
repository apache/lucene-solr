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

import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;

public class FiltersQParser extends QParser {

  protected String getFiltersParamName() {
    return "param";
  }

  FiltersQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public final Query parse() throws SyntaxError {
    Map<Query,Occur> clauses = clauses();
    
    exclude(clauses);
    
    int numClauses = 0;
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    numClauses += addQuery(builder, clauses);
    numClauses += addFilters(builder, clauses);
    // what about empty query? 
    return numClauses > 0 ? wrapSubordinateClause(builder.build()) : noClausesQuery();
  }

  protected Query wrapSubordinateClause(Query subordinate) throws SyntaxError {
    return subordinate;
  }

  protected Query noClausesQuery() throws SyntaxError {
    return new MatchAllDocsQuery();
  }

  protected int addQuery(BooleanQuery.Builder builder, Map<Query,Occur> clauses) {
    int cnt=0;
    for (Map.Entry<Query, Occur> clause: clauses.entrySet()) {
      if (clause.getValue() == Occur.MUST) {
        builder.add(clause.getKey(), clause.getValue());
        cnt++;// shouldn't count more than once 
      }
    }
    return cnt;
  }

  /** @return number of added clauses */
  protected int addFilters(BooleanQuery.Builder builder, Map<Query,Occur> clauses) throws SyntaxError {
    int count=0;
    for (Map.Entry<Query, Occur> clause: clauses.entrySet()) {
      if (clause.getValue() == Occur.FILTER) {
        builder.add( clause.getKey(), Occur.FILTER);
        count++;
      }
    }
    return count;
  }

  protected void exclude(Map<Query,Occur> clauses) {
    Set<String> tagsToExclude = new HashSet<>();
    String excludeTags = localParams.get("excludeTags");
    if (excludeTags != null) {
      tagsToExclude.addAll(StrUtils.splitSmart(excludeTags, ','));
    }
    @SuppressWarnings("rawtypes")
    Map tagMap = (Map) req.getContext().get("tags");
    if (tagMap != null && !tagMap.isEmpty() && !tagsToExclude.isEmpty()) {
      clauses.keySet().removeAll(excludeSet(tagMap, tagsToExclude));
    } // else no filters were tagged
  }

  protected Map<Query,Occur> clauses() throws SyntaxError {
    String[] params = localParams.getParams(getFiltersParamName());
    if(params!=null && params.length == 0) { // never happens 
      throw new SyntaxError("Local parameter "+getFiltersParamName() + 
                           " is not defined for "+stringIncludingLocalParams);
    }
    Map<Query,Occur> clauses = new IdentityHashMap<>();
    
    for (String filter : params==null ? new String[0] : params) {
      if(filter==null || filter.length() == 0) {
        throw new SyntaxError("Filter '"+filter + 
                             "' has been picked in "+stringIncludingLocalParams);
      }
      // as a side effect, qparser is mapped by tags in req context
      QParser parser = subQuery(filter, null);
      Query query = parser.getQuery();
      clauses.put(query, BooleanClause.Occur.FILTER);
    }
    String queryText = localParams.get(QueryParsing.V);
    if (queryText != null && queryText.length() > 0) {
      QParser parser = subQuery(queryText, null);
      clauses.put(parser.getQuery(), BooleanClause.Occur.MUST);
    }
    return clauses;
  }

  private Collection<?> excludeSet(@SuppressWarnings("rawtypes") 
                                     Map tagMap, Set<String> tagsToExclude) {

    IdentityHashMap<Query,Boolean> excludeSet = new IdentityHashMap<>();
    for (String excludeTag : tagsToExclude) {
      Object olst = tagMap.get(excludeTag);
      // tagMap has entries of List<String,List<QParser>>, but subject to change in the future
      if (!(olst instanceof Collection)) continue;
      for (Object o : (Collection<?>)olst) {
        if (!(o instanceof QParser)) continue;
        QParser qp = (QParser)o;
        try {
          excludeSet.put(qp.getQuery(), Boolean.TRUE);
        } catch (SyntaxError syntaxError) {
          // This should not happen since we should only be retrieving a previously parsed query
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
        }
      }
    }
    return excludeSet.keySet();
  }
}
