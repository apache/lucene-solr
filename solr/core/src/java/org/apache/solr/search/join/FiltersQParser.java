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

import java.util.*;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
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

  protected FiltersQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() throws SyntaxError {
    BooleanQuery query = parseImpl();
    return !query.clauses().isEmpty() ? wrapSubordinateClause(query) : noClausesQuery();
  }

  protected BooleanQuery parseImpl() throws SyntaxError {
    Map<QParser, Occur> clauses = clauses();

    exclude(clauses.keySet());

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (Map.Entry<QParser, Occur> clause: clauses.entrySet()) {
      builder.add(unwrapQuery(clause.getKey().getQuery(), clause.getValue()), clause.getValue());
    }
    // what about empty query?
    return builder.build();
  }

  protected Query unwrapQuery(Query query, BooleanClause.Occur occur) {
    return query;
  }

  protected Query wrapSubordinateClause(Query subordinate) throws SyntaxError {
    return subordinate;
  }

  protected Query noClausesQuery() throws SyntaxError {
    return new MatchAllDocsQuery();
  }

  protected void exclude(Collection<QParser> clauses) {
    Set<String> tagsToExclude = new HashSet<>();
    String excludeTags = localParams.get("excludeTags");
    if (excludeTags != null) {
      tagsToExclude.addAll(StrUtils.splitSmart(excludeTags, ','));
    }
    @SuppressWarnings("rawtypes")
    Map tagMap = (Map) req.getContext().get("tags");
    final Collection<QParser> excludeSet;
    if (tagMap != null && !tagMap.isEmpty() && !tagsToExclude.isEmpty()) {
      excludeSet = excludeSet(tagMap, tagsToExclude);
    } else {
      excludeSet = Collections.emptySet();
    }
    clauses.removeAll(excludeSet);
  }

  protected Map<QParser,Occur> clauses() throws SyntaxError {
    String[] params = localParams.getParams(getFiltersParamName());
    if(params!=null && params.length == 0) { // never happens 
      throw new SyntaxError("Local parameter "+getFiltersParamName() + 
                           " is not defined for "+stringIncludingLocalParams);
    }
    Map<QParser,Occur> clauses = new IdentityHashMap<>();
    
    for (String filter : params==null ? new String[0] : params) {
      if(filter==null || filter.length() == 0) {
        throw new SyntaxError("Filter '"+filter + 
                             "' has been picked in "+stringIncludingLocalParams);
      }
      // as a side effect, qparser is mapped by tags in req context
      QParser parser = subQuery(filter, null);
      clauses.put(parser, BooleanClause.Occur.FILTER);
    }
    String queryText = localParams.get(QueryParsing.V);
    if (queryText != null && queryText.length() > 0) {
      QParser parser = subQuery(queryText, null);
      clauses.put(parser, BooleanClause.Occur.MUST);
    }
    return clauses;
  }

  private Collection<QParser> excludeSet(@SuppressWarnings("rawtypes")
                                     Map tagMap, Set<String> tagsToExclude) {

    IdentityHashMap<QParser,Boolean> excludeSet = new IdentityHashMap<>();
    for (String excludeTag : tagsToExclude) {
      Object olst = tagMap.get(excludeTag);
      // tagMap has entries of List<String,List<QParser>>, but subject to change in the future
      if (!(olst instanceof Collection)) continue;
      for (Object o : (Collection<?>)olst) {
        if (!(o instanceof QParser)) continue;
        QParser qp = (QParser)o;
        excludeSet.put(qp, Boolean.TRUE);
      }
    }
    return excludeSet.keySet();
  }
}
