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

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Create a dismax query from the input value.
 * <br>localParams are checked before global request params.
 * <br>Example: <code>{!dismax qf='myfield mytitle^2'}foo</code> creates a dismax query across
 * across myfield and mytitle, with a higher weight on mytitle.
 *
  * <p>
  * A Generic query plugin designed to be given a simple query expression
  * from a user, which it will then query against a variety of
  * pre-configured fields, in a variety of ways, using BooleanQueries,
  * DisjunctionMaxQueries, and PhraseQueries.
  * </p>
  *
  * <p>
  * All of the following options may be configured for this plugin
  * in the solrconfig as defaults, and may be overriden as request parameters
  * </p>
  *
  * <ul>
  * <li>q.alt - An alternate query to be used in cases where the main
  *             query (q) is not specified (or blank).  This query should
  *             be expressed in the Standard SolrQueryParser syntax (you
  *             can use <code>q.alt=*:*</code> to denote that all documents
  *             should be returned when no query is specified)
  * </li>
  * <li>tie - (Tie breaker) float value to use as tiebreaker in
  *           DisjunctionMaxQueries (should be something much less than 1)
  * </li>
  * <li> qf - (Query Fields) fields and boosts to use when building
  *           DisjunctionMaxQueries from the users query.  Format is:
  *           "<code>fieldA^1.0 fieldB^2.2</code>".
  *           This param can be specified multiple times, and the fields
  *           are additive.
  * </li>
  * <li> mm - (Minimum Match) this supports a wide variety of
  *           complex expressions.
  *           read {@link org.apache.solr.util.SolrPluginUtils#setMinShouldMatch SolrPluginUtils.setMinShouldMatch} and <a href="{@docRoot}/org/apache/solr/util/doc-files/min-should-match.html">mm expression format</a> for details.
  * </li>
  * <li> pf - (Phrase Fields) fields/boosts to make phrase queries out
  *           of, to boost the users query for exact matches on the specified fields.
  *           Format is: "<code>fieldA^1.0 fieldB^2.2</code>".
  *           This param can be specified multiple times, and the fields
  *           are additive.
  * </li>
  * <li> ps - (Phrase Slop) amount of slop on phrase queries built for pf
  *           fields.
  * </li>
  * <li> qs - (Query Slop) amount of slop on phrase queries explicitly
  *           specified in the "q" for qf fields.
  * </li>
  * <li> bq - (Boost Query) a raw lucene query that will be included in the
  *           users query to influence the score.  If this is a BooleanQuery
  *           with a default boost (1.0f), then the individual clauses will be
  *           added directly to the main query.  Otherwise, the query will be
  *           included as is.
  *           This param can be specified multiple times, and the boosts are
  *           are additive.  NOTE: the behaviour listed above is only in effect
  *           if a single <code>bq</code> paramter is specified.  Hence you can
  *           disable it by specifying an additional, blank, <code>bq</code>
  *           parameter.
  * </li>
  * <li> bf - (Boost Functions) functions (with optional boosts) that will be
  *           included in the users query to influence the score.
  *           Format is: "<code>funcA(arg1,arg2)^1.2
  *           funcB(arg3,arg4)^2.2</code>".  NOTE: Whitespace is not allowed
  *           in the function arguments.
  *           This param can be specified multiple times, and the functions
  *           are additive.
  * </li>
  * <li> fq - (Filter Query) a raw lucene query that can be used
  *           to restrict the super set of products we are interested in - more
  *           efficient then using bq, but doesn't influence score.
  *           This param can be specified multiple times, and the filters
  *           are additive.
  * </li>
  * </ul>
  *
  * <p>
  * The following options are only available as request params...
  * </p>
  *
  * <ul>
  * <li>   q - (Query) the raw unparsed, unescaped, query from the user.
  * </li>
  * <li>sort - (Order By) list of fields and direction to sort on.
  * </li>
  * </ul>
 *
 */
public class DisMaxQParserPlugin extends QParserPlugin {
  public static final String NAME = "dismax";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new DisMaxQParser(qstr, localParams, params, req);
  }
}
