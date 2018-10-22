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

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;

/**
 * Solr query parser that will handle parsing graph query requests.
 */
public class GraphQueryParser extends QParser {
  
  public GraphQueryParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }
  
  @Override
  public Query parse() throws SyntaxError {
    // grab query params and defaults
    SolrParams localParams = getLocalParams();

    Query rootNodeQuery = subQuery(localParams.get(QueryParsing.V), null).getQuery();
    String traversalFilterS = localParams.get("traversalFilter");
    Query traversalFilter = traversalFilterS == null ? null : subQuery(traversalFilterS, null).getQuery();

    // NOTE: the from/to are reversed from {!join}
    String fromField = localParams.get("from", "node_id");
    String toField = localParams.get("to", "edge_ids");

    validateFields(fromField);
    validateFields(toField);

    // only documents that do not have values in the edge id fields.
    boolean onlyLeafNodes = localParams.getBool("returnOnlyLeaf", false);
    // choose if you want to return documents that match the initial query or not.
    boolean returnRootNodes = localParams.getBool("returnRoot", true);
    // enable or disable the use of an automaton term for the frontier traversal.
    int maxDepth = localParams.getInt("maxDepth", -1);
    // if true, an automaton will be compiled to issue the next graph hop
    // this avoid having a large number of boolean clauses. (and it's faster too!)
    boolean useAutn = localParams.getBool("useAutn", false);

    // Construct a graph query object based on parameters passed in.
    GraphQuery gq = new GraphQuery(rootNodeQuery, fromField, toField, traversalFilter);
    // set additional parameters that are not in the constructor.
    gq.setMaxDepth(maxDepth);
    gq.setOnlyLeafNodes(onlyLeafNodes);
    gq.setReturnRoot(returnRootNodes);
    gq.setUseAutn(useAutn);
    // return the parsed graph query.
    return gq;
  }

  public void validateFields(String field) throws SyntaxError {

    if (req.getSchema().getField(field) == null) {
      throw new SyntaxError("field " + field + " not defined in schema");
    }

    if (req.getSchema().getField(field).getType().isPointField()) {
      if (req.getSchema().getField(field).hasDocValues()) {
        return;
      } else {
        throw new SyntaxError("point field " + field + " must have docValues=true");
      }
    }

    if (req.getSchema().getField(field).getType() instanceof StrField) {
      if ((req.getSchema().getField(field).hasDocValues() || req.getSchema().getField(field).indexed())) {
        return;
      } else {
        throw new SyntaxError("string field " + field + " must have indexed=true or docValues=true");
      }
    }

    throw new SyntaxError("FieldType for field=" + field + " not supported");

  }
  
}
