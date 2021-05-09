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
package org.apache.solr.response.transform;

import java.io.IOException;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.join.BlockJoinParentQParser;

import static org.apache.solr.schema.IndexSchema.NEST_PATH_FIELD_NAME;

/**
 * Attaches all descendants (child documents) to each parent document.
 *
 * Optionally you can provide a "parentFilter" param to designate which documents are the root
 * documents (parent-most documents).  Solr can figure this out on its own but you might want to
 * specify it.
 *
 * Optionally you can provide a "childFilter" param to filter out which child documents should be returned and a
 * "limit" param which provides an option to specify the number of child documents
 * to be returned per parent document. By default it's set to 10.
 *
 * Examples -
 * [child parentFilter="fieldName:fieldValue"]
 * [child parentFilter="fieldName:fieldValue" childFilter="fieldName:fieldValue"]
 * [child parentFilter="fieldName:fieldValue" childFilter="fieldName:fieldValue" limit=20]
 *
 * @since solr 4.9
 */
public class ChildDocTransformerFactory extends TransformerFactory {

  static final char PATH_SEP_CHAR = '/';
  static final char NUM_SEP_CHAR = '#';
  private static final ThreadLocal<Boolean> recursionCheckThreadLocal = ThreadLocal.withInitial(() -> Boolean.FALSE);
  private static final BooleanQuery rootFilter = new BooleanQuery.Builder()
      .add(new BooleanClause(new MatchAllDocsQuery(), BooleanClause.Occur.MUST))
      .add(new BooleanClause(new DocValuesFieldExistsQuery(NEST_PATH_FIELD_NAME), BooleanClause.Occur.MUST_NOT)).build();
  public static final String CACHE_NAME="perSegFilter";

  @Override
  public DocTransformer create(String field, SolrParams params, SolrQueryRequest req) {
    if(recursionCheckThreadLocal.get()) {
      // this is a recursive call by SolrReturnFields, see ChildDocTransformerFactory#createChildDocTransformer
      return new DocTransformer.NoopFieldTransformer();
    } else {
      try {
        // transformer is yet to be initialized in this thread, create it
        recursionCheckThreadLocal.set(true);
        return createChildDocTransformer(field, params, req);
      } finally {
        recursionCheckThreadLocal.set(false);
      }
    }
  }

  private DocTransformer createChildDocTransformer(String field, SolrParams params, SolrQueryRequest req) {
    SchemaField uniqueKeyField = req.getSchema().getUniqueKeyField();
    if (uniqueKeyField == null) {
      throw new SolrException( ErrorCode.BAD_REQUEST,
          " ChildDocTransformer requires the schema to have a uniqueKeyField." );
    }
    // Do we build a hierarchy or flat list of child docs (attached anonymously)?
    boolean buildHierarchy = req.getSchema().hasExplicitField(NEST_PATH_FIELD_NAME);

    String parentFilterStr = params.get( "parentFilter" );
    BitSetProducer parentsFilter;
    // TODO reuse org.apache.solr.search.join.BlockJoinParentQParser.getCachedFilter (uses a cache)
    // TODO shouldn't we try to use the Solr filter cache, and then ideally implement
    //  BitSetProducer over that?
    // DocSet parentDocSet = req.getSearcher().getDocSet(parentFilterQuery);
    // then return BitSetProducer with custom BitSet impl accessing the docSet
    if (parentFilterStr == null) {
      parentsFilter = !buildHierarchy ? null : getCachedBitSetProducer(req, rootFilter);
    } else {
      if(buildHierarchy) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Parent filter should not be sent when the schema is nested");
      }
      Query query = parseQuery(parentFilterStr, req, "parentFilter");
      if (query == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid Parent filter '" + parentFilterStr + "', resolves to null");
      }
      parentsFilter = getCachedBitSetProducer(req, query);
    }

    String childFilterStr = params.get( "childFilter" );
    DocSet childDocSet;
    if (childFilterStr == null) {
      childDocSet = null;
    } else {
      if (buildHierarchy) {
        childFilterStr = processPathHierarchyQueryString(childFilterStr);
      }
      Query childFilter = parseQuery(childFilterStr, req, "childFilter");
      try {
        childDocSet = req.getSearcher().getDocSet(childFilter);
      } catch (IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    String childReturnFields = params.get("fl");
    SolrReturnFields childSolrReturnFields;
    if (childReturnFields != null) {
      childSolrReturnFields = new SolrReturnFields(childReturnFields, req);
    } else {
      childSolrReturnFields = new SolrReturnFields(req);
    }

    int limit = params.getInt( "limit", 10 );

    return new ChildDocTransformer(field, parentsFilter, childDocSet, childSolrReturnFields,
        buildHierarchy, limit, req.getSchema().getUniqueKeyField().getName());
  }

  private static Query parseQuery(String qstr, SolrQueryRequest req, String param) {
    try {
      return QParser.getParser(qstr, req).getQuery();
    } catch (SyntaxError syntaxError) {
      throw new SolrException(
              ErrorCode.BAD_REQUEST,
              "Failed to parse '" + param + "' param: " + syntaxError.getMessage(),
              syntaxError);
    }
  }

  protected static String processPathHierarchyQueryString(String queryString) {
    // if the filter includes a path string, build a lucene query string to match those specific child documents.
    // e.g. /toppings/ingredients/name_s:cocoa -> +_nest_path_:/toppings/ingredients +(name_s:cocoa)
    // ingredients/name_s:cocoa -> +_nest_path_:*/ingredients +(name_s:cocoa)
    int indexOfFirstColon = queryString.indexOf(':');
    if (indexOfFirstColon <= 0) {
      return queryString; // regular filter, not hierarchy based.
    }
    int indexOfLastPathSepChar = queryString.lastIndexOf(PATH_SEP_CHAR, indexOfFirstColon);
    if (indexOfLastPathSepChar < 0) {
      return queryString; // regular filter, not hierarchy based.
    }
    final boolean isAbsolutePath = queryString.charAt(0) == PATH_SEP_CHAR;
    String path = ClientUtils.escapeQueryChars(queryString.substring(0, indexOfLastPathSepChar));
    String remaining = queryString.substring(indexOfLastPathSepChar + 1); // last part of path hierarchy

    return
        "+" + NEST_PATH_FIELD_NAME + (isAbsolutePath? ":": ":*\\/") + path
        + " +(" + remaining + ")";
  }

  private static BitSetProducer getCachedBitSetProducer(final SolrQueryRequest request, Query query) {
    return BlockJoinParentQParser.getCachedFilter(request, query).getFilter();
  }
}

