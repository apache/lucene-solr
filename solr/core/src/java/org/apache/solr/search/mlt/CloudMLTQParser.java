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
package org.apache.solr.search.mlt;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.index.Term;
import org.apache.lucene.legacy.LegacyNumericUtils;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.util.SolrPluginUtils;

public class CloudMLTQParser extends QParser {
  // Pattern is thread safe -- TODO? share this with general 'fl' param
  private static final Pattern splitList = Pattern.compile(",| ");

  public CloudMLTQParser(String qstr, SolrParams localParams,
                         SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  public Query parse() {
    String id = localParams.get(QueryParsing.V);
    // Do a Real Time Get for the document
    SolrDocument doc = getDocument(id);
    if(doc == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Error completing MLT request. Could not fetch " +
          "document with id [" + id + "]");
    }
    
    String[] qf = localParams.getParams("qf");
    Map<String,Float> boostFields = new HashMap<>();
    MoreLikeThis mlt = new MoreLikeThis(req.getSearcher().getIndexReader());
    
    if(localParams.getInt("mintf") != null)
      mlt.setMinTermFreq(localParams.getInt("mintf"));

    mlt.setMinDocFreq(localParams.getInt("mindf", 0));

    if(localParams.get("minwl") != null)
      mlt.setMinWordLen(localParams.getInt("minwl"));

    if(localParams.get("maxwl") != null)
      mlt.setMaxWordLen(localParams.getInt("maxwl"));

    if(localParams.get("maxqt") != null)
      mlt.setMaxQueryTerms(localParams.getInt("maxqt"));

    if(localParams.get("maxntp") != null)
      mlt.setMaxNumTokensParsed(localParams.getInt("maxntp"));
    
    if(localParams.get("maxdf") != null) {
      mlt.setMaxDocFreq(localParams.getInt("maxdf"));
    }

    if(localParams.get("boost") != null) {
      mlt.setBoost(localParams.getBool("boost"));
      boostFields = SolrPluginUtils.parseFieldBoosts(qf);
    }

    mlt.setAnalyzer(req.getSchema().getIndexAnalyzer());

    Map<String, Collection<Object>> filteredDocument = new HashMap<>();
    ArrayList<String> fieldNames = new ArrayList<>();

    if (qf != null) {
      for (String fieldName : qf) {
        if (!StringUtils.isEmpty(fieldName))  {
          String[] strings = splitList.split(fieldName);
          for (String string : strings) {
            if (!StringUtils.isEmpty(string)) {
              fieldNames.add(string);
            }
          }
        }
      }
    } else {
      for (String field : doc.getFieldNames()) {
        // Only use fields that are stored and have an explicit analyzer.
        // This makes sense as the query uses tf/idf/.. for query construction.
        // We might want to relook and change this in the future though.
        SchemaField f = req.getSchema().getFieldOrNull(field);
        if (f != null && f.stored() && f.getType().isExplicitAnalyzer()) {
          fieldNames.add(field);
        }
      }
    }

    if( fieldNames.size() < 1 ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "MoreLikeThis requires at least one similarity field: qf" );
    }

    mlt.setFieldNames(fieldNames.toArray(new String[fieldNames.size()]));
    for (String field : fieldNames) {
      filteredDocument.put(field, doc.getFieldValues(field));
    }

    try {
      Query rawMLTQuery = mlt.like(filteredDocument);
      BooleanQuery boostedMLTQuery = (BooleanQuery) rawMLTQuery;

      if (boostFields.size() > 0) {
        BooleanQuery.Builder newQ = new BooleanQuery.Builder();
        newQ.setMinimumNumberShouldMatch(boostedMLTQuery.getMinimumNumberShouldMatch());

        for (BooleanClause clause : boostedMLTQuery) {
          Query q = clause.getQuery();
          float originalBoost = 1f;
          if (q instanceof BoostQuery) {
            BoostQuery bq = (BoostQuery) q;
            q = bq.getQuery();
            originalBoost = bq.getBoost();
          }
          Float fieldBoost = boostFields.get(((TermQuery) q).getTerm().field());
          q = ((fieldBoost != null) ? new BoostQuery(q, fieldBoost * originalBoost) : clause.getQuery());
          newQ.add(q, clause.getOccur());
        }

        boostedMLTQuery = newQ.build();
      }

      // exclude current document from results
      BooleanQuery.Builder realMLTQuery = new BooleanQuery.Builder();
      realMLTQuery.add(boostedMLTQuery, BooleanClause.Occur.MUST);
      realMLTQuery.add(createIdQuery(req.getSchema().getUniqueKeyField().getName(), id), BooleanClause.Occur.MUST_NOT);

      return realMLTQuery.build();
    } catch (IOException e) {
      e.printStackTrace();
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Bad Request");
    }

  }

  private SolrDocument getDocument(String id) {
    SolrCore core = req.getCore();
    SolrQueryResponse rsp = new SolrQueryResponse();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("id", id);

    SolrQueryRequestBase request = new SolrQueryRequestBase(core, params) {
    };

    core.getRequestHandler("/get").handleRequest(request, rsp);
    NamedList response = rsp.getValues();

    return (SolrDocument) response.get("doc");
  }

  private Query createIdQuery(String defaultField, String uniqueValue) {
    return new TermQuery(req.getSchema().getField(defaultField).getType().getNumericType() != null
        ? createNumericTerm(defaultField, uniqueValue)
        : new Term(defaultField, uniqueValue));
  }

  private Term createNumericTerm(String field, String uniqueValue) {
    BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();
    bytesRefBuilder.grow(LegacyNumericUtils.BUF_SIZE_INT);
    LegacyNumericUtils.intToPrefixCoded(Integer.parseInt(uniqueValue), 0, bytesRefBuilder);
    return new Term(field, bytesRefBuilder.toBytesRef());
  }

}
