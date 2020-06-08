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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.solr.legacy.LegacyNumericUtils;
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
import org.apache.solr.search.QueryUtils;
import org.apache.solr.util.SolrPluginUtils;

import static org.apache.solr.common.params.CommonParams.ID;

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

    mlt.setMinTermFreq(localParams.getInt("mintf", MoreLikeThis.DEFAULT_MIN_TERM_FREQ));
    mlt.setMinDocFreq(localParams.getInt("mindf", 0));
    mlt.setMinWordLen(localParams.getInt("minwl", MoreLikeThis.DEFAULT_MIN_WORD_LENGTH));
    mlt.setMaxWordLen(localParams.getInt("maxwl", MoreLikeThis.DEFAULT_MAX_WORD_LENGTH));
    mlt.setMaxQueryTerms(localParams.getInt("maxqt", MoreLikeThis.DEFAULT_MAX_QUERY_TERMS));
    mlt.setMaxNumTokensParsed(localParams.getInt("maxntp", MoreLikeThis.DEFAULT_MAX_NUM_TOKENS_PARSED));
    mlt.setMaxDocFreq(localParams.getInt("maxdf", MoreLikeThis.DEFAULT_MAX_DOC_FREQ));

    Boolean boost = localParams.getBool("boost", MoreLikeThis.DEFAULT_BOOST);
    mlt.setBoost(boost);

    mlt.setAnalyzer(req.getSchema().getIndexAnalyzer());

    Map<String, Collection<Object>> filteredDocument = new HashMap<>();
    String[] fieldNames;

    if (qf != null) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      ArrayList<String> fields = new ArrayList();
      for (String fieldName : qf) {
        if (!StringUtils.isEmpty(fieldName))  {
          String[] strings = splitList.split(fieldName);
          for (String string : strings) {
            if (!StringUtils.isEmpty(string)) {
              fields.add(string);
            }
          }
        }
      }
      // Parse field names and boosts from the fields
      boostFields = SolrPluginUtils.parseFieldBoosts(fields.toArray(new String[0]));
      fieldNames = boostFields.keySet().toArray(new String[0]);
    } else {
      @SuppressWarnings({"unchecked", "rawtypes"})
      ArrayList<String> fields = new ArrayList();
      for (String field : doc.getFieldNames()) {
        // Only use fields that are stored and have an explicit analyzer.
        // This makes sense as the query uses tf/idf/.. for query construction.
        // We might want to relook and change this in the future though.
        SchemaField f = req.getSchema().getFieldOrNull(field);
        if (f != null && f.stored() && f.getType().isExplicitAnalyzer()) {
          fields.add(field);
        }
      }
      fieldNames = fields.toArray(new String[0]);
    }

    if (fieldNames.length < 1) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "MoreLikeThis requires at least one similarity field: qf" );
    }

    mlt.setFieldNames(fieldNames);
    for (String field : fieldNames) {
      Collection<Object> fieldValues = doc.getFieldValues(field);
      if (fieldValues != null) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Collection<Object> values = new ArrayList();
        for (Object val : fieldValues) {
          if (val instanceof IndexableField) {
            values.add(((IndexableField)val).stringValue());
          }
          else {
            values.add(val);
          }
        }
        filteredDocument.put(field, values);
      }
    }

    try {
      Query rawMLTQuery = mlt.like(filteredDocument);
      BooleanQuery boostedMLTQuery = (BooleanQuery) rawMLTQuery;

      if (boost && boostFields.size() > 0) {
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

        boostedMLTQuery = QueryUtils.build(newQ, this);
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
    params.add(ID, id);

    SolrQueryRequestBase request = new SolrQueryRequestBase(core, params) {
    };

    core.getRequestHandler("/get").handleRequest(request, rsp);
    @SuppressWarnings({"rawtypes"})
    NamedList response = rsp.getValues();

    return (SolrDocument) response.get("doc");
  }

  private Query createIdQuery(String defaultField, String uniqueValue) {
    return new TermQuery(req.getSchema().getField(defaultField).getType().getNumberType() != null
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
