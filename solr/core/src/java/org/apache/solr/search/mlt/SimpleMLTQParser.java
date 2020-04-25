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
import org.apache.lucene.index.Term;
import org.apache.solr.legacy.LegacyNumericUtils;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class SimpleMLTQParser extends QParser {
  // Pattern is thread safe -- TODO? share this with general 'fl' param
  private static final Pattern splitList = Pattern.compile(",| ");

  public SimpleMLTQParser(String qstr, SolrParams localParams,
                          SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  public Query parse() {

    String defaultField = req.getSchema().getUniqueKeyField().getName();
    String uniqueValue = localParams.get(QueryParsing.V);
    String[] qf = localParams.getParams("qf");

    SolrIndexSearcher searcher = req.getSearcher();
    Query docIdQuery = createIdQuery(defaultField, uniqueValue);
    Map<String,Float> boostFields = new HashMap<>();

    try {
      TopDocs td = searcher.search(docIdQuery, 2);
      if (td.totalHits.value != 1) throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Error completing MLT request. Could not fetch " +
          "document with id [" + uniqueValue + "]");
      ScoreDoc[] scoreDocs = td.scoreDocs;
      MoreLikeThis mlt = new MoreLikeThis(req.getSearcher().getIndexReader());
      
      mlt.setMinTermFreq(localParams.getInt("mintf", MoreLikeThis.DEFAULT_MIN_TERM_FREQ));
      mlt.setMinDocFreq(localParams.getInt("mindf", MoreLikeThis.DEFAULT_MIN_DOC_FREQ));
      mlt.setMinWordLen(localParams.getInt("minwl", MoreLikeThis.DEFAULT_MIN_WORD_LENGTH));
      mlt.setMaxWordLen(localParams.getInt("maxwl", MoreLikeThis.DEFAULT_MAX_WORD_LENGTH));
      mlt.setMaxQueryTerms(localParams.getInt("maxqt", MoreLikeThis.DEFAULT_MAX_QUERY_TERMS));
      mlt.setMaxNumTokensParsed(localParams.getInt("maxntp", MoreLikeThis.DEFAULT_MAX_NUM_TOKENS_PARSED));
      mlt.setMaxDocFreq(localParams.getInt("maxdf", MoreLikeThis.DEFAULT_MAX_DOC_FREQ));
      Boolean boost = localParams.getBool("boost", false);
      mlt.setBoost(boost);

      String[] fieldNames;
      
      if (qf != null) {
        ArrayList<String> fields = new ArrayList<>();
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
        Map<String, SchemaField> fieldDefinitions = req.getSearcher().getSchema().getFields();
        ArrayList<String> fields = new ArrayList();
        for (Map.Entry<String, SchemaField> entry : fieldDefinitions.entrySet()) {
          if (entry.getValue().indexed() && entry.getValue().stored())
            if (entry.getValue().getType().getNumberType() == null)
              fields.add(entry.getKey());
        }
        fieldNames = fields.toArray(new String[0]);
      }
      if (fieldNames.length < 1) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "MoreLikeThis requires at least one similarity field: qf" );
      }

      mlt.setFieldNames(fieldNames);
      mlt.setAnalyzer(req.getSchema().getIndexAnalyzer());

      Query rawMLTQuery = mlt.like(scoreDocs[0].doc);
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
      realMLTQuery.add(docIdQuery, BooleanClause.Occur.MUST_NOT);

      return realMLTQuery.build();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Error completing MLT request" + e.getMessage());
    }
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
    return new Term(field, bytesRefBuilder);
  }


}
