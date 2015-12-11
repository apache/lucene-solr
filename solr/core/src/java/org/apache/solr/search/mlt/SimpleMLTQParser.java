package org.apache.solr.search.mlt;
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

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LegacyNumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
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
      TopDocs td = searcher.search(docIdQuery, 1);
      if (td.totalHits != 1) throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Error completing MLT request. Could not fetch " +
          "document with id [" + uniqueValue + "]");
      ScoreDoc[] scoreDocs = td.scoreDocs;
      MoreLikeThis mlt = new MoreLikeThis(req.getSearcher().getIndexReader());
      
      if(localParams.getInt("mintf") != null)
        mlt.setMinTermFreq(localParams.getInt("mintf"));
      
      if(localParams.getInt("mindf") != null)
      mlt.setMinDocFreq(localParams.getInt("mindf"));
      
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
      
      ArrayList<String> fields = new ArrayList();

      if (qf != null) {
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
      } else {
        Map<String, SchemaField> fieldNames = req.getSearcher().getSchema().getFields();
        for (String fieldName : fieldNames.keySet()) {
          if (fieldNames.get(fieldName).indexed() && fieldNames.get(fieldName).stored())
            if (fieldNames.get(fieldName).getType().getNumericType() == null)
              fields.add(fieldName);
        }
      }
      if( fields.size() < 1 ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "MoreLikeThis requires at least one similarity field: qf" );
      }

      mlt.setFieldNames(fields.toArray(new String[fields.size()]));
      mlt.setAnalyzer(req.getSchema().getIndexAnalyzer());

      Query rawMLTQuery = mlt.like(scoreDocs[0].doc);
      BooleanQuery boostedMLTQuery = (BooleanQuery) rawMLTQuery;

      if (boostFields.size() > 0) {
        BooleanQuery.Builder newQ = new BooleanQuery.Builder();
        newQ.setDisableCoord(boostedMLTQuery.isCoordDisabled());
        newQ.setMinimumNumberShouldMatch(boostedMLTQuery.getMinimumNumberShouldMatch());

        for (BooleanClause clause : boostedMLTQuery) {
          Query q = clause.getQuery();
          Float b = boostFields.get(((TermQuery) q).getTerm().field());

          if (b != null) {
            q = new BoostQuery(q, b);
          }

          newQ.add(q, clause.getOccur());
        }

        boostedMLTQuery = newQ.build();
      }

      // exclude current document from results
      BooleanQuery.Builder realMLTQuery = new BooleanQuery.Builder();
      realMLTQuery.setDisableCoord(true);
      realMLTQuery.add(boostedMLTQuery, BooleanClause.Occur.MUST);
      realMLTQuery.add(docIdQuery, BooleanClause.Occur.MUST_NOT);

      return realMLTQuery.build();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Error completing MLT request" + e.getMessage());
    }
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
    return new Term(field, bytesRefBuilder);
  }


}
