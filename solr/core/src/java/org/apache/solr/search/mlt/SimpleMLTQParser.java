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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class SimpleMLTQParser extends QParser {

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
        mlt.setMaxWordLen(localParams.getInt("maxqt"));

      if(localParams.get("maxntp") != null)
        mlt.setMaxWordLen(localParams.getInt("maxntp"));
      
      ArrayList<String> fields = new ArrayList();

      if (qf != null) {
        mlt.setFieldNames(qf);
      } else {

        Map<String, SchemaField> fieldNames = req.getSearcher().getSchema().getFields();
        for (String fieldName : fieldNames.keySet()) {
          if (fieldNames.get(fieldName).indexed() && fieldNames.get(fieldName).stored())
            if (fieldNames.get(fieldName).getType().getNumericType() == null)
              fields.add(fieldName);
        }
        mlt.setFieldNames(fields.toArray(new String[fields.size()]));
      }

      mlt.setAnalyzer(req.getSchema().getIndexAnalyzer());

      return mlt.like(scoreDocs[0].doc);

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
    bytesRefBuilder.grow(NumericUtils.BUF_SIZE_INT);
    NumericUtils.intToPrefixCoded(Integer.parseInt(uniqueValue), 0, bytesRefBuilder);
    return new Term(field, bytesRefBuilder.toBytesRef());
  }


}
