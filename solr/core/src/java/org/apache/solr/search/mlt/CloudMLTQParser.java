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

import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CloudMLTQParser extends QParser {

  public CloudMLTQParser(String qstr, SolrParams localParams,
                         SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  public Query parse() {
    String id = localParams.get(QueryParsing.V);
    // Do a Real Time Get for the document
    SolrDocument doc = getDocument(id);
    
    MoreLikeThis mlt = new MoreLikeThis(req.getSearcher().getIndexReader());
    // TODO: Are the mintf and mindf defaults ok at 1/0 ?
    
    mlt.setMinTermFreq(localParams.getInt("mintf", 1));
    mlt.setMinDocFreq(localParams.getInt("mindf", 0));
    if(localParams.get("minwl") != null)
      mlt.setMinWordLen(localParams.getInt("minwl"));
    
    if(localParams.get("maxwl") != null)
      mlt.setMaxWordLen(localParams.getInt("maxwl"));

    mlt.setAnalyzer(req.getSchema().getIndexAnalyzer());

    String[] qf = localParams.getParams("qf");
    Map<String, ArrayList<String>> filteredDocument = new HashMap();

    if (qf != null) {
      mlt.setFieldNames(qf);
      for (String field : qf) {
        filteredDocument.put(field, (ArrayList<String>) doc.get(field));
      }
    } else {
      for (String field : doc.getFieldNames()) {
        filteredDocument.put(field, (ArrayList<String>) doc.get(field));
      }
    }

    try {
      return mlt.like(filteredDocument);
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

}
