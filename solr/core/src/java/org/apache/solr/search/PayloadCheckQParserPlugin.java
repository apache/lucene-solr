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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.payloads.FloatEncoder;
import org.apache.lucene.analysis.payloads.IdentityEncoder;
import org.apache.lucene.analysis.payloads.IntegerEncoder;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.apache.lucene.queries.payloads.SpanPayloadCheckQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.util.PayloadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PayloadCheckQParserPlugin extends QParserPlugin {
  public static final String NAME = "payload_check";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {

    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String field = localParams.get(QueryParsing.F);
        String value = localParams.get(QueryParsing.V);
        String p = localParams.get("payloads");

        if (field == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'f' not specified");
        }

        if (value == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "query string missing");
        }

        if (p == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'payloads' not specified");
        }

        FieldType ft = req.getCore().getLatestSchema().getFieldType(field);
        Analyzer analyzer = ft.getQueryAnalyzer();
        SpanQuery query = null;
        try {
          query = PayloadUtils.createSpanQuery(field, value, analyzer);
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
        }

        if (query == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "SpanQuery is null");
        }

        PayloadEncoder encoder = null;
        String e = PayloadUtils.getPayloadEncoder(ft);
        if ("float".equals(e)) {    // TODO: centralize this string->PayloadEncoder logic (see DelimitedPayloadTokenFilterFactory)
          encoder = new FloatEncoder();
        } else if ("integer".equals(e)) {
          encoder = new IntegerEncoder();
        } else if ("identity".equals(e)) {
          encoder = new IdentityEncoder();
        }

        if (encoder == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "invalid encoder: " + e + " for field: " + field);
        }

        List<BytesRef> payloads = new ArrayList<>();
        String[] rawPayloads = p.split(" ");  // since payloads (most likely) came in whitespace delimited, just split
        for (String rawPayload : rawPayloads) {
          if (rawPayload.length() > 0)
            payloads.add(encoder.encode(rawPayload.toCharArray()));
        }

        return new SpanPayloadCheckQuery(query, payloads);
      }
    };


  }
}
