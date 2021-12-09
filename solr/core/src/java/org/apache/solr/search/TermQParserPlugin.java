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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;

/**
 * Create a single term query from the input value equivalent to readableToIndexed().
 * This is useful for generating filter queries from the external human readable terms returned by the
 * faceting or terms components.
 *
 * <p>
 * For text fields, no analysis is done since raw terms are already returned from the faceting
 * and terms components, and not all text analysis is idempotent.
 * To apply analysis to text fields as well, see the {@link FieldQParserPlugin}.
 * <br>
 * If no analysis or transformation is desired for any type of field, see the {@link RawQParserPlugin}.
 *
 * <p>Other parameters: <code>f</code>, the field
 * <br>Example: <code>{!term f=weight}1.5</code>
 */
public class TermQParserPlugin extends QParserPlugin {
  public static final String NAME = "term";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() {
        String fname = localParams.get(QueryParsing.F);
        if (fname == null || fname.isEmpty()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing field to query");
        }
        FieldType ft = req.getSchema().getFieldTypeNoEx(fname);
        String val = localParams.get(QueryParsing.V);
        if (ft != null) {
          return ft.getFieldTermQuery(this, req.getSchema().getField(fname), val);
        } else {
          BytesRefBuilder term = new BytesRefBuilder();
          term.copyChars(val);
          return new TermQuery(new Term(fname, term.get()));
        }
      }
    };
  }
}
