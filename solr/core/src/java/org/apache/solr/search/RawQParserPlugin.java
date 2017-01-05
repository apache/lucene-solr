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
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Create a term query from the input value without any text analysis or transformation whatsoever.
 * This is useful in debugging, or when raw terms are returned from the terms component (this is not the default).
 *
 * <br>For easy filter construction to drill down in faceting, the {@link TermQParserPlugin} is recommended.
 * <br>For full analysis on all fields, including text fields, see the {@link FieldQParserPlugin}. 
 *
 * <br>Other parameters: <code>f</code>, the field
 * <br>Example: <code>{!raw f=myfield}Foo Bar</code> creates <code>TermQuery(Term("myfield","Foo Bar"))</code>
 */
public class RawQParserPlugin extends QParserPlugin {
  public static final String NAME = "raw";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() {
        return new TermQuery(new Term(localParams.get(QueryParsing.F), localParams.get(QueryParsing.V)));
      }
    };
  }
}
