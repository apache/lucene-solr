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

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;

/**
 * Create a prefix query from the input value.  Currently no analysis or
 * value transformation is done to create this prefix query (subject to change).
 * <br>Other parameters: <code>f</code>, the field
 * <br>Example: <code>{!prefix f=myfield}foo</code> is generally equivalent
 * to the Lucene query parser expression <code>myfield:foo*</code>
 */
public class PrefixQParserPlugin extends QParserPlugin {
  public static final String NAME = "prefix";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() {
        SchemaField sf = req.getSchema().getField(localParams.get(QueryParsing.F));
        return sf.getType().getPrefixQuery(this, sf, localParams.get(QueryParsing.V));
      }
    };
  }
}

