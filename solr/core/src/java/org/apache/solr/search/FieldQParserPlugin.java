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
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;


/**
 * Create a field query from the input value, applying text analysis and constructing a phrase query if appropriate.
 * <br>Other parameters: <code>f</code>, the field
 * <br>Example: <code>{!field f=myfield}Foo Bar</code> creates a phrase query with "foo" followed by "bar"
 * if the analyzer for myfield is a text field with an analyzer that splits on whitespace and lowercases terms.
 * This is generally equivalent to the Lucene query parser expression <code>myfield:"Foo Bar"</code>
 */
public class FieldQParserPlugin extends QParserPlugin {
  public static final String NAME = "field";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() {
        String field = localParams.get(QueryParsing.F);
        String queryText = localParams.get(QueryParsing.V);
        SchemaField sf = req.getSchema().getField(field);
        FieldType ft = sf.getType();
        return ft.getFieldQuery(this, sf, queryText);
      }
    };
  }
}
