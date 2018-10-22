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
package org.apache.solr.schema;

import java.io.IOException;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;

/**
 * Custom field that overrides the PrefixQuery behaviour to map queries such that:
 * (foo* becomes bar*) and (bar* becomes foo*).
 * This is used for testing overridden prefix query for custom fields in TestOverriddenPrefixQueryForCustomFieldType
 */
public class MyCrazyCustomField extends TextField {


  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }

  @Override
  public SortField getSortField(final SchemaField field, final boolean reverse) {
    field.checkSortability();
    return getStringSort(field, reverse);
  }

  @Override
  public Query getPrefixQuery(QParser parser, SchemaField sf, String termStr) {
    if(termStr.equals("foo")) {
      termStr = "bar";
    } else if (termStr.equals("bar")) {
      termStr = "foo";
    }

    PrefixQuery query = new PrefixQuery(new Term(sf.getName(), termStr));
    query.setRewriteMethod(sf.getType().getRewriteMethod(parser, sf));
    return query;
  }
}
