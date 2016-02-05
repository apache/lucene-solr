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
package org.apache.solr.search.facet;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;

/** @lucene.internal
 * Porting helper... may be removed if it offers no value in the future.
 */
public class FieldUtil {

  /** Simpler method that creates a request context and looks up the field for you */
  public static SortedDocValues getSortedDocValues(SolrIndexSearcher searcher, String field) throws IOException {
    SchemaField sf = searcher.getSchema().getField(field);
    QueryContext qContext = QueryContext.newContext(searcher);
    return getSortedDocValues( qContext, sf, null );
  }


  public static SortedDocValues getSortedDocValues(QueryContext context, SchemaField field, QParser qparser) throws IOException {
    SortedDocValues si = context.searcher().getLeafReader().getSortedDocValues( field.getName() );
    // if (!field.hasDocValues() && (field.getType() instanceof StrField || field.getType() instanceof TextField)) {
    // }

    return si == null ? DocValues.emptySorted() : si;
  }

  public static SortedSetDocValues getSortedSetDocValues(QueryContext context, SchemaField field, QParser qparser) throws IOException {
    SortedSetDocValues si = context.searcher().getLeafReader().getSortedSetDocValues(field.getName());
    return si == null ? DocValues.emptySortedSet() : si;
  }

}
