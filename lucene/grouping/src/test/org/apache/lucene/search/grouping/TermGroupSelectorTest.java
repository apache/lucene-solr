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

package org.apache.lucene.search.grouping;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

public class TermGroupSelectorTest extends BaseGroupSelectorTestCase<BytesRef> {

  @Override
  protected void addGroupField(Document document, int id) {
    if (rarely()) {
      return;   // missing value
    }
    String groupValue = "group" + random().nextInt(10);
    document.add(new SortedDocValuesField("groupField", new BytesRef(groupValue)));
    document.add(new TextField("groupField", groupValue, Field.Store.NO));
  }

  @Override
  protected GroupSelector<BytesRef> getGroupSelector() {
    return new TermGroupSelector("groupField");
  }

  @Override
  protected Query filterQuery(BytesRef groupValue) {
    if (groupValue == null) {
      return new BooleanQuery.Builder()
          .add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
          .add(new DocValuesFieldExistsQuery("groupField"), BooleanClause.Occur.MUST_NOT)
          .build();
    }
    return new TermQuery(new Term("groupField", groupValue));
  }
}
