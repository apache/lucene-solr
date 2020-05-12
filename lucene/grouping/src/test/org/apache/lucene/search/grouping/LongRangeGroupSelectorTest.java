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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

public class LongRangeGroupSelectorTest extends BaseGroupSelectorTestCase<LongRange> {

  @Override
  protected void addGroupField(Document document, int id) {
    if (rarely()) {
      return; // missing value
    }
    // numbers between 0 and 1000, groups are 100 wide from 100 to 900
    long value = random().nextInt(1000);
    document.add(new LongPoint("long", value));
    document.add(new NumericDocValuesField("long", value));
  }

  @Override
  protected GroupSelector<LongRange> getGroupSelector() {
    return new LongRangeGroupSelector(LongValuesSource.fromLongField("long"),
        new LongRangeFactory(100, 100, 900));
  }

  @Override
  protected Query filterQuery(LongRange groupValue) {
    if (groupValue == null) {
      return new BooleanQuery.Builder()
          .add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
          .add(new DocValuesFieldExistsQuery("long"), BooleanClause.Occur.MUST_NOT)
          .build();
    }
    return LongPoint.newRangeQuery("long", groupValue.min, groupValue.max - 1);
  }
}
