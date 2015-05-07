package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

/**
 * A query that matches all documents.
 *
 */
public final class MatchAllDocsQuery extends Query {

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) {
    return new RandomAccessWeight(this) {
      @Override
      protected Bits getMatchingDocs(LeafReaderContext context) throws IOException {
        return new Bits.MatchAllBits(context.reader().maxDoc());
      }
      @Override
      public String toString() {
        return "weight(" + MatchAllDocsQuery.this + ")";
      }
    };
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("*:*");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }
}
