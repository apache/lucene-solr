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

package org.apache.lucene.luwak;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * Class for recording terms stored in the query index.
 * <p>
 * An instance of QueryTermFilter is passed to {@link Presearcher#buildQuery(LeafReader, QueryTermFilter)},
 * and can be used to restrict the presearcher's disjunction query to terms in the index.
 */
public class QueryTermFilter {

  private final Map<String, BytesRefHash> termsHash = new HashMap<>();

  /**
   * Create a QueryTermFilter for an IndexReader
   *
   * @param reader the {@link IndexReader}
   * @throws IOException on error
   */
  public QueryTermFilter(IndexReader reader) throws IOException {
    for (LeafReaderContext ctx : reader.leaves()) {
      for (FieldInfo fi : ctx.reader().getFieldInfos()) {
        BytesRefHash terms = termsHash.computeIfAbsent(fi.name, f -> new BytesRefHash());
        Terms t = ctx.reader().terms(fi.name);
        if (t != null) {
          TermsEnum te = t.iterator();
          BytesRef term;
          while ((term = te.next()) != null) {
            terms.add(term);
          }
        }
      }
    }
  }

  /**
   * Get a BytesRefHash containing all terms for a particular field
   *
   * @param field the field
   * @return a {@link BytesRefHash} containing all terms for the specified field
   */
  public BytesRefHash getTerms(String field) {
    BytesRefHash existing = termsHash.get(field);
    if (existing != null)
      return existing;

    return new BytesRefHash();
  }
}
