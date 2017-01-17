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
package org.apache.lucene.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;

/**
 * @deprecated Use {@link org.apache.lucene.search.TermInSetQuery}
 */
@Deprecated
public class TermsQuery extends Query implements Accountable {

  private final Query rewritten;
  private final long ramBytesUsed;

  /**
   * Creates a new {@link TermsQuery} from the given collection. It
   * can contain duplicate terms and multiple fields.
   */
  public TermsQuery(Collection<Term> terms) {
    Map<String, List<BytesRef>> termsPerField = new HashMap<>();
    for (Term term : terms) {
      List<BytesRef> t = termsPerField.computeIfAbsent(term.field(), s -> new ArrayList<>());
      t.add(term.bytes());
    }
    if (termsPerField.size() == 1) {
      Map.Entry<String, List<BytesRef>> entry = termsPerField.entrySet().iterator().next();
      TermInSetQuery tisq = new TermInSetQuery(entry.getKey(), entry.getValue());
      rewritten = tisq;
      ramBytesUsed = tisq.ramBytesUsed();
    } else {
      BooleanQuery.Builder bq = new BooleanQuery.Builder()
          .setDisableCoord(true);
      long ramBytesUsed = 0;
      for (Map.Entry<String, List<BytesRef>> entry : termsPerField.entrySet()) {
        TermInSetQuery tisq = new TermInSetQuery(entry.getKey(), entry.getValue());
        bq.add(tisq, Occur.SHOULD);
        ramBytesUsed += tisq.ramBytesUsed();
      }
      rewritten = new ConstantScoreQuery(bq.build());
      this.ramBytesUsed = ramBytesUsed;
    }
  }

  /**
   * Creates a new {@link TermsQuery} from the given collection for
   * a single field. It can contain duplicate terms.
   */
  public TermsQuery(String field, Collection<BytesRef> terms) {
    TermInSetQuery tisq = new TermInSetQuery(field, terms);
    rewritten = tisq;
    ramBytesUsed = tisq.ramBytesUsed();
  }

  /**
   * Creates a new {@link TermsQuery} from the given {@link BytesRef} array for
   * a single field.
   */
  public TermsQuery(String field, BytesRef...terms) {
    this(field, Arrays.asList(terms));
  }

  /**
   * Creates a new {@link TermsQuery} from the given array. The array can
   * contain duplicate terms and multiple fields.
   */
  public TermsQuery(final Term... terms) {
    this(Arrays.asList(terms));
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return rewritten;
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    return Objects.equals(rewritten, ((TermsQuery) obj).rewritten);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + rewritten.hashCode();
  }

  @Override
  public String toString(String field) {
    return rewritten.toString(field);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }
}
