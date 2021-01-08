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
package org.apache.lucene.queryparser.surround.query;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/** Simple single-term clause */
public class SrndTermQuery extends SimpleTerm {
  public SrndTermQuery(String termText, boolean quoted) {
    super(quoted);
    this.termText = termText;
  }

  private final String termText;

  public String getTermText() {
    return termText;
  }

  public Term getLuceneTerm(String fieldName) {
    return new Term(fieldName, getTermText());
  }

  @Override
  public String toStringUnquoted() {
    return getTermText();
  }

  @Override
  public void visitMatchingTerms(IndexReader reader, String fieldName, MatchingTermVisitor mtv)
      throws IOException {
    /* check term presence in index here for symmetry with other SimpleTerm's */
    Terms terms = MultiTerms.getTerms(reader, fieldName);
    if (terms != null) {
      TermsEnum termsEnum = terms.iterator();

      TermsEnum.SeekStatus status = termsEnum.seekCeil(new BytesRef(getTermText()));
      if (status == TermsEnum.SeekStatus.FOUND) {
        mtv.visitMatchingTerm(getLuceneTerm(fieldName));
      }
    }
  }
}
