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

package org.apache.lucene.search.uhighlight;

import java.io.IOException;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * Overlays a 2nd LeafReader for the terms of one field, otherwise the primary reader is
 * consulted.  The 2nd reader is assumed to have one document of 0 and we remap it to a target doc ID.
 *
 * @lucene.internal
 */
public class OverlaySingleDocTermsLeafReader extends FilterLeafReader {

  private final LeafReader in2;
  private final String in2Field;
  private final int in2TargetDocId;

  public OverlaySingleDocTermsLeafReader(LeafReader in, LeafReader in2, String in2Field, int in2TargetDocId) {
    super(in);
    this.in2 = in2;
    this.in2Field = in2Field;
    this.in2TargetDocId = in2TargetDocId;
    assert in2.maxDoc() == 1;
  }

  @Override
  public Terms terms(String field) throws IOException {
    if (!in2Field.equals(field)) {
      return in.terms(field);
    }

    // Shifts leafReader in2 with only doc ID 0 to a target doc ID
    final Terms terms = in2.terms(field);
    if (terms == null) {
      return null;
    }
    if (in2TargetDocId == 0) { // no doc ID remapping to do
      return terms;
    }
    return new FilterTerms(terms) {
      @Override
      public TermsEnum iterator() throws IOException {
        return filterTermsEnum(super.iterator());
      }

      @Override
      public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
        return filterTermsEnum(super.intersect(compiled, startTerm));
      }

      private TermsEnum filterTermsEnum(TermsEnum termsEnum) throws IOException {
        return new FilterTermsEnum(termsEnum) {
          @Override
          public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            //TODO 'reuse' will always fail to reuse unless we unwrap it
            return new FilterPostingsEnum(super.postings(reuse, flags)) {
              @Override
              public int nextDoc() throws IOException {
                final int doc = super.nextDoc();
                return doc == 0 ? in2TargetDocId : doc;
              }

              @Override
              public int advance(int target) throws IOException {
                return slowAdvance(target);
              }

              @Override
              public int docID() {
                final int doc = super.docID();
                return doc == 0 ? in2TargetDocId : doc;
              }
            };
          }
        };
      }
    };
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return null;
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return null;
  }
}
