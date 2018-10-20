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
package org.apache.lucene.search.spell;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * HighFrequencyDictionary: terms taken from the given field
 * of a Lucene index, which appear in a number of documents
 * above a given threshold.
 *
 * Threshold is a value in [0..1] representing the minimum
 * number of documents (of the total) where a term should appear.
 * 
 * Based on LuceneDictionary.
 */
public class HighFrequencyDictionary implements Dictionary {
  private IndexReader reader;
  private String field;
  private float thresh;

  /**
   * Creates a new Dictionary, pulling source terms from
   * the specified <code>field</code> in the provided <code>reader</code>.
   * <p>
   * Terms appearing in less than <code>thresh</code> percentage of documents
   * will be excluded.
   */
  public HighFrequencyDictionary(IndexReader reader, String field, float thresh) {
    this.reader = reader;
    this.field = field;
    this.thresh = thresh;
  }

  @Override
  public final InputIterator getEntryIterator() throws IOException {
    return new HighFrequencyIterator();
  }

  final class HighFrequencyIterator implements InputIterator {
    private final BytesRefBuilder spare = new BytesRefBuilder();
    private final TermsEnum termsEnum;
    private int minNumDocs;
    private long freq;

    HighFrequencyIterator() throws IOException {
      Terms terms = MultiTerms.getTerms(reader, field);
      if (terms != null) {
        termsEnum = terms.iterator();
      } else {
        termsEnum = null;
      }
      minNumDocs = (int)(thresh * (float)reader.numDocs());
    }

    private boolean isFrequent(int freq) {
      return freq >= minNumDocs;
    }
    
    @Override
    public long weight() {
      return freq;
    }

    @Override
    public BytesRef next() throws IOException {
      if (termsEnum != null) {
        BytesRef next;
        while((next = termsEnum.next()) != null) {
          if (isFrequent(termsEnum.docFreq())) {
            freq = termsEnum.docFreq();
            spare.copyBytes(next);
            return spare.get();
          }
        }
      }
      return  null;
    }

    @Override
    public BytesRef payload() {
      return null;
    }

    @Override
    public boolean hasPayloads() {
      return false;
    }

    @Override
    public Set<BytesRef> contexts() {
      return null;
    }

    @Override
    public boolean hasContexts() {
      return false;
    }
  }
}
