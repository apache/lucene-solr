/**
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
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.BytesRef;

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
  private final CharsRef spare = new CharsRef();

  public HighFrequencyDictionary(IndexReader reader, String field, float thresh) {
    this.reader = reader;
    this.field = field;
    this.thresh = thresh;
  }

  public final Iterator<String> getWordsIterator() {
    return new HighFrequencyIterator();
  }

  final class HighFrequencyIterator implements TermFreqIterator, SortedIterator {
    private TermsEnum termsEnum;
    private BytesRef actualTerm;
    private boolean hasNextCalled;
    private int minNumDocs;

    HighFrequencyIterator() {
      try {
        Terms terms = MultiFields.getTerms(reader, field);
        if (terms != null) {
          termsEnum = terms.iterator(null);
        }
        minNumDocs = (int)(thresh * (float)reader.numDocs());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private boolean isFrequent(int freq) {
      return freq >= minNumDocs;
    }
    
    public float freq() {
      try {
        return termsEnum.docFreq();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    
    public String next() {
      if (!hasNextCalled && !hasNext()) {
        return null;
      }
      hasNextCalled = false;

      return (actualTerm != null) ? actualTerm.utf8ToChars(spare).toString() : null;
    }

    public boolean hasNext() {
      if (hasNextCalled) {
        return actualTerm != null;
      }
      hasNextCalled = true;

      if (termsEnum == null) {
        return false;
      }

      while(true) {

        try {
          actualTerm = termsEnum.next();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // if there are no words return false
        if (actualTerm == null) {
          return false;
        }

        // got a valid term, does it pass the threshold?
        try {
          if (isFrequent(termsEnum.docFreq())) {
            return true;
          }
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
