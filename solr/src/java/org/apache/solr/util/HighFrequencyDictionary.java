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

package org.apache.solr.util;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.util.StringHelper;

/**
 * HighFrequencyDictionary: terms taken from the given field
 * of a Lucene index, which appear in a number of documents
 * above a given threshold.
 *
 * When using IndexReader.terms(Term) the code must not call next() on TermEnum
 * as the first call to TermEnum, see: http://issues.apache.org/jira/browse/LUCENE-6
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

  public HighFrequencyDictionary(IndexReader reader, String field, float thresh) {
    this.reader = reader;
    this.field = StringHelper.intern(field);
    this.thresh = thresh;
  }

  public final Iterator getWordsIterator() {
    return new HighFrequencyIterator();
  }

  final class HighFrequencyIterator implements Iterator {
    private TermEnum termEnum;
    private Term actualTerm;
    private boolean hasNextCalled;
    private int minNumDocs;

    HighFrequencyIterator() {
      try {
        termEnum = reader.terms(new Term(field, ""));
        minNumDocs = (int)(thresh * (float)reader.numDocs());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private boolean isFrequent(Term term) {
      try {
        return reader.docFreq(term) >= minNumDocs;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public Object next() {
      if (!hasNextCalled) {
        hasNext();
      }
      hasNextCalled = false;

      try {
        termEnum.next();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return (actualTerm != null) ? actualTerm.text() : null;
    }

    public boolean hasNext() {
      if (hasNextCalled) {
        return actualTerm != null;
      }
      hasNextCalled = true;

      do {
        actualTerm = termEnum.term();

        // if there are no words return false
        if (actualTerm == null) {
          return false;
        }

        String currentField = actualTerm.field();

        // if the next word doesn't have the same field return false
        if (currentField != field) {   // intern'd comparison
          actualTerm = null;
          return false;
        }

        // got a valid term, does it pass the threshold?
        if (isFrequent(actualTerm)) {
          return true;
        }

        // term not up to threshold
        try {
          termEnum.next();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

      } while (true);
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
