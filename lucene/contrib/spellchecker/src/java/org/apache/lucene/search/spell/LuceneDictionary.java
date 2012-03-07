package org.apache.lucene.search.spell;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.StringHelper;

import java.io.*;
import java.util.Comparator;

/**
 * Lucene Dictionary: terms taken from the given field
 * of a Lucene index.
 *
 * When using IndexReader.terms(Term) the code must not call next() on TermEnum
 * as the first call to TermEnum, see: http://issues.apache.org/jira/browse/LUCENE-6
 *
 *
 *
 */
public class LuceneDictionary implements Dictionary {
  private IndexReader reader;
  private String field;

  public LuceneDictionary(IndexReader reader, String field) {
    this.reader = reader;
    this.field = StringHelper.intern(field);
  }

  public final BytesRefIterator getWordsIterator() throws IOException {
    return new TermIterator();
  }
  

  final class TermIterator implements TermFreqIterator {
    private final BytesRef spare = new BytesRef();
    private final TermEnum termsEnum;
    private long freq;
    private final Comparator<BytesRef> comp;

    TermIterator() throws IOException {
      termsEnum = reader.terms(new Term(field, ""));
      Term term = termsEnum.term();
      if (term == null || term.field() != field) {
        comp = null;
      } else {
        comp = BytesRef.getUTF8SortedAsUnicodeComparator();
      }
    }

    public long weight() {
      return freq;
    }

    //@Override - not until Java 6
    public BytesRef next() throws IOException {
      if (termsEnum != null) {
        Term actualTerm;
        do {
          actualTerm = termsEnum.term();
          if (actualTerm == null || actualTerm.field() != field) {
            return null;
          }
          freq = termsEnum.docFreq();
          spare.copyChars(actualTerm.text());
          termsEnum.next();
          return spare;
        } while(termsEnum.next());

      }
      return null;
    }

    //@Override - not until Java 6
    public Comparator<BytesRef> getComparator() {
      return comp;
    }
  }
  
}
