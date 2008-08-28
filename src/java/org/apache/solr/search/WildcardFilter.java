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

package org.apache.solr.search;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.WildcardTermEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.util.OpenBitSet;

import java.util.BitSet;
import java.io.IOException;


/**
 *
 * @version $Id$
 */
public class WildcardFilter extends Filter {
  protected final Term term;

  public WildcardFilter(Term wildcardTerm) {
    this.term = wildcardTerm;
  }

  public Term getTerm() { return term; }

  /**
   * @deprecated Use {@link #getDocIdSet(IndexReader)} instead.
   */
  public BitSet bits(IndexReader reader) throws IOException {
    final BitSet bitSet = new BitSet(reader.maxDoc());
    new WildcardGenerator(term) {
      public void handleDoc(int doc) {
        bitSet.set(doc);
      }
    }.generate(reader);
    return bitSet;
  }

  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    final OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
    new WildcardGenerator(term) {
      public void handleDoc(int doc) {
        bitSet.set(doc);
      }
    }.generate(reader);
    return bitSet;
  }

  public String toString () {
    StringBuilder sb = new StringBuilder();
    sb.append("WildcardFilter(");
    sb.append(term.toString());
    sb.append(")");
    return sb.toString();
  }
}


abstract class WildcardGenerator implements IdGenerator {
  protected final Term wildcard;

  WildcardGenerator(Term wildcard) {
    this.wildcard = wildcard;
  }

  public void generate(IndexReader reader) throws IOException {
    TermEnum enumerator = new WildcardTermEnum(reader, wildcard);
    TermDocs termDocs = reader.termDocs();
    try {
      do {
        Term term = enumerator.term();
        if (term==null) break;
        termDocs.seek(term);
        while (termDocs.next()) {
          handleDoc(termDocs.doc());
        }
      } while (enumerator.next());
    } finally {
      termDocs.close();
      enumerator.close();
    }
  }
}
