package org.apache.lucene.search.spans;

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
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Set;

/** Matches spans containing a term. */
public class SpanTermQuery extends SpanQuery {
  protected Term term;

  /** Construct a SpanTermQuery matching the named term's spans. */
  public SpanTermQuery(Term term) { this.term = term; }

  /** Return the term whose spans are matched. */
  public Term getTerm() { return term; }

  @Override
  public String getField() { return term.field(); }
  
  @Override
  public void extractTerms(Set<Term> terms) {
    terms.add(term);
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (term.field().equals(field))
      buffer.append(term.text());
    else
      buffer.append(term.toString());
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    SpanTermQuery other = (SpanTermQuery) obj;
    if (term == null) {
      if (other.term != null)
        return false;
    } else if (!term.equals(other.term))
      return false;
    return true;
  }

  @Override
  public Spans getSpans(final AtomicReaderContext context) throws IOException {
    final IndexReader reader = context.reader;
    final DocsAndPositionsEnum postings = reader.termPositionsEnum(reader.getLiveDocs(),
                                                                   term.field(),
                                                                   term.bytes());

    if (postings != null) {
      return new TermSpans(postings, term);
    } else {
      if (reader.termDocsEnum(reader.getLiveDocs(), term.field(), term.bytes()) != null) {
        // term does exist, but has no positions
        throw new IllegalStateException("field \"" + term.field() + "\" was indexed without position data; cannot run SpanTermQuery (term=" + term.text() + ")");
      } else {
        // term does not exist
        return TermSpans.EMPTY_TERM_SPANS;
      }
    }
  }
}
