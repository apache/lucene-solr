package org.apache.lucene.search.payloads;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.spans.TermSpans;

/**
 * Copyright 2004 The Apache Software Foundation
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The BoostingTermQuery is very similar to the {@link org.apache.lucene.search.spans.SpanTermQuery} except
 * that it factors in the value of the payload located at each of the positions where the
 * {@link org.apache.lucene.index.Term} occurs.
 * <p>
 * In order to take advantage of this, you must override {@link org.apache.lucene.search.Similarity#scorePayload(String, byte[],int,int)}
 * which returns 1 by default.
 * <p>
 * Payload scores are averaged across term occurrences in the document.  
 * 
 * @see org.apache.lucene.search.Similarity#scorePayload(String, byte[], int, int)
 *
 * @deprecated See {@link org.apache.lucene.search.payloads.PayloadTermQuery}
 */
public class BoostingTermQuery extends PayloadTermQuery {

  public BoostingTermQuery(Term term) {
    this(term, true);
  }

  public BoostingTermQuery(Term term, boolean includeSpanScore) {
    super(term, new AveragePayloadFunction(), includeSpanScore);
  }

  public Weight createWeight(Searcher searcher) throws IOException {
    return new BoostingTermWeight(this, searcher);
  }

  protected class BoostingTermWeight extends PayloadTermWeight {

    public BoostingTermWeight(BoostingTermQuery query, Searcher searcher) throws IOException {
      super(query, searcher);
    }

    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
      return new PayloadTermSpanScorer((TermSpans) query.getSpans(reader), this,
          similarity, reader.norms(query.getField()));
    }

  }


  public boolean equals(Object o) {
    if (!(o instanceof BoostingTermQuery))
      return false;
    BoostingTermQuery other = (BoostingTermQuery) o;
    return (this.getBoost() == other.getBoost())
            && this.term.equals(other.term);
  }
}
