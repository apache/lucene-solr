package org.apache.lucene.search.spans;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionIntervalFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.TermContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class MockSpanQuery extends SpanQuery {

  private Query other;
  private boolean needsPayloads;
  private String field;
  private PositionIntervalFilter filter;

  public MockSpanQuery(Query other, boolean needsPayloads, String field, PositionIntervalFilter filter) {
    this.other = other;
    this.needsPayloads = needsPayloads;
    this.field = field;
    this.filter = filter;
  }
  
  public MockSpanQuery(SpanQuery other, boolean needsPayloads) {
    this(other, needsPayloads, other.getField(), null);
  }
  
  @Override
  public Spans getSpans(AtomicReaderContext context,
      Bits acceptDocs, Map<Term,TermContext> termContexts) throws IOException {
    
    if(other instanceof SpanQuery) {
      return ((SpanQuery) other).getSpans(context, acceptDocs, termContexts);
    }
    
    AtomicReaderContext topReaderContext = context.reader().getTopReaderContext();

    Weight weight = other.createWeight(new IndexSearcher(topReaderContext));
    Scorer scorer = weight.scorer((AtomicReaderContext) topReaderContext, true, false, acceptDocs);
    if (scorer == null) {
      return EMPTY_SPANS;
    }
    // nocommit - what about offsets here?
    return new SpansScorerWrapper(scorer, filter == null ? scorer.positions(needsPayloads, false)
                                                         : filter.filter(scorer.positions(needsPayloads, false)));
  }
  

  @Override
  public void extractTerms(Set<Term> terms) {
    other.extractTerms(terms);
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public String toString(String field) {
    return other.toString();
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    result = prime * result + (needsPayloads ? 1231 : 1237);
    result = prime * result + ((other == null) ? 0 : other.hashCode());
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
    MockSpanQuery other = (MockSpanQuery) obj;
    if (field == null) {
      if (other.field != null)
        return false;
    } else if (!field.equals(other.field))
      return false;
    if (needsPayloads != other.needsPayloads)
      return false;
    if (this.other == null) {
      if (other.other != null)
        return false;
    } else if (!this.other.equals(other.other))
      return false;
    return true;
  }




  private static final class EmptySpans extends SpansScorerWrapper {

    public EmptySpans() {
      super(null, null);
    }

    @Override
    public boolean next() {
      return false;
    }

    @Override
    public boolean skipTo(int target) {
      return false;
    }

    @Override
    public int doc() {
      return DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public int start() {
      return -1;
    }

    @Override
    public int end() {
      return -1;
    }

    @Override
    public Collection<byte[]> getPayload() {
      return null;
    }

    @Override
    public boolean isPayloadAvailable() {
      return false;
    }
  }

  public static final Spans EMPTY_SPANS = new EmptySpans();

}
