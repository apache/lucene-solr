package org.apache.lucene.search.spans;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Weight.ScorerContext;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionIntervalFilter;

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
  public Spans getSpans(AtomicReaderContext context) throws IOException {
    
    if(other instanceof SpanQuery) {
      return ((SpanQuery) other).getSpans(context);
    }
    
    ReaderContext topReaderContext = context.reader.getTopReaderContext();

    Weight weight = other.createWeight(new IndexSearcher(topReaderContext));
    Scorer scorer = weight.scorer((AtomicReaderContext) topReaderContext, ScorerContext.def().needsPositions(true).needsPayloads(needsPayloads));
    if (scorer == null) {
      return EMPTY_SPANS;
    }
    return new SpansScorerWrapper(scorer, filter == null ? scorer.positions() : filter.filter(scorer.positions()));
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
