package org.apache.lucene.search.spans;

import java.io.IOException;

import java.util.Collection;

import org.apache.lucene.index.IndexReader;

/** Matches spans near the beginning of a field. */
public class SpanFirstQuery extends SpanQuery {
  private SpanQuery match;
  private int end;

  /** Construct a SpanFirstQuery matching spans in <code>match</code> whose end
   * position is less than or equal to <code>end</code>. */
  public SpanFirstQuery(SpanQuery match, int end) {
    this.match = match;
    this.end = end;
  }

  /** Return the SpanQuery whose matches are filtered. */
  public SpanQuery getMatch() { return match; }

  /** Return the maximum end position permitted in a match. */
  public int getEnd() { return end; }

  public String getField() { return match.getField(); }

  public Collection getTerms() { return match.getTerms(); }

  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("spanFirst(");
    buffer.append(match.toString(field));
    buffer.append(", ");
    buffer.append(end);
    buffer.append(")");
    return buffer.toString();
  }

  public Spans getSpans(final IndexReader reader) throws IOException {
    return new Spans() {
        private Spans spans = match.getSpans(reader);

        public boolean next() throws IOException {
          while (spans.next()) {                  // scan to next match
            if (end() <= end)
              return true;
          }
          return false;
        }

        public boolean skipTo(int target) throws IOException {
          if (!spans.skipTo(target))
            return false;

          if (spans.end() <= end)                 // there is a match
            return true;
          
          return next();                          // scan to next match
        }

        public int doc() { return spans.doc(); }
        public int start() { return spans.start(); }
        public int end() { return spans.end(); }

        public String toString() {
          return "spans(" + SpanFirstQuery.this.toString() + ")";
        }

      };
  }

}
