package org.apache.lucene.search.spans;

import java.io.IOException;

import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;

/** Matches the union of its clauses.*/
public class SpanOrQuery extends SpanQuery {
  private List clauses;
  private String field;

  /** Construct a SpanOrQuery merging the provided clauses. */
  public SpanOrQuery(SpanQuery[] clauses) {

    // copy clauses array into an ArrayList
    this.clauses = new ArrayList(clauses.length);
    for (int i = 0; i < clauses.length; i++) {
      SpanQuery clause = clauses[i];
      if (i == 0) {                               // check field
        field = clause.getField();
      } else if (!clause.getField().equals(field)) {
        throw new IllegalArgumentException("Clauses must have same field.");
      }
      this.clauses.add(clause);
    }
  }

  /** Return the clauses whose spans are matched. */
  public SpanQuery[] getClauses() {
    return (SpanQuery[])clauses.toArray(new SpanQuery[clauses.size()]);
  }

  public String getField() { return field; }

  public Collection getTerms() {
    Collection terms = new ArrayList();
    Iterator i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = (SpanQuery)i.next();
      terms.addAll(clause.getTerms());
    }
    return terms;
  }

  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("spanOr([");
    Iterator i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = (SpanQuery)i.next();
      buffer.append(clause.toString(field));
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("])");
    return buffer.toString();
  }

  public Spans getSpans(final IndexReader reader) throws IOException {
    if (clauses.size() == 1)                      // optimize 1-clause case
      return ((SpanQuery)clauses.get(0)).getSpans(reader);

    return new Spans() {
        private List all = new ArrayList(clauses.size());
        private SpanQueue queue = new SpanQueue(clauses.size());

        {
          Iterator i = clauses.iterator();
          while (i.hasNext()) {                   // initialize all
            all.add(((SpanQuery)i.next()).getSpans(reader)); 
          }
        }
        
        private boolean firstTime = true;

        public boolean next() throws IOException {
          if (firstTime) {                        // first time -- initialize
            for (int i = 0; i < all.size(); i++) {
              Spans spans = (Spans)all.get(i);
              if (spans.next()) {                 // move to first entry
                queue.put(spans);                 // build queue
              }
            }
            firstTime = false;
            return queue.size() != 0;
          }

          if (queue.size() == 0) {                // all done
            return false;
          }

          if (top().next()) {                       // move to next
            queue.adjustTop();
            return true;
          }

          queue.pop();                            // exhausted a clause
          return queue.size() != 0;
        }

        private Spans top() { return (Spans)queue.top(); }

        public boolean skipTo(int target) throws IOException {
          queue.clear();                          // clear the queue
          for (int i = 0; i < all.size(); i++) {
            Spans spans = (Spans)all.get(i);
            if (spans.skipTo(target)) {           // skip each spans in all
              queue.put(spans);                   // rebuild queue
            }
          }
          firstTime = false;
          return queue.size() != 0;
        }

        public int doc() { return top().doc(); }
        public int start() { return top().start(); }
        public int end() { return top().end(); }

        public String toString() {
          return "spans(" + SpanOrQuery.this.toString() + ")";
        }

      };
  }

}
