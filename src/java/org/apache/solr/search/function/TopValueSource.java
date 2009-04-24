package org.apache.solr.search.function;

import org.apache.solr.search.SolrIndexReader;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;

/**
 * A value source that wraps another and ensures that the top level reader
 * is used.  This is useful for value sources like ord() who's value depend
 * on all those around it.
 */
public class TopValueSource extends ValueSource {
  private final ValueSource vs;

  public TopValueSource(ValueSource vs) {
    this.vs = vs;
  }

  public ValueSource getValueSource() {
    return vs;
  }

  public String description() {
    return "top(" + vs.description() + ')';
  }

  public DocValues getValues(IndexReader reader) throws IOException {
    int offset = 0;
    IndexReader topReader = reader;
    if (topReader instanceof SolrIndexReader) {
      SolrIndexReader r = (SolrIndexReader)topReader;
      while (r.getParent() != null) {
        offset += r.getBase();
        r = r.getParent();
      }
      topReader = r;
    }
    final int off = offset;
    final DocValues vals = vs.getValues(topReader);
    if (topReader == reader) return vals;

    return new DocValues() {
      public float floatVal(int doc) {
        return vals.floatVal(doc + off);
      }

      public int intVal(int doc) {
        return vals.intVal(doc + off);
      }

      public long longVal(int doc) {
        return vals.longVal(doc + off);
      }

      public double doubleVal(int doc) {
        return vals.doubleVal(doc + off);
      }

      public String strVal(int doc) {
        return vals.strVal(doc + off);
      }

      public String toString(int doc) {
        return vals.strVal(doc + off);
      }
    };
  }

  public boolean equals(Object o) {
    if (o.getClass() !=  TopValueSource.class) return false;
    TopValueSource other = (TopValueSource)o;
    return vs.equals(other.vs);
  }

  public int hashCode() {
    int h = vs.hashCode();
    return (h<<1) | (h>>>31);
  }

  public String toString() {
    return "top("+vs.toString()+')';
  }
}