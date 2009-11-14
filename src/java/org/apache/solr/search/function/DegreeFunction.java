package org.apache.solr.search.function;

import org.apache.lucene.index.IndexReader;

import java.util.Map;
import java.io.IOException;


/**
 *
 *
 **/
public class DegreeFunction extends ValueSource{
  protected ValueSource valSource;

  public DegreeFunction(ValueSource valSource) {
    this.valSource = valSource;
  }

  public String description() {
    return "deg(" + valSource.description() + ')';
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final DocValues dv = valSource.getValues(context, reader);
    return new DocValues() {
      public float floatVal(int doc) {
        return (float) doubleVal(doc);
      }

      public int intVal(int doc) {
        return (int) doubleVal(doc);
      }

      public long longVal(int doc) {
        return (long) doubleVal(doc);
      }

      public double doubleVal(int doc) {
        return Math.toDegrees(dv.doubleVal(doc));
      }

      public String strVal(int doc) {
        return Double.toString(doubleVal(doc));
      }

      public String toString(int doc) {
        return description() + '=' + floatVal(doc);
      }
    };
  }

  public boolean equals(Object o) {
    if (o.getClass() != DegreeFunction.class) return false;
    DegreeFunction other = (DegreeFunction) o;
    return description().equals(other.description()) && valSource.equals(other.valSource);
  }

  public int hashCode() {
    return description().hashCode() + valSource.hashCode();
  };

}
