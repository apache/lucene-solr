package org.apache.solr.search.function;

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;


/**
 *
 *
 **/
public class ShortFieldSource extends FieldCacheSource{
  FieldCache.ShortParser parser;

  public ShortFieldSource(String field) {
    this(field, null);
  }

  public ShortFieldSource(String field, FieldCache.ShortParser parser) {
    super(field);
    this.parser = parser;
  }

  public String description() {
    return "short(" + field + ')';
  }

  public DocValues getValues(IndexReader reader) throws IOException {
    final short[] arr = (parser == null) ?
            cache.getShorts(reader, field) :
            cache.getShorts(reader, field, parser);
    return new DocValues() {
      @Override
      public byte byteVal(int doc) {
        return (byte) arr[doc];
      }

      @Override
      public short shortVal(int doc) {
        return (short) arr[doc];
      }

      public float floatVal(int doc) {
        return (float) arr[doc];
      }

      public int intVal(int doc) {
        return (int) arr[doc];
      }

      public long longVal(int doc) {
        return (long) arr[doc];
      }

      public double doubleVal(int doc) {
        return (double) arr[doc];
      }

      public String strVal(int doc) {
        return Short.toString(arr[doc]);
      }

      public String toString(int doc) {
        return description() + '=' + shortVal(doc);
      }

    };
  }

  public boolean equals(Object o) {
    if (o.getClass() != ShortFieldSource.class) return false;
    ShortFieldSource
            other = (ShortFieldSource) o;
    return super.equals(other)
            && this.parser == null ? other.parser == null :
            this.parser.getClass() == other.parser.getClass();
  }

  public int hashCode() {
    int h = parser == null ? Short.class.hashCode() : parser.getClass().hashCode();
    h += super.hashCode();
    return h;
  }
}
