package org.apache.lucene.search;

import org.apache.lucene.index.IndexReader;

import java.io.IOException;


/**
 *
 *
 **/
public interface ExtendedFieldCache extends FieldCache {
  public interface LongParser {
    /**
     * Return an long representation of this field's value.
     */
    public long parseLong(String string);
  }

  public interface DoubleParser {
    /**
     * Return an long representation of this field's value.
     */
    public double parseDouble(String string);
  }

  public static ExtendedFieldCache EXT_DEFAULT = new ExtendedFieldCacheImpl();

  /**
   * Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as longs and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the longs.
   * @return The values in the given field for each document.
   * @throws java.io.IOException If any error occurs.
   */
  public long[] getLongs(IndexReader reader, String field)
          throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as longs and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the longs.
   * @param parser Computes integer for string values.
   * @return The values in the given field for each document.
   * @throws IOException If any error occurs.
   */
  public long[] getLongs(IndexReader reader, String field, LongParser parser)
          throws IOException;


  /**
   * Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as integers and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the doubles.
   * @return The values in the given field for each document.
   * @throws IOException If any error occurs.
   */
  public double[] getDoubles(IndexReader reader, String field)
          throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as doubles and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the doubles.
   * @param parser Computes integer for string values.
   * @return The values in the given field for each document.
   * @throws IOException If any error occurs.
   */
  public double[] getDoubles(IndexReader reader, String field, DoubleParser parser)
          throws IOException;
}
