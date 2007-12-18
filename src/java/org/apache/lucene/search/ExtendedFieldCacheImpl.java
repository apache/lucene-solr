package org.apache.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

import java.io.IOException;


/**
 *
 *
 **/
class ExtendedFieldCacheImpl extends FieldCacheImpl implements ExtendedFieldCache {
  private static final LongParser LONG_PARSER = new LongParser() {
      public long parseLong(String value) {
        return Long.parseLong(value);
      }
  };

  private static final DoubleParser DOUBLE_PARSER = new DoubleParser() {
      public double parseDouble(String value) {
        return Double.parseDouble(value);
      }
  };


  public long[] getLongs(IndexReader reader, String field) throws IOException {
    return getLongs(reader, field, LONG_PARSER);
  }

  // inherit javadocs
  public long[] getLongs(IndexReader reader, String field, LongParser parser)
      throws IOException {
    return (long[]) longsCache.get(reader, new Entry(field, parser));
  }

  Cache longsCache = new Cache() {

    protected Object createValue(IndexReader reader, Object entryKey)
        throws IOException {
      Entry entry = (Entry) entryKey;
      String field = entry.field;
      LongParser parser = (LongParser) entry.custom;
      final long[] retArray = new long[reader.maxDoc()];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term(field, ""));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          long termval = parser.parseLong(term.text());
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }
      return retArray;
    }
  };

  // inherit javadocs
  public double[] getDoubles(IndexReader reader, String field)
    throws IOException {
    return getDoubles(reader, field, DOUBLE_PARSER);
  }

  // inherit javadocs
  public double[] getDoubles(IndexReader reader, String field, DoubleParser parser)
      throws IOException {
    return (double[]) doublesCache.get(reader, new Entry(field, parser));
  }

  Cache doublesCache = new Cache() {

    protected Object createValue(IndexReader reader, Object entryKey)
        throws IOException {
      Entry entry = (Entry) entryKey;
      String field = entry.field;
      DoubleParser parser = (DoubleParser) entry.custom;
      final double[] retArray = new double[reader.maxDoc()];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field, ""));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          double termval = parser.parseDouble(term.text());
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }
      return retArray;
    }
  };


  // inherit javadocs
  public Object getAuto(IndexReader reader, String field) throws IOException {
    return autoCache.get(reader, field);
  }

  Cache autoCache = new Cache() {

    protected Object createValue(IndexReader reader, Object fieldKey)
        throws IOException {
      String field = ((String)fieldKey).intern();
      TermEnum enumerator = reader.terms (new Term (field, ""));
      try {
        Term term = enumerator.term();
        if (term == null) {
          throw new RuntimeException ("no terms in field " + field + " - cannot determine sort type");
        }
        Object ret = null;
        if (term.field() == field) {
          String termtext = term.text().trim();

          /**
           * Java 1.4 level code:

           if (pIntegers.matcher(termtext).matches())
           return IntegerSortedHitQueue.comparator (reader, enumerator, field);

           else if (pFloats.matcher(termtext).matches())
           return FloatSortedHitQueue.comparator (reader, enumerator, field);
           */

          // Java 1.3 level code:
          try {
            Integer.parseInt (termtext);
            ret = getInts (reader, field);
          } catch (NumberFormatException nfe1) {
            try {
              Long.parseLong(termtext);
              ret = getLongs (reader, field);
            } catch (NumberFormatException nfe2) {
              try {
                Float.parseFloat (termtext);
                ret = getFloats (reader, field);
              } catch (NumberFormatException nfe3) {
                ret = getStringIndex (reader, field);
              }
            }
          }
        } else {
          throw new RuntimeException ("field \"" + field + "\" does not appear to be indexed");
        }
        return ret;
      } finally {
        enumerator.close();
      }
    }
  };

}
