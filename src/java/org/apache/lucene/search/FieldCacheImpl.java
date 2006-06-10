package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.HashMap;

/**
 * Expert: The default cache implementation, storing all values in memory.
 * A WeakHashMap is used for storage.
 *
 * <p>Created: May 19, 2004 4:40:36 PM
 *
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 */
class FieldCacheImpl
implements FieldCache {

  /** Expert: Every key in the internal cache is of this type. */
  static class Entry {
    final String field;        // which Fieldable
    final int type;            // which SortField type
    final Object custom;       // which custom comparator
    final Locale locale;       // the locale we're sorting (if string)

    /** Creates one of these objects. */
    Entry (String field, int type, Locale locale) {
      this.field = field.intern();
      this.type = type;
      this.custom = null;
      this.locale = locale;
    }

    /** Creates one of these objects for a custom comparator. */
    Entry (String field, Object custom) {
      this.field = field.intern();
      this.type = SortField.CUSTOM;
      this.custom = custom;
      this.locale = null;
    }

    /** Two of these are equal iff they reference the same field and type. */
    public boolean equals (Object o) {
      if (o instanceof Entry) {
        Entry other = (Entry) o;
        if (other.field == field && other.type == type) {
          if (other.locale == null ? locale == null : other.locale.equals(locale)) {
            if (other.custom == null) {
              if (custom == null) return true;
            } else if (other.custom.equals (custom)) {
              return true;
            }
          }
        }
      }
      return false;
    }

    /** Composes a hashcode based on the field and type. */
    public int hashCode() {
      return field.hashCode() ^ type ^ (custom==null ? 0 : custom.hashCode()) ^ (locale==null ? 0 : locale.hashCode());
    }
  }

  private static final IntParser INT_PARSER = new IntParser() {
      public int parseInt(String value) {
        return Integer.parseInt(value);
      }
    };

  private static final FloatParser FLOAT_PARSER = new FloatParser() {
      public float parseFloat(String value) {
        return Float.parseFloat(value);
      }
    };

  /** The internal cache. Maps Entry to array of interpreted term values. **/
  final Map cache = new WeakHashMap();

  /** See if an object is in the cache. */
  Object lookup (IndexReader reader, String field, int type, Locale locale) {
    Entry entry = new Entry (field, type, locale);
    synchronized (this) {
      HashMap readerCache = (HashMap)cache.get(reader);
      if (readerCache == null) return null;
      return readerCache.get (entry);
    }
  }

  /** See if a custom object is in the cache. */
  Object lookup (IndexReader reader, String field, Object comparer) {
    Entry entry = new Entry (field, comparer);
    synchronized (this) {
      HashMap readerCache = (HashMap)cache.get(reader);
      if (readerCache == null) return null;
      return readerCache.get (entry);
    }
  }

  /** Put an object into the cache. */
  Object store (IndexReader reader, String field, int type, Locale locale, Object value) {
    Entry entry = new Entry (field, type, locale);
    synchronized (this) {
      HashMap readerCache = (HashMap)cache.get(reader);
      if (readerCache == null) {
        readerCache = new HashMap();
        cache.put(reader,readerCache);
      }
      return readerCache.put (entry, value);
    }
  }

  /** Put a custom object into the cache. */
  Object store (IndexReader reader, String field, Object comparer, Object value) {
    Entry entry = new Entry (field, comparer);
    synchronized (this) {
      HashMap readerCache = (HashMap)cache.get(reader);
      if (readerCache == null) {
        readerCache = new HashMap();
        cache.put(reader, readerCache);
      }
      return readerCache.put (entry, value);
    }
  }

  // inherit javadocs
  public int[] getInts (IndexReader reader, String field) throws IOException {
    return getInts(reader, field, INT_PARSER);
  }

  // inherit javadocs
  public int[] getInts (IndexReader reader, String field, IntParser parser)
  throws IOException {
    field = field.intern();
    Object ret = lookup (reader, field, parser);
    if (ret == null) {
      final int[] retArray = new int[reader.maxDoc()];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field, ""));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          int termval = parser.parseInt(term.text());
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }
      store (reader, field, parser, retArray);
      return retArray;
    }
    return (int[]) ret;
  }

  // inherit javadocs
  public float[] getFloats (IndexReader reader, String field)
    throws IOException {
    return getFloats(reader, field, FLOAT_PARSER);
  }

  // inherit javadocs
  public float[] getFloats (IndexReader reader, String field,
                            FloatParser parser) throws IOException {
    field = field.intern();
    Object ret = lookup (reader, field, parser);
    if (ret == null) {
      final float[] retArray = new float[reader.maxDoc()];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field, ""));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          float termval = parser.parseFloat(term.text());
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }
      store (reader, field, parser, retArray);
      return retArray;
    }
    return (float[]) ret;
  }

  // inherit javadocs
  public String[] getStrings (IndexReader reader, String field)
  throws IOException {
    field = field.intern();
    Object ret = lookup (reader, field, SortField.STRING, null);
    if (ret == null) {
      final String[] retArray = new String[reader.maxDoc()];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field, ""));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          String termval = term.text();
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }
      store (reader, field, SortField.STRING, null, retArray);
      return retArray;
    }
    return (String[]) ret;
  }

  // inherit javadocs
  public StringIndex getStringIndex (IndexReader reader, String field)
  throws IOException {
    field = field.intern();
    Object ret = lookup (reader, field, STRING_INDEX, null);
    if (ret == null) {
      final int[] retArray = new int[reader.maxDoc()];
      String[] mterms = new String[reader.maxDoc()+1];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field, ""));
      int t = 0;  // current term number

      // an entry for documents that have no terms in this field
      // should a document with no terms be at top or bottom?
      // this puts them at the top - if it is changed, FieldDocSortedHitQueue
      // needs to change as well.
      mterms[t++] = null;

      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;

          // store term text
          // we expect that there is at most one term per document
          if (t >= mterms.length) throw new RuntimeException ("there are more terms than " +
                  "documents in field \"" + field + "\", but it's impossible to sort on " +
                  "tokenized fields");
          mterms[t] = term.text();

          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = t;
          }

          t++;
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }

      if (t == 0) {
        // if there are no terms, make the term array
        // have a single null entry
        mterms = new String[1];
      } else if (t < mterms.length) {
        // if there are less terms than documents,
        // trim off the dead array space
        String[] terms = new String[t];
        System.arraycopy (mterms, 0, terms, 0, t);
        mterms = terms;
      }

      StringIndex value = new StringIndex (retArray, mterms);
      store (reader, field, STRING_INDEX, null, value);
      return value;
    }
    return (StringIndex) ret;
  }

  /** The pattern used to detect integer values in a field */
  /** removed for java 1.3 compatibility
   protected static final Pattern pIntegers = Pattern.compile ("[0-9\\-]+");
   **/

  /** The pattern used to detect float values in a field */
  /**
   * removed for java 1.3 compatibility
   * protected static final Object pFloats = Pattern.compile ("[0-9+\\-\\.eEfFdD]+");
   */

  // inherit javadocs
  public Object getAuto (IndexReader reader, String field)
  throws IOException {
    field = field.intern();
    Object ret = lookup (reader, field, SortField.AUTO, null);
    if (ret == null) {
      TermEnum enumerator = reader.terms (new Term (field, ""));
      try {
        Term term = enumerator.term();
        if (term == null) {
          throw new RuntimeException ("no terms in field " + field + " - cannot determine sort type");
        }
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
              Float.parseFloat (termtext);
              ret = getFloats (reader, field);
            } catch (NumberFormatException nfe2) {
              ret = getStringIndex (reader, field);
            }
          }
          if (ret != null) {
            store (reader, field, SortField.AUTO, null, ret);
          }
        } else {
          throw new RuntimeException ("field \"" + field + "\" does not appear to be indexed");
        }
      } finally {
        enumerator.close();
      }

    }
    return ret;
  }

  // inherit javadocs
  public Comparable[] getCustom (IndexReader reader, String field, SortComparator comparator)
  throws IOException {
    field = field.intern();
    Object ret = lookup (reader, field, comparator);
    if (ret == null) {
      final Comparable[] retArray = new Comparable[reader.maxDoc()];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field, ""));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          Comparable termval = comparator.getComparable (term.text());
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }
      store (reader, field, comparator, retArray);
      return retArray;
    }
    return (Comparable[]) ret;
  }

}

