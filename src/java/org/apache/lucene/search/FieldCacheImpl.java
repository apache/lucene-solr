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
import java.util.Map;
import java.util.WeakHashMap;

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
    final IndexReader reader;  // which Reader
    final String field;        // which Field
    final int type;            // which SortField type
    final Object custom;       // which custom comparator
    final int hashcode;        // unique for this object

    /** Creates one of these objects. */
    Entry (IndexReader reader, String field, int type) {
      this.reader = reader;
      this.field = field.intern();
      this.type = type;
      this.custom = null;
      this.hashcode = reader.hashCode() ^ field.hashCode() ^ type;
    }

    /** Creates one of these objects for a custom comparator. */
    Entry (IndexReader reader, String field, Object custom) {
      this.reader = reader;
      this.field = field.intern();
      this.type = SortField.CUSTOM;
      this.custom = custom;
      this.hashcode = reader.hashCode() ^ field.hashCode() ^ type ^ custom.hashCode();
    }

    /** Two of these are equal iff they reference the same reader, field and type. */
    public boolean equals (Object o) {
      if (o instanceof Entry) {
        Entry other = (Entry) o;
        if (other.reader == reader && other.field == field && other.type == type) {
          if (other.custom == null) {
            if (custom == null) return true;
          } else if (other.custom.equals (custom)) {
            return true;
          }
        }
      }
      return false;
    }

    /** Composes a hashcode based on the referenced reader, field and type. */
    public int hashCode() {
      return hashcode;
    }
  }


  /** The internal cache. Maps Entry to array of interpreted term values. **/
  final Map cache = new WeakHashMap();

  /** See if an object is in the cache. */
  Object lookup (IndexReader reader, String field, int type) {
    Entry entry = new Entry (reader, field, type);
    synchronized (this) {
      return cache.get (entry);
    }
  }

  /** See if a custom object is in the cache. */
  Object lookup (IndexReader reader, String field, Object comparer) {
    Entry entry = new Entry (reader, field, comparer);
    synchronized (this) {
      return cache.get (entry);
    }
  }

  /** Put an object into the cache. */
  Object store (IndexReader reader, String field, int type, Object value) {
    Entry entry = new Entry (reader, field, type);
    synchronized (this) {
      return cache.put (entry, value);
    }
  }

  /** Put a custom object into the cache. */
  Object store (IndexReader reader, String field, Object comparer, Object value) {
    Entry entry = new Entry (reader, field, comparer);
    synchronized (this) {
      return cache.put (entry, value);
    }
  }

  // inherit javadocs
  public int[] getInts (IndexReader reader, String field)
  throws IOException {
    field = field.intern();
    Object ret = lookup (reader, field, SortField.INT);
    if (ret == null) {
      final int[] retArray = new int[reader.maxDoc()];
      if (retArray.length > 0) {
        TermDocs termDocs = reader.termDocs();
        TermEnum termEnum = reader.terms (new Term (field, ""));
        try {
          if (termEnum.term() == null) {
            throw new RuntimeException ("no terms in field " + field);
          }
          do {
            Term term = termEnum.term();
            if (term.field() != field) break;
            int termval = Integer.parseInt (term.text());
            termDocs.seek (termEnum);
            while (termDocs.next()) {
              retArray[termDocs.doc()] = termval;
            }
          } while (termEnum.next());
        } finally {
          termDocs.close();
          termEnum.close();
        }
      }
      store (reader, field, SortField.INT, retArray);
      return retArray;
    }
    return (int[]) ret;
  }

  // inherit javadocs
  public float[] getFloats (IndexReader reader, String field)
  throws IOException {
    field = field.intern();
    Object ret = lookup (reader, field, SortField.FLOAT);
    if (ret == null) {
      final float[] retArray = new float[reader.maxDoc()];
      if (retArray.length > 0) {
        TermDocs termDocs = reader.termDocs();
        TermEnum termEnum = reader.terms (new Term (field, ""));
        try {
          if (termEnum.term() == null) {
            throw new RuntimeException ("no terms in field " + field);
          }
          do {
            Term term = termEnum.term();
            if (term.field() != field) break;
            float termval = Float.parseFloat (term.text());
            termDocs.seek (termEnum);
            while (termDocs.next()) {
              retArray[termDocs.doc()] = termval;
            }
          } while (termEnum.next());
        } finally {
          termDocs.close();
          termEnum.close();
        }
      }
      store (reader, field, SortField.FLOAT, retArray);
      return retArray;
    }
    return (float[]) ret;
  }

  // inherit javadocs
  public String[] getStrings (IndexReader reader, String field)
  throws IOException {
    field = field.intern();
    Object ret = lookup (reader, field, SortField.STRING);
    if (ret == null) {
      final String[] retArray = new String[reader.maxDoc()];
      if (retArray.length > 0) {
        TermDocs termDocs = reader.termDocs();
        TermEnum termEnum = reader.terms (new Term (field, ""));
        try {
          if (termEnum.term() == null) {
            throw new RuntimeException ("no terms in field " + field);
          }
          do {
            Term term = termEnum.term();
            if (term.field() != field) break;
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
      }
      store (reader, field, SortField.STRING, retArray);
      return retArray;
    }
    return (String[]) ret;
  }

  // inherit javadocs
  public StringIndex getStringIndex (IndexReader reader, String field)
  throws IOException {
    field = field.intern();
    Object ret = lookup (reader, field, STRING_INDEX);
    if (ret == null) {
      final int[] retArray = new int[reader.maxDoc()];
      String[] mterms = new String[reader.maxDoc()];
      if (retArray.length > 0) {
        TermDocs termDocs = reader.termDocs();
        TermEnum termEnum = reader.terms (new Term (field, ""));
        int t = 0;  // current term number
        try {
          if (termEnum.term() == null) {
            throw new RuntimeException ("no terms in field " + field);
          }
          do {
            Term term = termEnum.term();
            if (term.field() != field) break;

            // store term text
            // we expect that there is at most one term per document
            if (t >= mterms.length) throw new RuntimeException ("there are more terms than documents in field \"" + field + "\"");
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
      }
      StringIndex value = new StringIndex (retArray, mterms);
      store (reader, field, STRING_INDEX, value);
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
    Object ret = lookup (reader, field, SortField.AUTO);
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
            store (reader, field, SortField.AUTO, ret);
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
      if (retArray.length > 0) {
        TermDocs termDocs = reader.termDocs();
        TermEnum termEnum = reader.terms (new Term (field, ""));
        try {
          if (termEnum.term() == null) {
            throw new RuntimeException ("no terms in field " + field);
          }
          do {
            Term term = termEnum.term();
            if (term.field() != field) break;
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
      }
      store (reader, field, SortField.CUSTOM, retArray);
      return retArray;
    }
    return (String[]) ret;
  }

}
