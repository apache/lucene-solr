package org.apache.lucene.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.io.Serializable;

/**
 * An example Comparable for use with the custom sort tests.
 * It implements a comparable for "id" sort of values which
 * consist of an alphanumeric part and a numeric part, such as:
 * <p/>
 * <P>ABC-123, A-1, A-7, A-100, B-99999
 * <p/>
 * <p>Such values cannot be sorted as strings, since A-100 needs
 * to come after A-7.
 * <p/>
 * <p>It could be argued that the "ids" should be rewritten as
 * A-0001, A-0100, etc. so they will sort as strings.  That is
 * a valid alternate way to solve it - but
 * this is only supposed to be a simple test case.
 * <p/>
 * <p>Created: Apr 21, 2004 5:34:47 PM
 *
 *
 * @version $Id$
 * @since 1.4
 */
public class SampleComparable
implements Comparable, Serializable {

  String string_part;
  Integer int_part;

  public SampleComparable (String s) {
    int i = s.indexOf ("-");
    string_part = s.substring (0, i);
    int_part = new Integer (s.substring (i + 1));
  }

  public int compareTo (Object o) {
    SampleComparable otherid = (SampleComparable) o;
    int i = string_part.compareTo (otherid.string_part);
    if (i == 0) return int_part.compareTo (otherid.int_part);
    return i;
  }

  public static SortComparatorSource getComparatorSource () {
    return new SortComparatorSource () {
      public ScoreDocComparator newComparator (final IndexReader reader, String fieldname)
      throws IOException {
        final String field = StringHelper.intern(fieldname);
        final TermEnum enumerator = reader.terms (new Term (fieldname, ""));
        try {
          return new ScoreDocComparator () {
            protected Comparable[] cachedValues = fillCache (reader, enumerator, field);

            public int compare (ScoreDoc i, ScoreDoc j) {
              return cachedValues[i.doc].compareTo (cachedValues[j.doc]);
            }

            public Comparable sortValue (ScoreDoc i) {
              return cachedValues[i.doc];
            }

            public int sortType () {
              return SortField.CUSTOM;
            }
          };
        } finally {
          enumerator.close ();
        }
      }

      /**
       * Returns an array of objects which represent that natural order
       * of the term values in the given field.
       *
       * @param reader     Terms are in this index.
       * @param enumerator Use this to get the term values and TermDocs.
       * @param fieldname  Comparables should be for this field.
       * @return Array of objects representing natural order of terms in field.
       * @throws IOException If an error occurs reading the index.
       */
      protected Comparable[] fillCache (IndexReader reader, TermEnum enumerator, String fieldname)
      throws IOException {
        final String field = StringHelper.intern(fieldname);
        Comparable[] retArray = new Comparable[reader.maxDoc ()];
        if (retArray.length > 0) {
          TermDocs termDocs = reader.termDocs ();
          try {
            if (enumerator.term () == null) {
              throw new RuntimeException ("no terms in field " + field);
            }
            do {
              Term term = enumerator.term ();
              if (term.field () != field) break;
              Comparable termval = getComparable (term.text ());
              termDocs.seek (enumerator);
              while (termDocs.next ()) {
                retArray[termDocs.doc ()] = termval;
              }
            } while (enumerator.next ());
          } finally {
            termDocs.close ();
          }
        }
        return retArray;
      }

      Comparable getComparable (String termtext) {
        return new SampleComparable (termtext);
      }
    };
  }

  private static final class InnerSortComparator extends SortComparator {
      protected Comparable getComparable (String termtext) {
        return new SampleComparable (termtext);
      }
      public int hashCode() { return this.getClass().getName().hashCode(); }
      public boolean equals(Object that) { return this.getClass().equals(that.getClass()); }
    };
  
  public static SortComparator getComparator() {
    return new InnerSortComparator();
  }
}
