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

import java.util.BitSet;
import java.util.Date;
import java.io.IOException;

import org.apache.lucene.document.DateField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;

/**
 * A Filter that restricts search results to a range of time.
 *
 * <p>For this to work, documents must have been indexed with a
 * {@link DateField}.
 */
public class DateFilter extends Filter {
  String field;

  String start = DateField.MIN_DATE_STRING();
  String end = DateField.MAX_DATE_STRING();

  private DateFilter(String f) {
    field = f;
  }

  /**
   * Constructs a filter for field <code>f</code> matching dates
   * between <code>from</code> and <code>to</code> inclusively.
   */
  public DateFilter(String f, Date from, Date to) {
    field = f;
    start = DateField.dateToString(from);
    end = DateField.dateToString(to);
  }

  /**
   * Constructs a filter for field <code>f</code> matching times
   * between <code>from</code> and <code>to</code> inclusively.
   */
  public DateFilter(String f, long from, long to) {
    field = f;
    start = DateField.timeToString(from);
    end = DateField.timeToString(to);
  }

  /**
   * Constructs a filter for field <code>f</code> matching
   * dates on or before before <code>date</code>.
   */
  public static DateFilter Before(String field, Date date) {
    DateFilter result = new DateFilter(field);
    result.end = DateField.dateToString(date);
    return result;
  }

  /**
   * Constructs a filter for field <code>f</code> matching times
   * on or before <code>time</code>.
   */
  public static DateFilter Before(String field, long time) {
    DateFilter result = new DateFilter(field);
    result.end = DateField.timeToString(time);
    return result;
  }

  /**
   * Constructs a filter for field <code>f</code> matching
   * dates on or after <code>date</code>.
   */
  public static DateFilter After(String field, Date date) {
    DateFilter result = new DateFilter(field);
    result.start = DateField.dateToString(date);
    return result;
  }

  /**
   * Constructs a filter for field <code>f</code> matching
   * times on or after <code>time</code>.
   */
  public static DateFilter After(String field, long time) {
    DateFilter result = new DateFilter(field);
    result.start = DateField.timeToString(time);
    return result;
  }

  /**
   * Returns a BitSet with true for documents which should be
   * permitted in search results, and false for those that should
   * not.
   */
  public BitSet bits(IndexReader reader) throws IOException {
    BitSet bits = new BitSet(reader.maxDoc());
    TermEnum enumerator = reader.terms(new Term(field, start));
    TermDocs termDocs = reader.termDocs();
    if (enumerator.term() == null) {
      return bits;
    }

    try {
      Term stop = new Term(field, end);
      while (enumerator.term().compareTo(stop) <= 0) {
        termDocs.seek(enumerator.term());
        while (termDocs.next()) {
          bits.set(termDocs.doc());
        }
        if (!enumerator.next()) {
          break;
        }
      }
    } finally {
      enumerator.close();
      termDocs.close();
    }
    return bits;
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append(field);
    buffer.append(":");
    buffer.append(DateField.stringToDate(start).toString());
    buffer.append("-");
    buffer.append(DateField.stringToDate(end).toString());
    return buffer.toString();
  }
}
