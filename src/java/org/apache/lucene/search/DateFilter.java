package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.util.BitSet;
import java.util.Date;
import java.io.IOException;

import org.apache.lucene.document.DateField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;

/** A Filter that restricts search results to a range of time.

   <p>For this to work, documents must have been indexed with a {@link
   DateField}.  */

public final class DateFilter extends Filter {
  String field;

  String start = DateField.MIN_DATE_STRING();
  String end = DateField.MAX_DATE_STRING();

  private DateFilter(String f) {
    field = f;
  }

  /** Constructs a filter for field <code>f</code> matching dates between
    <code>from</code> and <code>to</code>. */
  public DateFilter(String f, Date from, Date to) {
    field = f;
    start = DateField.dateToString(from);
    end = DateField.dateToString(to);
  }
  /** Constructs a filter for field <code>f</code> matching times between
    <code>from</code> and <code>to</code>. */
  public DateFilter(String f, long from, long to) {
    field = f;
    start = DateField.timeToString(from);
    end = DateField.timeToString(to);
  }

  /** Constructs a filter for field <code>f</code> matching dates before
    <code>date</code>. */
  public static DateFilter Before(String field, Date date) {
    DateFilter result = new DateFilter(field);
    result.end = DateField.dateToString(date);
    return result;
  }
  /** Constructs a filter for field <code>f</code> matching times before
    <code>time</code>. */
  public static DateFilter Before(String field, long time) {
    DateFilter result = new DateFilter(field);
    result.end = DateField.timeToString(time);
    return result;
  }

  /** Constructs a filter for field <code>f</code> matching dates before
    <code>date</code>. */
  public static DateFilter After(String field, Date date) {
    DateFilter result = new DateFilter(field);
    result.start = DateField.dateToString(date);
    return result;
  }
  /** Constructs a filter for field <code>f</code> matching times before
    <code>time</code>. */
  public static DateFilter After(String field, long time) {
    DateFilter result = new DateFilter(field);
    result.start = DateField.timeToString(time);
    return result;
  }

  /** Returns a BitSet with true for documents which should be permitted in
    search results, and false for those that should not. */
  final public BitSet bits(IndexReader reader) throws IOException {
    BitSet bits = new BitSet(reader.maxDoc());
    TermEnum enum = reader.terms(new Term(field, start));
    try {
      Term stop = new Term(field, end);
      while (enum.term().compareTo(stop) <= 0) {
	TermDocs termDocs = reader.termDocs(enum.term());
	try {
	  while (termDocs.next())
	    bits.set(termDocs.doc());
	} finally {
	  termDocs.close();
	}
	if (!enum.next()) {
	  break;
	}
      }
    } finally {
      enum.close();
    }
    return bits;
  }

  public final String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append(field);
    buffer.append(":");
    buffer.append(DateField.stringToDate(start).toString());
    buffer.append("-");
    buffer.append(DateField.stringToDate(end).toString());
    return buffer.toString();
  }
}
