package org.apache.lucene.document;

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

import java.util.Date;

/**
 * Provides support for converting dates to strings and vice-versa.
 * The strings are structured so that lexicographic sorting orders by date.
 * This makes them suitable for use as field values and search terms.
 * <P>
 * Note: currenly dates before 1970 cannot be used, and therefore cannot be
 * indexed.
 */
public class DateField {
  private DateField() {}

  // make date strings long enough to last a millenium
  private static int DATE_LEN = Long.toString(1000L*365*24*60*60*1000,
					       Character.MAX_RADIX).length();

  public static String MIN_DATE_STRING() {
    return timeToString(0);
  }

  public static String MAX_DATE_STRING() {
    char[] buffer = new char[DATE_LEN];
    char c = Character.forDigit(Character.MAX_RADIX-1, Character.MAX_RADIX);
    for (int i = 0 ; i < DATE_LEN; i++)
      buffer[i] = c;
    return new String(buffer);
  }

  /**
   * Converts a Date to a string suitable for indexing.
   * This method will throw a RuntimeException if the date specified in the
   * method argument is before 1970.
   */
  public static String dateToString(Date date) {
    return timeToString(date.getTime());
  }
  /**
   * Converts a millisecond time to a string suitable for indexing.
   * This method will throw a RuntimeException if the time specified in the
   * method argument is negative, that is, before 1970.
   */
  public static String timeToString(long time) {
    if (time < 0)
      throw new RuntimeException("time too early");

    String s = Long.toString(time, Character.MAX_RADIX);

    if (s.length() > DATE_LEN)
      throw new RuntimeException("time too late");

    // Pad with leading zeros
    if (s.length() < DATE_LEN) {
      StringBuffer sb = new StringBuffer(s);
      while (sb.length() < DATE_LEN)
        sb.insert(0, ' ');
      s = sb.toString();
    }

    return s;
  }

  /** Converts a string-encoded date into a millisecond time. */
  public static long stringToTime(String s) {
    return Long.parseLong(s, Character.MAX_RADIX);
  }
  /** Converts a string-encoded date into a Date object. */
  public static Date stringToDate(String s) {
    return new Date(stringToTime(s));
  }
}
