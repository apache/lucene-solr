/*
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
package org.apache.lucene.analysis.miscellaneous;


import java.text.DateFormat;
import java.text.ParseException;
import java.util.Locale;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/** Filters all tokens that cannot be parsed to a date, using the provided {@link DateFormat}. */
public class DateRecognizerFilter extends FilteringTokenFilter {

  public static final String DATE_TYPE = "date";

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final DateFormat dateFormat;

  /**
   * Uses {@link DateFormat#DEFAULT} and {@link Locale#ENGLISH} to create a {@link DateFormat} instance.
   */
  public DateRecognizerFilter(TokenStream input) {
    this(input, null);
  }

  public DateRecognizerFilter(TokenStream input, DateFormat dateFormat) {
    super(input);
    this.dateFormat = dateFormat != null ? dateFormat : DateFormat.getDateInstance(DateFormat.DEFAULT, Locale.ENGLISH);
  }

  @Override
  public boolean accept() {
    try {
      // We don't care about the date, just that the term can be parsed to one.
      dateFormat.parse(termAtt.toString());
      return true;
    } catch (ParseException e) {
      // This term is not a date.
    }

    return false;
  }

}
