package org.apache.lucene.analysis.sinks;
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

import org.apache.lucene.analysis.SinkTokenizer;
import org.apache.lucene.analysis.Token;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.List;
import java.util.Date;


/**
 * Attempts to parse the {@link org.apache.lucene.analysis.Token#termBuffer()} as a Date using a {@link java.text.DateFormat}.
 * If the value is a Date, it will add it to the sink.
 * <p/>
 * Also marks the sink token with {@link org.apache.lucene.analysis.Token#type()} equal to {@link #DATE_TYPE}
 *
 *
 **/
public class DateRecognizerSinkTokenizer extends SinkTokenizer {
  public static final String DATE_TYPE = "date";

  protected DateFormat dateFormat;

  /**
   * Uses {@link java.text.SimpleDateFormat#getDateInstance()} as the {@link java.text.DateFormat} object.
   */
  public DateRecognizerSinkTokenizer() {
    this(null, SimpleDateFormat.getDateInstance());
  }

  public DateRecognizerSinkTokenizer(DateFormat dateFormat) {
    this(null, dateFormat);
  }

  /**
   * Uses {@link java.text.SimpleDateFormat#getDateInstance()} as the {@link java.text.DateFormat} object.
   * @param input The input list of Tokens that are already Dates.  They should be marked as type {@link #DATE_TYPE} for completeness
   */
  public DateRecognizerSinkTokenizer(List/*<Token>*/ input) {
    this(input, SimpleDateFormat.getDateInstance());
  }

  /**
   *
   * @param input
   * @param dateFormat The date format to use to try and parse the date.  Note, this SinkTokenizer makes no attempt to synchronize the DateFormat object
   */
  public DateRecognizerSinkTokenizer(List/*<Token>*/ input, DateFormat dateFormat) {
    super(input);
    this.dateFormat = dateFormat;
  }


  public void add(Token t) {
    //Check to see if this token is a date
    if (t != null) {
      try {
        Date date = dateFormat.parse(t.term());//We don't care about the date, just that we can parse it as a date
        if (date != null) {
          t.setType(DATE_TYPE);
          super.add(t);
        }
      } catch (ParseException e) {

      }
    }

  }
}
