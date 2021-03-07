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
package org.apache.solr.internal.csv;

import java.io.Serializable;

/**
 * CSVStrategy
 *
 * Represents the strategy for a CSV.
 */
public class CSVStrategy implements Cloneable, Serializable {

  final private char delimiter;
  final private char encapsulator;
  final private char commentStart;
  final private char escape;
  final private boolean ignoreLeadingWhitespaces;
  final private boolean ignoreTrailingWhitespaces;
  final private boolean interpretUnicodeEscapes;
  final private boolean ignoreEmptyLines;

  // controls for output
  final private String printerNewline;

  // -2 is used to signal disabled, because it won't be confused with
  // an EOF signal (-1), and because \ufffe in UTF-16 would be
  // encoded as two chars (using surrogates) and thus there should never
  // be a collision with a real text char.
  public static char COMMENTS_DISABLED       = (char)-2;
  public static char ESCAPE_DISABLED         = (char)-2;
  public static char ENCAPSULATOR_DISABLED   = (char)-2;
  public static String DEFAULT_PRINTER_NEWLINE = "\n";

  public static final CSVStrategy DEFAULT_STRATEGY = new CSVStrategy
      (',', '"', COMMENTS_DISABLED, ESCAPE_DISABLED, true, true, false, true, DEFAULT_PRINTER_NEWLINE);
  public static final CSVStrategy EXCEL_STRATEGY = new CSVStrategy
      (',', '"', COMMENTS_DISABLED, ESCAPE_DISABLED, false, false, false, false, DEFAULT_PRINTER_NEWLINE);
  public static final CSVStrategy TDF_STRATEGY = new CSVStrategy
      ('\t', '"', COMMENTS_DISABLED, ESCAPE_DISABLED, true, true, false, true, DEFAULT_PRINTER_NEWLINE);

  public CSVStrategy(char delimiter, char encapsulator, char commentStart) {
    this(delimiter, encapsulator, commentStart, ESCAPE_DISABLED, true, true, false, true, DEFAULT_PRINTER_NEWLINE);
  }

  /**
   * Customized CSV strategy setter.
   *
   * @param delimiter a Char used for value separation
   * @param encapsulator a Char used as value encapsulation marker
   * @param commentStart a Char used for comment identification
   * @param escape a Char used for escaping
   * @param ignoreTrailingWhitespaces TRUE when trailing whitespaces should be
   *                                 ignored
   * @param ignoreLeadingWhitespaces TRUE when leading whitespaces should be
   *                                ignored
   * @param interpretUnicodeEscapes TRUE when unicode escapes should be
   *                                interpreted
   * @param ignoreEmptyLines TRUE when the parser should skip emtpy lines
   * @param printerNewline The string to use when printing a newline
   */
  public CSVStrategy(char delimiter, char encapsulator, char commentStart, char escape,
      boolean ignoreLeadingWhitespaces, boolean ignoreTrailingWhitespaces,
      boolean interpretUnicodeEscapes, boolean ignoreEmptyLines,
      String printerNewline) {
    this.delimiter = delimiter;
    this.encapsulator = encapsulator;
    this.commentStart = commentStart;
    this.escape = escape;
    this.ignoreLeadingWhitespaces = ignoreLeadingWhitespaces;
    this.ignoreTrailingWhitespaces = ignoreTrailingWhitespaces;
    this.interpretUnicodeEscapes = interpretUnicodeEscapes;
    this.ignoreEmptyLines = ignoreEmptyLines;
    this.printerNewline = printerNewline;
  }

  public char getDelimiter() { return this.delimiter; }

  public char getEncapsulator() { return this.encapsulator; }

  public char getCommentStart() { return this.commentStart; }
  public boolean isCommentingDisabled() { return this.commentStart == COMMENTS_DISABLED; }

  public char getEscape() { return this.escape; }

  public boolean getIgnoreLeadingWhitespaces() { return this.ignoreLeadingWhitespaces; }

  public boolean getIgnoreTrailingWhitespaces() { return this.ignoreTrailingWhitespaces; }

  public boolean getUnicodeEscapeInterpretation() { return this.interpretUnicodeEscapes; }

  public boolean getIgnoreEmptyLines() { return this.ignoreEmptyLines; }

  public String getPrinterNewline() {
    return this.printerNewline;
  }

}
