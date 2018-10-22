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

  private char delimiter;
  private char encapsulator;
  private char commentStart;
  private char escape;
  private boolean ignoreLeadingWhitespaces;
  private boolean ignoreTrailingWhitespaces;
  private boolean interpretUnicodeEscapes;
  private boolean ignoreEmptyLines;

  // controls for output
  private String printerNewline;

  // -2 is used to signal disabled, because it won't be confused with
  // an EOF signal (-1), and because \ufffe in UTF-16 would be
  // encoded as two chars (using surrogates) and thus there should never
  // be a collision with a real text char.
  public static char COMMENTS_DISABLED       = (char)-2;
  public static char ESCAPE_DISABLED         = (char)-2;
  public static char ENCAPSULATOR_DISABLED   = (char)-2;
  public static String DEFAULT_PRINTER_NEWLINE = "\n";

  public static final CSVStrategy DEFAULT_STRATEGY = new ImmutableCSVStrategy
      (',', '"', COMMENTS_DISABLED, ESCAPE_DISABLED, true, true, false, true, DEFAULT_PRINTER_NEWLINE);
  public static final CSVStrategy EXCEL_STRATEGY = new ImmutableCSVStrategy
      (',', '"', COMMENTS_DISABLED, ESCAPE_DISABLED, false, false, false, false, DEFAULT_PRINTER_NEWLINE);
  public static final CSVStrategy TDF_STRATEGY = new ImmutableCSVStrategy
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
   * @deprecated Use the ctor that also takes printerNewline.  This ctor will be removed in Solr 7.
   */
  @Deprecated
  public CSVStrategy(char delimiter, char encapsulator, char commentStart, char escape,
                     boolean ignoreLeadingWhitespaces, boolean ignoreTrailingWhitespaces,
                     boolean interpretUnicodeEscapes, boolean ignoreEmptyLines) {
    this(delimiter, encapsulator, commentStart, escape,
        ignoreLeadingWhitespaces, ignoreTrailingWhitespaces,
        interpretUnicodeEscapes, ignoreEmptyLines,
        DEFAULT_PRINTER_NEWLINE);
  }

  /** @deprecated will be removed in Solr 7 */
  @Deprecated
  public void setDelimiter(char delimiter) { this.delimiter = delimiter; }
  public char getDelimiter() { return this.delimiter; }

  /** @deprecated will be removed in Solr 7 */
  @Deprecated
  public void setEncapsulator(char encapsulator) { this.encapsulator = encapsulator; }
  public char getEncapsulator() { return this.encapsulator; }

  /** @deprecated will be removed in Solr 7 */
  @Deprecated
  public void setCommentStart(char commentStart) { this.commentStart = commentStart; }
  public char getCommentStart() { return this.commentStart; }
  public boolean isCommentingDisabled() { return this.commentStart == COMMENTS_DISABLED; }

  /** @deprecated will be removed in Solr 7 */
  @Deprecated
  public void setEscape(char escape) { this.escape = escape; }
  public char getEscape() { return this.escape; }

  /** @deprecated will be removed in Solr 7 */
  @Deprecated
  public void setIgnoreLeadingWhitespaces(boolean ignoreLeadingWhitespaces) {
    this.ignoreLeadingWhitespaces = ignoreLeadingWhitespaces;
  }
  public boolean getIgnoreLeadingWhitespaces() { return this.ignoreLeadingWhitespaces; }

  /** @deprecated will be removed in Solr 7 */
  @Deprecated
  public void setIgnoreTrailingWhitespaces(boolean ignoreTrailingWhitespaces) {
    this.ignoreTrailingWhitespaces = ignoreTrailingWhitespaces;
  }
  public boolean getIgnoreTrailingWhitespaces() { return this.ignoreTrailingWhitespaces; }

  /** @deprecated will be removed in Solr 7 */
  @Deprecated
  public void setUnicodeEscapeInterpretation(boolean interpretUnicodeEscapes) {
    this.interpretUnicodeEscapes = interpretUnicodeEscapes;
  }
  public boolean getUnicodeEscapeInterpretation() { return this.interpretUnicodeEscapes; }

  /** @deprecated will be removed in Solr 7 */
  @Deprecated
  public void setIgnoreEmptyLines(boolean ignoreEmptyLines) { this.ignoreEmptyLines = ignoreEmptyLines; }
  public boolean getIgnoreEmptyLines() { return this.ignoreEmptyLines; }

  /** @deprecated will be removed in Solr 7 */
  @Deprecated
  public void setPrinterNewline(String newline) {
    this.printerNewline = newline;
  }
  public String getPrinterNewline() {
    return this.printerNewline;
  }

  /** @deprecated will be removed in Solr 7 */
  @Deprecated
  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);  // impossible
    }
  }
}

/**
 * @deprecated will be removed in Solr 7
 * @lucene.internal
 */
@Deprecated
class ImmutableCSVStrategy extends CSVStrategy {
  ImmutableCSVStrategy(char delimiter, char encapsulator, char commentStart, char escape,
                       boolean ignoreLeadingWhitespaces, boolean ignoreTrailingWhitespaces,
                       boolean interpretUnicodeEscapes, boolean ignoreEmptyLines,
                       String printerNewline) {
    super(delimiter, encapsulator, commentStart, escape,
        ignoreLeadingWhitespaces, ignoreTrailingWhitespaces,
        interpretUnicodeEscapes, ignoreEmptyLines,
        printerNewline);
  }
  @Override
  public void setDelimiter(char delimiter) {
    throw new UnsupportedOperationException();
  }
  @Override
  public void setEncapsulator(char encapsulator) {
    throw new UnsupportedOperationException();
  }
  @Override
  public void setCommentStart(char commentStart) {
    throw new UnsupportedOperationException();
  }
  @Override
  public void setEscape(char escape) {
    throw new UnsupportedOperationException();
  }
  @Override
  public void setIgnoreLeadingWhitespaces(boolean ignoreLeadingWhitespaces) {
    throw new UnsupportedOperationException();
  }
  @Override
  public void setIgnoreTrailingWhitespaces(boolean ignoreTrailingWhitespaces) {
    throw new UnsupportedOperationException();
  }
  @Override
  public void setUnicodeEscapeInterpretation(boolean interpretUnicodeEscapes) {
    throw new UnsupportedOperationException();
  }
  @Override
  public void setIgnoreEmptyLines(boolean ignoreEmptyLines) {
    throw new UnsupportedOperationException();
  }
  @Override
  public void setPrinterNewline(String newline) {
    throw new UnsupportedOperationException();
  }

  /** Returns a mutable clone */
  @Override
  public Object clone() {
    return new CSVStrategy(getDelimiter(), getEncapsulator(), getCommentStart(), getEscape(),
        getIgnoreLeadingWhitespaces(), getIgnoreTrailingWhitespaces(),
        getUnicodeEscapeInterpretation(), getIgnoreEmptyLines(),
        getPrinterNewline());
  }
}
