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

import java.io.IOException;
import java.io.Writer;

/**
 * Print values as a comma separated list.
 */
public class CSVPrinter {

  /** The place that the values get written. */
  protected final Writer out;
  protected final CSVStrategy strategy;

  /** True if we just began a new line. */
  protected boolean newLine = true;

  protected char[] buf = new char[0];  // temporary buffer

  /**
   * Create a printer that will print values to the given
   * stream following the CSVStrategy.
   *
   * Currently, only a pure encapsulation strategy or a pure escaping strategy
   * is supported.  Hybrid strategies (encapsulation and escaping with a different character) are not supported.
   *
   * @param out stream to which to print.
   * @param strategy describes the CSV variation.
   */
  public CSVPrinter(Writer out, CSVStrategy strategy) {
    this.out = out;
    this.strategy = strategy==null ? CSVStrategy.DEFAULT_STRATEGY : strategy;
  }
  
  // ======================================================
  //  printing implementation
  // ======================================================

  /**
   * Output a blank line
   */
  public void println() throws IOException {
    out.write(strategy.getPrinterNewline());
    newLine = true;
  }

  public void flush() throws IOException {
    out.flush();
  }


  /**
   * Print a single line of comma separated values.
   * The values will be quoted if needed.  Quotes and
   * newLine characters will be escaped.
   *
   * @param values values to be outputted.
   */
  public void println(String[] values) throws IOException {
    for (int i = 0; i < values.length; i++) {
      print(values[i]);
    }
    println();
  }


  /**
   * Put a comment among the comma separated values.
   * Comments will always begin on a new line and occupy a
   * least one full line. The character specified to star
   * comments and a space will be inserted at the beginning of
   * each new line in the comment.
   *
   * @param comment the comment to output
   */
  @SuppressWarnings({"fallthrough"})
  public void printlnComment(String comment) throws IOException {
    if(this.strategy.isCommentingDisabled()) {
        return;
    }
    if (!newLine) {
      println();
    }
    out.write(this.strategy.getCommentStart());
    out.write(' ');
    for (int i = 0; i < comment.length(); i++) {
      char c = comment.charAt(i);
      switch (c) {
        case '\r' :
          if (i + 1 < comment.length() && comment.charAt(i + 1) == '\n') {
            i++;
          }
          // break intentionally excluded.
        case '\n' :
          println();
          out.write(this.strategy.getCommentStart());
          out.write(' ');
          break;
        default :
          out.write(c);
          break;
      }
    }
    println();
  }


  public void print(char[] value, int offset, int len, boolean checkForEscape) throws IOException {
    if (!checkForEscape) {
      printSep();
      out.write(value, offset, len);
      return;
    }

    if (strategy.getEncapsulator() != CSVStrategy.ENCAPSULATOR_DISABLED) {
      printAndEncapsulate(value, offset, len);
    } else if (strategy.getEscape() != CSVStrategy.ESCAPE_DISABLED) {
      printAndEscape(value, offset, len);
    } else {
      printSep();
      out.write(value, offset, len);
    }
  }

  void printSep() throws IOException {
    if (newLine) {
      newLine = false;
    } else {
      out.write(this.strategy.getDelimiter());
    }
  }

  void printAndEscape(char[] value, int offset, int len) throws IOException {
    int start = offset;
    int pos = offset;
    int end = offset + len;

    printSep();

    char delim = this.strategy.getDelimiter();
    char escape = this.strategy.getEscape();

    while (pos < end) {
      char c = value[pos];
      if (c == '\r' || c=='\n' || c==delim || c==escape) {
        // write out segment up until this char
        int l = pos-start;
        if (l>0) {
          out.write(value, start, l);
        }
        if (c=='\n') c='n';
        else if (c=='\r') c='r';

        out.write(escape);
        out.write(c);

        start = pos+1; // start on the current char after this one
      }

      pos++;
    }

    // write last segment
    int l = pos-start;
    if (l>0) {
      out.write(value, start, l);      
    }
  }

  void printAndEncapsulate(char[] value, int offset, int len) throws IOException {
    boolean first = newLine;  // is this the first value on this line?
    boolean quote = false;
    int start = offset;
    int pos = offset;
    int end = offset + len;

    printSep();    

    char delim = this.strategy.getDelimiter();
    char encapsulator = this.strategy.getEncapsulator();

    if (len <= 0) {
      // always quote an empty token that is the first
      // on the line, as it may be the only thing on the
      // line. If it were not quoted in that case,
      // an empty line has no tokens.
      if (first) {
        quote = true;
      }
    } else {
      char c = value[pos];

      // Hmmm, where did this rule come from?
      if (first
          && (c < '0'
          || (c > '9' && c < 'A')
          || (c > 'Z' && c < 'a')
          || (c > 'z'))) {
        quote = true;
      // } else if (c == ' ' || c == '\f' || c == '\t') {
      } else if (c <= '#') {
        // Some other chars at the start of a value caused the parser to fail, so for now
        // encapsulate if we start in anything less than '#'.  We are being conservative
        // by including the default comment char too.
        quote = true;
      } else {
        while (pos < end) {
          c = value[pos];
          if (c=='\n' || c=='\r' || c==encapsulator || c==delim) {
            quote = true;
            break;
          }
          pos++;
        }

        if (!quote) {
          pos = end-1;
          c = value[pos];
          // if (c == ' ' || c == '\f' || c == '\t') {
          // Some other chars at the end caused the parser to fail, so for now
          // encapsulate if we end in anything less than ' '
          if (c <= ' ') {
            quote = true;
          }
        }
      }
    }

    if (!quote) {
      // no encapsulation needed - write out the original value
      out.write(value, offset, len);
      return;
    }

    // we hit something that needed encapsulation
    out.write(encapsulator);

    // Pick up where we left off: pos should be positioned on the first character that caused
    // the need for encapsulation.
    while (pos<end) {
      char c = value[pos];
      if (c==encapsulator) {
        // write out the chunk up until this point

        // add 1 to the length to write out the encapsulator also
        out.write(value, start, pos-start+1);
        // put the next starting position on the encapsulator so we will
        // write it out again with the next string (effectively doubling it)
        start = pos;
      }
      pos++;
    }

    // write the last segment
    out.write(value, start, pos-start);
    out.write(encapsulator);    
  }

  /**
   * Print the string as the next value on the line. The value
   * will be escaped or encapsulated as needed if checkForEscape==true
   *
   * @param value value to be outputted.
   */
  public void print(String value, boolean checkForEscape) throws IOException {
    if (!checkForEscape) {
      // write directly from string
      printSep();
      out.write(value);
      return;
    }

    if (buf.length < value.length()) {
      buf = new char[value.length()];
    }

    value.getChars(0, value.length(), buf, 0);
    print(buf, 0, value.length(), checkForEscape);
  }

  /**
   * Print the string as the next value on the line. The value
   * will be escaped or encapsulated as needed.
   *
   * @param value value to be outputted.
   */
  public void print(String value) throws IOException {
    print(value, true);   
  }
}
