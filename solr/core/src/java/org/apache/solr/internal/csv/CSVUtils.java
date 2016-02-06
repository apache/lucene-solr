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

import java.io.StringWriter;
import java.io.StringReader;
import java.io.IOException;

/**
 * Utility methods for dealing with CSV files
 */
public class CSVUtils {

    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static final String[][] EMPTY_DOUBLE_STRING_ARRAY = new String[0][0];

    /**
     * <p><code>CSVUtils</code> instances should NOT be constructed in
     * standard programming. 
     *
     * <p>This constructor is public to permit tools that require a JavaBean
     * instance to operate.</p>
     */
    public CSVUtils() {
    }
  
    /**
     * Converts an array of string values into a single CSV line. All
     * <code>null</code> values are converted to the string <code>"null"</code>,
     * all strings equal to <code>"null"</code> will additionally get quotes
     * around.
     *
     * @param values the value array
     * @return the CSV string, will be an empty string if the length of the
     * value array is 0
     */
    public static String printLine(String[] values, CSVStrategy strategy) {
        // set up a CSVUtils
        StringWriter stringWriter = new StringWriter();
        CSVPrinter csvPrinter = new CSVPrinter(stringWriter, strategy);
  
        // check for null values an "null" as strings and convert them
        // into the strings "null" and "\"null\""
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                values[i] = "null";
            } else if (values[i].equals("null")) {
                values[i] = "\"null\"";
            }
        }
  
        // convert to CSV
        try {
          csvPrinter.println(values);
        } catch (IOException e) {
          // should not happen with StringWriter
        }
        // as the resulting string has \r\n at the end, we will trim that away
        return stringWriter.toString().trim();
    }
  
  // ======================================================
  //  static parsers
  // ======================================================
  
  /**
   * Parses the given String according to the default {@link CSVStrategy}.
   * 
   * @param s CSV String to be parsed.
   * @return parsed String matrix (which is never null)
   * @throws IOException in case of error
   */
  public static String[][] parse(String s) throws IOException {
    if (s == null) {
      throw new IllegalArgumentException("Null argument not allowed.");
    }
    String[][] result = (new CSVParser(new StringReader(s))).getAllValues();
    if (result == null) {
      // since CSVStrategy ignores empty lines an empty array is returned
      // (i.e. not "result = new String[][] {{""}};")
      result = EMPTY_DOUBLE_STRING_ARRAY;
    }
    return result;
  }
  
  /**
   * Parses the first line only according to the default {@link CSVStrategy}.
   * 
   * Parsing empty string will be handled as valid records containing zero
   * elements, so the following property holds: parseLine("").length == 0.
   * 
   * @param s CSV String to be parsed.
   * @return parsed String vector (which is never null)
   * @throws IOException in case of error
   */
  public static String[] parseLine(String s) throws IOException {
    if (s == null) {
      throw new IllegalArgumentException("Null argument not allowed.");
    }
    // uh,jh: make sure that parseLine("").length == 0
    if (s.length() == 0) {
      return EMPTY_STRING_ARRAY;
    }
    return (new CSVParser(new StringReader(s))).getLine();
  }
  
}
