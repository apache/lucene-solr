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
package org.apache.lucene.benchmark.byTask.utils;


import java.text.NumberFormat;
import java.util.Locale;

/**
 * Formatting utilities (for reports).
 */
public class Format {

  private static NumberFormat numFormat [] = { 
    NumberFormat.getInstance(Locale.ROOT), 
    NumberFormat.getInstance(Locale.ROOT),
    NumberFormat.getInstance(Locale.ROOT),
  };
  private static final String padd = "                                                 ";
  
  static {
    numFormat[0].setMaximumFractionDigits(0);
    numFormat[0].setMinimumFractionDigits(0);
    numFormat[1].setMaximumFractionDigits(1);
    numFormat[1].setMinimumFractionDigits(1);
    numFormat[2].setMaximumFractionDigits(2);
    numFormat[2].setMinimumFractionDigits(2);
  }

  /**
   * Padd a number from left.
   * @param numFracDigits number of digits in fraction part - must be 0 or 1 or 2.
   * @param f number to be formatted.
   * @param col column name (used for deciding on length).
   * @return formatted string.
   */
  public static String format(int numFracDigits, float f, String col) {
    String res = padd + numFormat[numFracDigits].format(f);
    return res.substring(res.length() - col.length());
  }

  public static String format(int numFracDigits, double f, String col) {
    String res = padd + numFormat[numFracDigits].format(f);
    return res.substring(res.length() - col.length());
  }

  /**
   * Pad a number from right.
   * @param numFracDigits number of digits in fraction part - must be 0 or 1 or 2.
   * @param f number to be formatted.
   * @param col column name (used for deciding on length).
   * @return formatted string.
   */
  public static String formatPaddRight(int numFracDigits, float f, String col) {
    String res = numFormat[numFracDigits].format(f) + padd;
    return res.substring(0, col.length());
  }

  public static String formatPaddRight(int numFracDigits, double f, String col) {
    String res = numFormat[numFracDigits].format(f) + padd;
    return res.substring(0, col.length());
  }

  /**
   * Pad a number from left.
   * @param n number to be formatted.
   * @param col column name (used for deciding on length).
   * @return formatted string.
   */
  public static String format(int n, String col) {
    String res = padd + n;
    return res.substring(res.length() - col.length());
  }

  /**
   * Pad a string from right.
   * @param s string to be formatted.
   * @param col column name (used for deciding on length).
   * @return formatted string.
   */
  public static String format(String s, String col) {
    String s1 = (s + padd);
    return s1.substring(0, Math.min(col.length(), s1.length()));
  }

  /**
   * Pad a string from left.
   * @param s string to be formatted.
   * @param col column name (used for deciding on length).
   * @return formatted string.
   */
  public static String formatPaddLeft(String s, String col) {
    String res = padd + s;
    return res.substring(res.length() - col.length());
  }

}
