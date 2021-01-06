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
package org.apache.lucene.analysis.cn.smart;

/**
 * Internal SmartChineseAnalyzer character type constants.
 *
 * @lucene.experimental
 */
public class CharType {

  /** Punctuation Characters */
  public static final int DELIMITER = 0;

  /** Letters */
  public static final int LETTER = 1;

  /** Numeric Digits */
  public static final int DIGIT = 2;

  /** Han Ideographs */
  public static final int HANZI = 3;

  /** Characters that act as a space */
  public static final int SPACE_LIKE = 4;

  /** Full-Width letters */
  public static final int FULLWIDTH_LETTER = 5;

  /** Full-Width alphanumeric characters */
  public static final int FULLWIDTH_DIGIT = 6;

  /** Other (not fitting any of the other categories) */
  public static final int OTHER = 7;

  /** Surrogate character */
  public static final int SURROGATE = 8;
}
