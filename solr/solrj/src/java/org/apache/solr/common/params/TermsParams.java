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
package org.apache.solr.common.params;

import java.util.regex.Pattern;

import static org.apache.solr.common.params.CommonParams.SORT;

/**
 *
 *
 **/
public interface TermsParams {
  /**
   * The component name.  Set to true to turn on the TermsComponent
   */
  public static final String TERMS = "terms";

  /**
   * Used for building up the other terms
   */
  public static final String TERMS_PREFIX = TERMS + ".";

  /**
   * Required.  Specify the field to look up terms in.
   */
  public static final String TERMS_FIELD = TERMS_PREFIX + "fl";

  /**
   * Optional. The list of terms to be retrieved.
   */
  public static final String TERMS_LIST = TERMS_PREFIX + "list";

  /**
   * Optional. If true, also returns index-level statistics, such as numDocs.
   */
  public static final String TERMS_STATS = TERMS_PREFIX + "stats";

  /**
   * Optional. If true, also returns terms' total term frequency.
   */
  public static final String TERMS_TTF = TERMS_PREFIX + "ttf";

  /**
   * Optional.  The lower bound term to start at.  The TermEnum will start at the next term after this term in the dictionary.
   *
   * If not specified, the empty string is used
   */
  public static final String TERMS_LOWER = TERMS_PREFIX + "lower";

  /**
   * Optional.  The term to stop at.
   *
   * @see #TERMS_UPPER_INCLUSIVE
   */
  public static final String TERMS_UPPER = TERMS_PREFIX + "upper";
  /**
   * Optional.  If true, include the upper bound term in the results.  False by default.
   */
  public static final String TERMS_UPPER_INCLUSIVE = TERMS_PREFIX + "upper.incl";

  /**
   * Optional.  If true, include the lower bound term in the results, otherwise skip to the next one.  True by default.
   */
  public static final String TERMS_LOWER_INCLUSIVE = TERMS_PREFIX + "lower.incl";

  /**
   * Optional.  The number of results to return.  If not specified, looks for {@link org.apache.solr.common.params.CommonParams#ROWS}.  If that's not specified, uses {@link org.apache.solr.common.params.CommonParams#ROWS_DEFAULT}.
   */
  public static final String TERMS_LIMIT = TERMS_PREFIX + "limit";

  public static final String TERMS_PREFIX_STR = TERMS_PREFIX + "prefix";

  public static final String TERMS_REGEXP_STR = TERMS_PREFIX + "regex";

  public static final String TERMS_REGEXP_FLAG = TERMS_REGEXP_STR + ".flag";

  public static enum TermsRegexpFlag {
      UNIX_LINES(Pattern.UNIX_LINES),
      CASE_INSENSITIVE(Pattern.CASE_INSENSITIVE),
      COMMENTS(Pattern.COMMENTS),
      MULTILINE(Pattern.MULTILINE),
      LITERAL(Pattern.LITERAL),
      DOTALL(Pattern.DOTALL),
      UNICODE_CASE(Pattern.UNICODE_CASE),
      CANON_EQ(Pattern.CANON_EQ);

      int value;

      TermsRegexpFlag(int value) {
          this.value = value;
      }

      public int getValue() {
          return value;
      }
  }

  /**
   * Optional.  The minimum value of docFreq to be returned.  1 by default
   */
  public static final String TERMS_MINCOUNT = TERMS_PREFIX + "mincount";

  /**
   * Optional.  The maximum value of docFreq to be returned.  -1 by default means no boundary
   */
  String TERMS_MAXCOUNT = TERMS_PREFIX + "maxcount";

  /**
   * Optional.  If true, return the raw characters of the indexed term, regardless of if it is readable.
   * For instance, the index form of numeric numbers is not human readable.  The default is false.
   */
  String TERMS_RAW = TERMS_PREFIX + "raw";

  /**
   * Optional.  If sorting by frequency is enabled.  Defaults to sorting by count.
   */
  String TERMS_SORT = TERMS_PREFIX + SORT;
  
  String TERMS_SORT_COUNT = "count";
  String TERMS_SORT_INDEX = "index";
}

