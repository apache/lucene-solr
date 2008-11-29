package org.apache.solr.common.params;
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


/**
 *
 *
 **/
public class TermsParams {
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
   * Optional.  The number of results to return.  If not specified, looks for {@link org.apache.solr.common.params.CommonParams#ROWS}.  If that's not specified, uses 10.
   */
  public static final String TERMS_ROWS = TERMS_PREFIX + "rows";

  public static final String TERMS_PREFIX_STR = TERMS_PREFIX + "prefix";
}
