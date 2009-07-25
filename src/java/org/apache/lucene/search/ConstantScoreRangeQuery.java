package org.apache.lucene.search;

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

import java.text.Collator;

/**
 * A range query that returns a constant score equal to its boost for
 * all documents in the exclusive range of terms.
 *
 * <p>It does not have an upper bound on the number of clauses covered in the range.
 *
 * <p>This query matches the documents looking for terms that fall into the
 * supplied range according to {@link String#compareTo(String)}. It is not intended
 * for numerical ranges, use {@link NumericRangeQuery} instead.
 *
 * <p>This query is hardwired to {@link MultiTermQuery#CONSTANT_SCORE_AUTO_REWRITE_DEFAULT}.
 * If you want to change this, use {@link TermRangeQuery} instead.
 *
 * @deprecated Use {@link TermRangeQuery} for term ranges or
 * {@link NumericRangeQuery} for numeric ranges instead.
 * This class will be removed in Lucene 3.0.
 * @version $Id$
 */
public class ConstantScoreRangeQuery extends TermRangeQuery
{

  public ConstantScoreRangeQuery(String fieldName, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper)
  {
    super(fieldName, lowerVal, upperVal, includeLower, includeUpper);
    rewriteMethod = CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
  }

  public ConstantScoreRangeQuery(String fieldName, String lowerVal,
                                 String upperVal, boolean includeLower,
                                 boolean includeUpper, Collator collator) {
    super(fieldName, lowerVal, upperVal, includeLower, includeUpper, collator);
    rewriteMethod = CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
  }

  public String getLowerVal() {
    return getLowerTerm();
  }

  public String getUpperVal() {
    return getUpperTerm();
  }

  /** Changes of mode are not supported by this class (fixed to constant score rewrite mode) */
  public void setRewriteMethod(RewriteMethod method) {
    throw new UnsupportedOperationException("Use TermRangeQuery instead to change the rewrite method.");
  }
}
