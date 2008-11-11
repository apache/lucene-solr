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
 * all documents in the range.
 * <p>
 * It does not have an upper bound on the number of clauses covered in the range.
 * <p>
 * If an endpoint is null, it is said to be "open".
 * Either or both endpoints may be open.  Open endpoints may not be exclusive
 * (you can't select all but the first or last term without explicitly specifying the term to exclude.)
 *
 * @deprecated Please use {@link RangeQuery}, and call
 * {@link RangeQuery#setConstantScoreRewrite}, instead.
 * @version $Id$
 */
public class ConstantScoreRangeQuery extends RangeQuery
{

  public ConstantScoreRangeQuery(String fieldName, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper)
  {
    super(fieldName, lowerVal, upperVal, includeLower, includeUpper);
    this.constantScoreRewrite = true;
  }

  public ConstantScoreRangeQuery(String fieldName, String lowerVal,
                                 String upperVal, boolean includeLower,
                                 boolean includeUpper, Collator collator) {
    super(fieldName, lowerVal, upperVal, includeLower, includeUpper, collator);
    this.constantScoreRewrite = true;
  }

  public String getLowerVal() {
    return getLowerTermText();
  }

  public String getUpperVal() {
    return getUpperTermText();
  }
}
