package org.apache.lucene.search;

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

/** Expert: Describes the score computation for document and query, and
 * can distinguish a match independent of a positive value. */
public class ComplexExplanation extends Explanation {
  private Boolean match;
  
  public ComplexExplanation() {
    super();
  }

  public ComplexExplanation(boolean match, float value, String description) {
    // NOTE: use of "boolean" instead of "Boolean" in params is conscious
    // choice to encourage clients to be specific.
    super(value, description);
    this.match = Boolean.valueOf(match);
  }

  /**
   * The match status of this explanation node.
   * @return May be null if match status is unknown
   */
  public Boolean getMatch() { return match; }
  /**
   * Sets the match status assigned to this explanation node.
   * @param match May be null if match status is unknown
   */
  public void setMatch(Boolean match) { this.match = match; }
  /**
   * Indicates whether or not this Explanation models a good match.
   *
   * <p>
   * If the match status is explicitly set (i.e.: not null) this method
   * uses it; otherwise it defers to the superclass.
   * </p>
   * @see #getMatch
   */
  @Override
  public boolean isMatch() {
    Boolean m = getMatch();
    return (null != m ? m.booleanValue() : super.isMatch());
  }

  @Override
  protected String getSummary() {
    if (null == getMatch())
      return super.getSummary();
    
    return getValue() + " = "
      + (isMatch() ? "(MATCH) " : "(NON-MATCH) ")
      + getDescription();
  }
  
}
