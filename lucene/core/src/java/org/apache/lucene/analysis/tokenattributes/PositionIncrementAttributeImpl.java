package org.apache.lucene.analysis.tokenattributes;

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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.AttributeImpl;

/** The positionIncrement determines the position of this token
 * relative to the previous Token in a {@link TokenStream}, used in phrase
 * searching.
 *
 * <p>The default value is one.
 *
 * <p>Some common uses for this are:<ul>
 *
 * <li>Set it to zero to put multiple terms in the same position.  This is
 * useful if, e.g., a word has multiple stems.  Searches for phrases
 * including either stem will match.  In this case, all but the first stem's
 * increment should be set to zero: the increment of the first instance
 * should be one.  Repeating a token with an increment of zero can also be
 * used to boost the scores of matches on that token.
 *
 * <li>Set it to values greater than one to inhibit exact phrase matches.
 * If, for example, one does not want phrases to match across removed stop
 * words, then one could build a stop word filter that removes stop words and
 * also sets the increment to the number of stop words removed before each
 * non-stop word.  Then exact phrase queries will only match when the terms
 * occur with no intervening stop words.
 *
 * </ul>
 */
public class PositionIncrementAttributeImpl extends AttributeImpl implements PositionIncrementAttribute, Cloneable {
  private int positionIncrement = 1;
  
  /** Set the position increment. The default value is one.
   *
   * @param positionIncrement the distance from the prior term
   */
  public void setPositionIncrement(int positionIncrement) {
    if (positionIncrement < 0) {
      throw new IllegalArgumentException
        ("Increment must be zero or greater: got " + positionIncrement);
    }
    this.positionIncrement = positionIncrement;
  }

  /** Returns the position increment of this Token.
   * @see #setPositionIncrement
   */
  public int getPositionIncrement() {
    return positionIncrement;
  }

  @Override
  public void clear() {
    this.positionIncrement = 1;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof PositionIncrementAttributeImpl) {
      PositionIncrementAttributeImpl _other = (PositionIncrementAttributeImpl) other;
      return positionIncrement ==  _other.positionIncrement;
    }
 
    return false;
  }

  @Override
  public int hashCode() {
    return positionIncrement;
  }
  
  @Override
  public void copyTo(AttributeImpl target) {
    PositionIncrementAttribute t = (PositionIncrementAttribute) target;
    t.setPositionIncrement(positionIncrement);
  }  
}
