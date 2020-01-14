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
package org.apache.lucene.analysis.tokenattributes;


import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/** Default implementation of {@link TermFrequencyAttribute}. */
public class TermFrequencyAttributeImpl extends AttributeImpl implements TermFrequencyAttribute, Cloneable {
  private int termFrequency = 1;
  
  /** Initialize this attribute with term frequency of 1 */
  public TermFrequencyAttributeImpl() {}

  @Override
  public void setTermFrequency(int termFrequency) {
    if (termFrequency < 1) {
      throw new IllegalArgumentException("Term frequency must be 1 or greater; got " + termFrequency);
    }
    this.termFrequency = termFrequency;
  }

  @Override
  public int getTermFrequency() {
    return termFrequency;
  }

  @Override
  public void clear() {
    this.termFrequency = 1;
  }
  
  @Override
  public void end() {
    this.termFrequency = 1;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof TermFrequencyAttributeImpl) {
      TermFrequencyAttributeImpl _other = (TermFrequencyAttributeImpl) other;
      return termFrequency ==  _other.termFrequency;
    }
 
    return false;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(termFrequency);
  }
  
  @Override
  public void copyTo(AttributeImpl target) {
    TermFrequencyAttribute t = (TermFrequencyAttribute) target;
    t.setTermFrequency(termFrequency);
  }  

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(TermFrequencyAttribute.class, "termFrequency", termFrequency);
  }
}
