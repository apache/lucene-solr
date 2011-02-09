package org.apache.lucene.queryParser.standard.config;

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

import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.processors.ParametricRangeQueryNodeProcessor;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by {@link ParametricRangeQueryNodeProcessor} processor
 * and must be defined in the {@link QueryConfigHandler}. This attribute tells
 * the processor which {@link Collator} should be used for a
 * {@link TermRangeQuery} <br/>
 * 
 * @see org.apache.lucene.queryParser.standard.config.RangeCollatorAttribute
 */
public class RangeCollatorAttributeImpl extends AttributeImpl
				implements RangeCollatorAttribute {

  private Collator rangeCollator;

  public RangeCollatorAttributeImpl() {
	  rangeCollator = null; // default value for 2.4
  }

  public void setDateResolution(Collator rangeCollator) {
    this.rangeCollator = rangeCollator;
  }

  public Collator getRangeCollator() {
    return this.rangeCollator;
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyTo(AttributeImpl target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object other) {

    if (other instanceof RangeCollatorAttributeImpl) {
    	RangeCollatorAttributeImpl rangeCollatorAttr = (RangeCollatorAttributeImpl) other;

      if (rangeCollatorAttr.rangeCollator == this.rangeCollator
          || rangeCollatorAttr.rangeCollator.equals(this.rangeCollator)) {

        return true;

      }

    }

    return false;

  }

  @Override
  public int hashCode() {
    return (this.rangeCollator == null) ? 0 : this.rangeCollator.hashCode();
  }

  @Override
  public String toString() {
    return "<rangeCollatorAttribute rangeCollator='" + this.rangeCollator
        + "'/>";
  }

}
