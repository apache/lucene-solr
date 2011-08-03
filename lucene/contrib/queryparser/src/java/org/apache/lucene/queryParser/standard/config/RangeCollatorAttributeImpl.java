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

import org.apache.lucene.queryParser.core.config.AbstractQueryConfig;
import org.apache.lucene.queryParser.core.config.ConfigAttribute;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
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
 * 
 * @deprecated
 * 
 */
@Deprecated
public class RangeCollatorAttributeImpl extends AttributeImpl
				implements RangeCollatorAttribute, ConfigAttribute {

  private static final long serialVersionUID = -6804360312723049526L;
  
  private AbstractQueryConfig config;

  { enableBackwards = false; }
  
  public RangeCollatorAttributeImpl() {}

  public void setDateResolution(Collator rangeCollator) {
    config.set(ConfigurationKeys.RANGE_COLLATOR, rangeCollator);
  }

  public Collator getRangeCollator() {
    return config.get(ConfigurationKeys.RANGE_COLLATOR);
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
    	
    	Collator thisCollator = getRangeCollator();
    	Collator otherCollator = rangeCollatorAttr.getRangeCollator();

      if (otherCollator == thisCollator
          || otherCollator.equals(thisCollator)) {

        return true;

      }

    }

    return false;

  }

  @Override
  public int hashCode() {
    Collator collator = getRangeCollator();
    return (collator == null) ? 0 : collator.hashCode();
  }

  @Override
  public String toString() {
    return "<rangeCollatorAttribute rangeCollator='" + getRangeCollator()
        + "'/>";
  }
  
  public void setQueryConfigHandler(AbstractQueryConfig config) {
    this.config = config;
  }

}
