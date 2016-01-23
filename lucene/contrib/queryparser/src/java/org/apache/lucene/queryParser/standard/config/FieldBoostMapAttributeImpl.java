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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.queryParser.core.config.FieldConfig;
import org.apache.lucene.queryParser.standard.processors.MultiFieldQueryNodeProcessor;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by {@link MultiFieldQueryNodeProcessor} processor and
 * it should be defined in a {@link FieldConfig}. This processor uses this
 * attribute to define which boost a specific field should have when none is
 * defined to it. <br/>
 * <br/>
 * 
 * @see org.apache.lucene.queryParser.standard.config.BoostAttribute
 */
public class FieldBoostMapAttributeImpl extends AttributeImpl 
				implements FieldBoostMapAttribute {

  private static final long serialVersionUID = -2104763012523049527L;

  private Map<CharSequence, Float> boosts = new LinkedHashMap<CharSequence, Float>();
  

  public FieldBoostMapAttributeImpl() {
    // empty constructor
  }

  public void setFieldBoostMap(Map<CharSequence, Float> boosts) {
    this.boosts = boosts;
  }
  
  public Map<CharSequence, Float> getFieldBoostMap() {
    return this.boosts;
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

    if (other instanceof FieldBoostMapAttributeImpl
        && ((FieldBoostMapAttributeImpl) other).boosts.equals(this.boosts) ) {

      return true;

    }

    return false;

  }

  @Override
  public int hashCode() {
    final int prime = 97;
    if (this.boosts != null) 
      return this.boosts.hashCode() * prime;
    else 
      return Float.valueOf(prime).hashCode();
  }

  @Override
  public String toString() {
    return "<fieldBoostMapAttribute map=" + this.boosts + "/>";
  }

}
