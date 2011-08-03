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

import org.apache.lucene.queryParser.core.config.AbstractQueryConfig;
import org.apache.lucene.queryParser.core.config.ConfigAttribute;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryParser.standard.processors.PhraseSlopQueryNodeProcessor;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by {@link PhraseSlopQueryNodeProcessor} processor and
 * must be defined in the {@link QueryConfigHandler}. This attribute tells the
 * processor what is the default phrase slop when no slop is defined in a
 * phrase. <br/>
 * 
 * @see org.apache.lucene.queryParser.standard.config.FuzzyAttribute
 * 
 * @deprecated
 * 
 */
@Deprecated
public class FuzzyAttributeImpl extends AttributeImpl implements
    FuzzyAttribute, ConfigAttribute {

  private static final long serialVersionUID = -2104763012527049527L;

  private AbstractQueryConfig config;

  {
    enableBackwards = false;
  }

  public FuzzyAttributeImpl() {
    // empty constructor
  }

  public void setPrefixLength(int prefixLength) {
    getFuzzyConfig().setPrefixLength(prefixLength);
  }

  public int getPrefixLength() {
    return getFuzzyConfig().getPrefixLength();
  }

  public void setFuzzyMinSimilarity(float minSimilarity) {
    getFuzzyConfig().setMinSimilarity(minSimilarity);
  }
  
  private FuzzyConfig getFuzzyConfig() {
    FuzzyConfig fuzzyConfig = config.get(ConfigurationKeys.FUZZY_CONFIG);
    
    if (fuzzyConfig == null) {
      fuzzyConfig = new FuzzyConfig();
      config.set(ConfigurationKeys.FUZZY_CONFIG, fuzzyConfig);
    }
    
    return fuzzyConfig;
    
  }

  public float getFuzzyMinSimilarity() {
    return getFuzzyConfig().getMinSimilarity();
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

    if (other instanceof FuzzyAttributeImpl
        && ((FuzzyAttributeImpl) other).getPrefixLength() == getPrefixLength()) {

      return true;

    }

    return false;

  }

  @Override
  public int hashCode() {
    return Integer.valueOf(getPrefixLength()).hashCode();
  }

  @Override
  public String toString() {
    FuzzyConfig fuzzyConfig = getFuzzyConfig();
    return "<fuzzyAttribute prefixLength=" + fuzzyConfig.getPrefixLength()
        + ",minSimilarity=" + fuzzyConfig.getMinSimilarity() + "/>";
  }

  public void setQueryConfigHandler(AbstractQueryConfig config) {
    this.config = config;

    if (!config.has(ConfigurationKeys.FUZZY_CONFIG)) {
      config.set(ConfigurationKeys.FUZZY_CONFIG, new FuzzyConfig());
    }

  }

}
