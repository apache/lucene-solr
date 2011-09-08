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
import org.apache.lucene.queryParser.standard.processors.GroupQueryNodeProcessor;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by {@link GroupQueryNodeProcessor} processor and must
 * be defined in the {@link QueryConfigHandler}. This attribute tells the
 * processor which is the default boolean operator when no operator is defined
 * between terms. <br/>
 * 
 * @see org.apache.lucene.queryParser.standard.config.DefaultOperatorAttribute
 * 
 * @deprecated
 * 
 */
@Deprecated
public class DefaultOperatorAttributeImpl extends AttributeImpl
				implements DefaultOperatorAttribute, ConfigAttribute {

  private static final long serialVersionUID = -6804760312723049526L;
  
  private AbstractQueryConfig config;

  { enableBackwards = false; }
  
  public DefaultOperatorAttributeImpl() {
    // empty constructor
  }

  public void setOperator(Operator operator) {

    if (operator == null) {
      throw new IllegalArgumentException("default operator cannot be null!");
    }

    org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.Operator newOperator;
    
    if (operator == Operator.AND) {
      newOperator = org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.Operator.AND;
    } else {
      newOperator = org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.Operator.OR;
    }
    
    config.set(ConfigurationKeys.DEFAULT_OPERATOR, newOperator);

  }

  public Operator getOperator() {
    org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.Operator newOperator = config.get(ConfigurationKeys.DEFAULT_OPERATOR, org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.Operator.OR);
    Operator oldOperator;
    
    if (newOperator == org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.Operator.AND) {
      oldOperator = Operator.AND;
    } else {
      oldOperator = Operator.OR;
    }
    
    return oldOperator;
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

    if (other instanceof DefaultOperatorAttributeImpl) {
    	DefaultOperatorAttributeImpl defaultOperatorAttr = (DefaultOperatorAttributeImpl) other;

      if (defaultOperatorAttr.getOperator() == this.getOperator()) {
        return true;

      }

    }

    return false;

  }

  @Override
  public int hashCode() {
    return getOperator().hashCode() * 31;
  }

  @Override
  public String toString() {
    return "<defaultOperatorAttribute operator=" + getOperator().name() + "/>";
  }
  
  public void setQueryConfigHandler(AbstractQueryConfig config) {
    this.config = config;
    
    if (!config.has(ConfigurationKeys.DEFAULT_OPERATOR)) {
      setOperator(Operator.OR); 
     }
    
  }

}
