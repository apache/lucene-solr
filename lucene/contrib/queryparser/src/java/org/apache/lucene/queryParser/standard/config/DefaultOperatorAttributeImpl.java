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

import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.processors.GroupQueryNodeProcessor;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by {@link GroupQueryNodeProcessor} processor and must
 * be defined in the {@link QueryConfigHandler}. This attribute tells the
 * processor which is the default boolean operator when no operator is defined
 * between terms. <br/>
 * 
 * @see org.apache.lucene.queryParser.standard.config.DefaultOperatorAttribute
 */
public class DefaultOperatorAttributeImpl extends AttributeImpl
				implements DefaultOperatorAttribute {

  private Operator operator = Operator.OR;

  public DefaultOperatorAttributeImpl() {
    // empty constructor
  }

  public void setOperator(Operator operator) {

    if (operator == null) {
      throw new IllegalArgumentException("default operator cannot be null!");
    }

    this.operator = operator;

  }

  public Operator getOperator() {
    return this.operator;
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
    return "<defaultOperatorAttribute operator=" + this.operator.name() + "/>";
  }

}
