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
package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.text.NumberFormat;
import java.util.Locale;

import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldValuePairQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax.Type;
import org.apache.lucene.queryparser.flexible.standard.config.NumericConfig;

/**
 * This query node represents a field query that holds a numeric value. It is
 * similar to {@link FieldQueryNode}, however the {@link #getValue()} returns a
 * {@link Number}.
 * 
 * @see NumericConfig
 */
public class NumericQueryNode extends QueryNodeImpl implements
    FieldValuePairQueryNode<Number> {
  
  private NumberFormat numberFormat;
  
  private CharSequence field;
  
  private Number value;
  
  /**
   * Creates a {@link NumericQueryNode} object using the given field,
   * {@link Number} value and {@link NumberFormat} used to convert the value to
   * {@link String}.
   * 
   * @param field the field associated with this query node
   * @param value the value hold by this node
   * @param numberFormat the {@link NumberFormat} used to convert the value to {@link String}
   */
  public NumericQueryNode(CharSequence field, Number value,
      NumberFormat numberFormat) {
    
    super();
    
    setNumberFormat(numberFormat);
    setField(field);
    setValue(value);
    
  }
  
  /**
   * Returns the field associated with this node.
   * 
   * @return the field associated with this node
   */
  @Override
  public CharSequence getField() {
    return this.field;
  }
  
  /**
   * Sets the field associated with this node.
   * 
   * @param fieldName the field associated with this node
   */
  @Override
  public void setField(CharSequence fieldName) {
    this.field = fieldName;
  }
  
  /**
   * This method is used to get the value converted to {@link String} and
   * escaped using the given {@link EscapeQuerySyntax}.
   * 
   * @param escaper the {@link EscapeQuerySyntax} used to escape the value {@link String}
   * 
   * @return the value converte to {@link String} and escaped
   */
  protected CharSequence getTermEscaped(EscapeQuerySyntax escaper) {
    return escaper.escape(numberFormat.format(this.value),
        Locale.ROOT, Type.NORMAL);
  }
  
  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    if (isDefaultField(this.field)) {
      return getTermEscaped(escapeSyntaxParser);
    } else {
      return this.field + ":" + getTermEscaped(escapeSyntaxParser);
    }
  }
  
  /**
   * Sets the {@link NumberFormat} used to convert the value to {@link String}.
   * 
   * @param format the {@link NumberFormat} used to convert the value to {@link String}
   */
  public void setNumberFormat(NumberFormat format) {
    this.numberFormat = format;
  }
  
  /**
   * Returns the {@link NumberFormat} used to convert the value to {@link String}.
   * 
   * @return the {@link NumberFormat} used to convert the value to {@link String}
   */
  public NumberFormat getNumberFormat() {
    return this.numberFormat;
  }
  
  /**
   * Returns the numeric value as {@link Number}.
   * 
   * @return the numeric value
   */
  @Override
  public Number getValue() {
    return value;
  }
  
  /**
   * Sets the numeric value.
   * 
   * @param value the numeric value
   */
  @Override
  public void setValue(Number value) {
    this.value = value;
  }
  
  @Override
  public String toString() {
    return "<numeric field='" + this.field + "' number='"
        + numberFormat.format(value) + "'/>";
  }
  
}
