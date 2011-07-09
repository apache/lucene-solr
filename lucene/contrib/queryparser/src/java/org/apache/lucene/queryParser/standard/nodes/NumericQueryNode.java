package org.apache.lucene.queryParser.standard.nodes;

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

import java.text.NumberFormat;
import java.util.Locale;

import org.apache.lucene.queryParser.core.nodes.FieldValuePairQueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryParser.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryParser.core.parser.EscapeQuerySyntax.Type;

public class NumericQueryNode extends QueryNodeImpl implements
    FieldValuePairQueryNode<Number> {
  
  private NumberFormat numberFormat;
  
  private CharSequence field;
  
  private Number value;
  
  public NumericQueryNode(CharSequence field, Number value,
      NumberFormat numberFormat) {
    
    super();
    
    setNumberFormat(numberFormat);
    setField(field);
    setValue(value);
    
  }
  
  public CharSequence getField() {
    return this.field;
  }
  
  public void setField(CharSequence fieldName) {
    this.field = fieldName;
  }
  
  protected CharSequence getTermEscaped(EscapeQuerySyntax escaper) {
    return escaper.escape(NumberFormat.getNumberInstance().format(this.value),
        Locale.ENGLISH, Type.NORMAL);
  }
  
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    if (isDefaultField(this.field)) {
      return getTermEscaped(escapeSyntaxParser);
    } else {
      return this.field + ":" + getTermEscaped(escapeSyntaxParser);
    }
  }
  
  public void setNumberFormat(NumberFormat format) {
    this.numberFormat = format;
  }
  
  public NumberFormat getNumberFormat() {
    return this.numberFormat;
  }
  
  public Number getValue() {
    return value;
  }
  
  public void setValue(Number value) {
    this.value = value;
  }
  
  @Override
  public String toString() {
    return "<numeric field='" + this.field + "' number='"
        + numberFormat.format(value) + "'/>";
  }
  
}
