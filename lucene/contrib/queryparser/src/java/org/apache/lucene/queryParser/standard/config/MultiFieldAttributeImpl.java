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

import java.util.Arrays;

import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.processors.MultiFieldQueryNodeProcessor;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by {@link MultiFieldQueryNodeProcessor} processor and
 * must be defined in the {@link QueryConfigHandler}. This attribute tells the
 * processor to which fields the terms in the query should be expanded. <br/>
 * 
 * @see org.apache.lucene.queryParser.standard.config.MultiFieldAttribute
 */
public class MultiFieldAttributeImpl extends AttributeImpl
				implements MultiFieldAttribute {

  private CharSequence[] fields;

  public MultiFieldAttributeImpl() {
    // empty constructor
  }

  public void setFields(CharSequence[] fields) {
    this.fields = fields;
  }

  public CharSequence[] getFields() {
    return this.fields;
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

    if (other instanceof MultiFieldAttributeImpl) {
    	MultiFieldAttributeImpl fieldsAttr = (MultiFieldAttributeImpl) other;

      return Arrays.equals(this.fields, fieldsAttr.fields);

    }

    return false;

  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.fields);
  }

  @Override
  public String toString() {
    return "<fieldsAttribute fields=" + Arrays.toString(this.fields) + "/>";
  }

}
