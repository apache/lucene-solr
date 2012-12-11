package org.apache.lucene.queryparser.flexible.spans;

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

import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by the {@link UniqueFieldQueryNodeProcessor}
 * processor. It holds a value that defines which is the unique field name that
 * should be set in every {@link FieldableNode}.<br/>
 * <br/>
 * 
 * @see UniqueFieldQueryNodeProcessor
 */
public class UniqueFieldAttributeImpl extends AttributeImpl implements
    UniqueFieldAttribute {

  private CharSequence uniqueField;

  public UniqueFieldAttributeImpl() {
    clear();
  }

  @Override
  public void clear() {
    this.uniqueField = "";
  }

  @Override
  public void setUniqueField(CharSequence uniqueField) {
    this.uniqueField = uniqueField;
  }

  @Override
  public CharSequence getUniqueField() {
    return this.uniqueField;
  }

  @Override
  public void copyTo(AttributeImpl target) {

    if (!(target instanceof UniqueFieldAttributeImpl)) {
      throw new IllegalArgumentException(
          "cannot copy the values from attribute UniqueFieldAttribute to an instance of "
              + target.getClass().getName());
    }

    UniqueFieldAttributeImpl uniqueFieldAttr = (UniqueFieldAttributeImpl) target;
    uniqueFieldAttr.uniqueField = uniqueField.toString();

  }

  @Override
  public boolean equals(Object other) {

    if (other instanceof UniqueFieldAttributeImpl) {

      return ((UniqueFieldAttributeImpl) other).uniqueField
          .equals(this.uniqueField);

    }

    return false;

  }

  @Override
  public int hashCode() {
    return this.uniqueField.hashCode();
  }

  @Override
  public String toString() {
    return "<uniqueField uniqueField='" + this.uniqueField + "'/>";
  }

}
