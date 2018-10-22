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
package org.apache.solr.schema;

import java.util.Map;

/**
 * Abstract class defining shared behavior for primitive types
 * Intended to be used as base class for non-analyzed fields like
 * int, float, string, date etc, and set proper defaults for them 
 */
public abstract class PrimitiveFieldType extends FieldType {
  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    super.init(schema, args);
    if(schema.getVersion() > 1.4F &&
       // only override if it's not explicitly false
       0 == (falseProperties & OMIT_NORMS)) {
      properties |= OMIT_NORMS;
    }
  }

  @Override
  protected void checkSupportsDocValues() { // primitive types support DocValues
  }

  @Override
  public MultiValueSelector getDefaultMultiValueSelectorForSort(SchemaField field, boolean reverse) {
    return reverse ? MultiValueSelector.MAX : MultiValueSelector.MIN;
  }
}
