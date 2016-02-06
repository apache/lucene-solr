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
package org.apache.lucene.queryparser.flexible.spans;

import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.util.Attribute;

/**
 * This attribute is used by the {@link UniqueFieldQueryNodeProcessor}
 * processor. It holds a value that defines which is the unique field name that
 * should be set in every {@link FieldableNode}.
 * 
 * @see UniqueFieldQueryNodeProcessor
 */
public interface UniqueFieldAttribute extends Attribute {
  public void setUniqueField(CharSequence uniqueField);

  public CharSequence getUniqueField();
}
