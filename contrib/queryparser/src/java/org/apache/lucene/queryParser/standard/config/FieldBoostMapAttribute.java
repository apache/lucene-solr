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

import java.util.Map;

import org.apache.lucene.queryParser.core.config.FieldConfig;
import org.apache.lucene.util.Attribute;

/**
 * This attribute enables the user to define a default boost per field.
 * it's used by {@link FieldBoostMapFCListener#buildFieldConfig(FieldConfig)}
 */
public interface FieldBoostMapAttribute extends Attribute {
  /**
   * @param boosts a mapping from field name to its default boost
   */
  public void setFieldBoostMap(Map<CharSequence, Float> boosts);
  public Map<CharSequence, Float> getFieldBoostMap();
}
