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

import org.apache.lucene.queryParser.core.config.FieldConfig;
import org.apache.lucene.queryParser.standard.processors.MultiFieldQueryNodeProcessor;
import org.apache.lucene.util.Attribute;

/**
 * This attribute is used by {@link MultiFieldQueryNodeProcessor} processor and
 * it should be defined in a {@link FieldConfig}. This processor uses this
 * attribute to define which boost a specific field should have when none is
 * defined to it. <br/>
 * <br/>
 * 
 * @deprecated
 * 
 */
@Deprecated
public interface BoostAttribute extends Attribute {
  public void setBoost(float boost);
  public float getBoost();
}
