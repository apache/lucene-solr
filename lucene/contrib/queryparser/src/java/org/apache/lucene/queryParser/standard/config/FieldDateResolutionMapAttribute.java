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

import org.apache.lucene.document.DateTools;
import org.apache.lucene.util.Attribute;

/**
 * This attribute enables the user to define a default DateResolution per field.
 * it's used by {@link FieldDateResolutionFCListener#buildFieldConfig(org.apache.lucene.queryParser.core.config.FieldConfig)}
 * 
 * @deprecated
 * 
 */
@Deprecated
public interface FieldDateResolutionMapAttribute extends Attribute {
  /**
   * @param dateRes a mapping from field name to its default boost
   */
  public void setFieldDateResolutionMap(Map<CharSequence, DateTools.Resolution> dateRes);
  public Map<CharSequence, DateTools.Resolution> getFieldDateResolutionMap();
}
