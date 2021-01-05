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
package org.apache.lucene.queryparser.flexible.core.config;

/** This class represents a field configuration. */
public class FieldConfig extends AbstractQueryConfig {

  private String fieldName;

  /**
   * Constructs a {@link FieldConfig}
   *
   * @param fieldName the field name, it must not be null
   * @throws IllegalArgumentException if the field name is null
   */
  public FieldConfig(String fieldName) {

    if (fieldName == null) {
      throw new IllegalArgumentException("field name must not be null!");
    }

    this.fieldName = fieldName;
  }

  /**
   * Returns the field name this configuration represents.
   *
   * @return the field name
   */
  public String getField() {
    return this.fieldName;
  }

  @Override
  public String toString() {
    return "<fieldconfig name=\""
        + this.fieldName
        + "\" configurations=\""
        + super.toString()
        + "\"/>";
  }
}
