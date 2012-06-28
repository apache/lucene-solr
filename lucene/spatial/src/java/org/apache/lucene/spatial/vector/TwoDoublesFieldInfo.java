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

package org.apache.lucene.spatial.vector;

import org.apache.lucene.spatial.SpatialFieldInfo;

/**
 * @lucene.experimental
 */
public class TwoDoublesFieldInfo implements SpatialFieldInfo {

  public static final String SUFFIX_X = "__x";
  public static final String SUFFIX_Y = "__y";

  private final String fieldName;
  private final String fieldNameX;
  private final String fieldNameY;

  public TwoDoublesFieldInfo(String fieldNamePrefix) {
    fieldName = fieldNamePrefix;
    fieldNameX = fieldNamePrefix + SUFFIX_X;
    fieldNameY = fieldNamePrefix + SUFFIX_Y;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getFieldNameX() {
    return fieldNameX;
  }

  public String getFieldNameY() {
    return fieldNameY;
  }
}
