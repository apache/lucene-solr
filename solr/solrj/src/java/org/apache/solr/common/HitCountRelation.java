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
package org.apache.solr.common;

/**
 * Used to express the relation between the value returned as "numFound" and the actual
 * number of hits for a query
 */
public enum HitCountRelation {
  // Ordinals are used for serialization, don't move without compatibility considerations
  /**
   * The number of hits for the query are equal to the number reported in "numFound"
   */
  EQUAL_TO,
  /**
   * The number of hits for the query are greater than or equal to the number reported in "numFound"
   */
  GREATER_THAN_OR_EQUAL_TO;
  
  @Override
  public String toString() {
    return this.name();
  }
  
  public static HitCountRelation forOrdinal(int idx) {
    if (idx < 0 || idx >= HitCountRelation.values().length) {
      throw new IllegalArgumentException("No element for index " + idx);
    }
    return HitCountRelation.values()[idx];
    
  }
}
