package org.apache.solr.util;

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

import java.util.Locale;

/**
 * Enum for modeling the elements of a (nested) pivot entry as expressed in a NamedList
 */
public enum PivotListEntry {
  
  FIELD(0), 
  VALUE(1),
  COUNT(2),
  PIVOT(3);
  
  // we could just use the ordinal(), but safer to be very explicit
  private final int index;
  
  private PivotListEntry(int index) {
    this.index = index;
  }
  
  /**
   * Case-insensitive lookup of PivotListEntry by name
   * @see #getName
   */
  public static PivotListEntry get(String name) {
    return PivotListEntry.valueOf(name.toUpperCase(Locale.ROOT));
  }

  /**
   * Name of this entry when used in response
   * @see #get
   */
  public String getName() {
    return name().toLowerCase(Locale.ROOT);
  }
  
  /**
   * Indec of this entry when used in response
   */
  public int getIndex() {
    return index;
  }

}
