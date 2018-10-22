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

import java.io.Serializable;

/**
 * Represents a Enum field value, which includes integer value (indicating the sort order) and string (displayed) value.
 * Note: this class has a natural ordering that is inconsistent with equals
 */

public final class EnumFieldValue implements Serializable, Comparable<EnumFieldValue> {
  private final Integer intValue;
  private final String stringValue;

  @Override
  public int hashCode() {
    int result = intValue != null ? intValue.hashCode() : 0;
    result = 31 * result + (stringValue != null ? stringValue.hashCode() : 0);
    return result;
  }

  public EnumFieldValue(Integer intValue, String stringValue) {
    this.intValue = intValue;
    this.stringValue = stringValue;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (!(obj instanceof EnumFieldValue))
      return false;

    EnumFieldValue otherEnumFieldValue = (EnumFieldValue) obj;
    return equalsIntegers(intValue, otherEnumFieldValue.intValue) && equalStrings(stringValue, otherEnumFieldValue.stringValue);
  }

  /**
   * @return string (displayed) value
   */
  @Override
  public String toString() {
    return stringValue;
  }

  /**
   * @return integer value (indicating the sort order)
   */
  public Integer toInt() {
    return intValue;
  }

  @Override
  public int compareTo(EnumFieldValue o) {
    if (o == null)
      return 1;
    return compareIntegers(intValue, o.intValue);
  }

  private boolean equalStrings(String str1, String str2) {
    if ((str1 == null) && (str2 == null))
      return true;

    if (str1 == null)
      return false;

    if (str2 == null)
      return false;

    return str1.equals(str2);
  }

  private boolean equalsIntegers(Integer int1, Integer int2) {
    if ((int1 == null) && (int2 == null))
      return true;

    if (int1 == null)
      return false;

    if (int2 == null)
      return false;

    return int1.equals(int2);
  }

  private int compareIntegers(Integer int1, Integer int2) {
    if ((int1 == null) && (int2 == null))
      return 0;

    if (int1 == null)
      return -1;

    if (int2 == null)
      return 1;

    return int1.compareTo(int2);
  }
}


