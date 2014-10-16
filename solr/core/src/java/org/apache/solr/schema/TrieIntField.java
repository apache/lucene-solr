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

/**
 * A numeric field that can contain 32-bit signed two's complement integer values.
 *
 * <ul>
 *  <li>Min Value Allowed: -2147483648</li>
 *  <li>Max Value Allowed: 2147483647</li>
 * </ul>
 * 
 * @see Integer
 */
public class TrieIntField extends TrieField implements IntValueFieldType {
  {
    type=TrieTypes.INTEGER;
  }

  @Override
  public Object toNativeType(Object val) {
    if(val==null) return null;
    if (val instanceof Number) return ((Number) val).intValue();
    try {
      if (val instanceof String) return Integer.parseInt((String) val);
    } catch (NumberFormatException e) {
      Float v = Float.parseFloat((String) val);
      return v.intValue();
    }
    return super.toNativeType(val);
  }
}
