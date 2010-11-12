package org.apache.lucene.util;

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

/**
 * Helper methods to ease implementing {@link Object#toString()}.
 */
public final class ToStringUtils {

  private ToStringUtils() {} // no instance

  /**
   * for printing boost only if not 1.0
   */
  public static String boost(float boost) {
    if (boost != 1.0f) {
      return "^" + Float.toString(boost);
    } else return "";
  }

  public static void byteArray(StringBuilder buffer, byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      buffer.append("b[").append(i).append("]=").append(bytes[i]);
      if (i < bytes.length - 1) {
        buffer.append(',');
      }

    }
  }

}
