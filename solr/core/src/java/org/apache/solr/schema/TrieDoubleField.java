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

package org.apache.solr.schema;

/**
 * A numeric field that can contain double-precision 64-bit IEEE 754 floating 
 * point values.
 *
 * <ul>
 *  <li>Min Value Allowed: 4.9E-324</li>
 *  <li>Max Value Allowed: 1.7976931348623157E308</li>
 * </ul>
 *
 * <b>NOTE:</b> The behavior of this class when given values of 
 * {@link Double#NaN}, {@link Double#NEGATIVE_INFINITY}, or 
 * {@link Double#POSITIVE_INFINITY} is undefined.
 * 
 * @see Double
 * @see <a href="http://java.sun.com/docs/books/jls/third_edition/html/typesValues.html#4.2.3">Java Language Specification, s4.2.3</a>
 */
public class TrieDoubleField extends TrieField {
  {
    type=TrieTypes.DOUBLE;
  }
}
