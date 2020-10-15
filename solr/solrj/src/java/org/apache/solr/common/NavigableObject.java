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

import java.util.List;
import java.util.function.BiConsumer;

import org.apache.solr.common.util.Utils;

/**This class contains helper methods for navigating deeply nested Objects. Keep in mind that
 * it may be expensive depending on the underlying implementation. each level needs an extra lookup
 * and the lookup may be as expensive as O(log(n)) to O(n) depending on the underlying impl
 *
 */
public interface NavigableObject {
  /**Get a child object value using json path. This usually ends up in String split operations
   *  use a list of strings where performance is important
   *
   * @param path the full path to that object such as a/b/c[4]/d etc
   * @param def the default
   * @return the found value or default
   */
  default Object _get(String path, Object def) {
    Object v = Utils.getObjectByPath(this, false, path);
    return v == null ? def : v;
  }

  /**get the value as a String. useful in tests
   *
   * @param path the full path
   * @param def default value
   */
  default String _getStr(String path, String def) {
    Object v = Utils.getObjectByPath(this, false, path);
    return v == null ? def : String.valueOf(v);
  }

  /**Iterate through the entries of a navigable Object at a certain path
   * @param path the json path
   */
  default void _forEachEntry(String path, @SuppressWarnings({"rawtypes"})BiConsumer fun) {
    Utils.forEachMapEntry(this, path, fun);
  }

  /**Iterate through the entries of a navigable Object at a certain path
   * @param path the json path
   */
  default void _forEachEntry(List<String> path, @SuppressWarnings({"rawtypes"})BiConsumer fun) {
    Utils.forEachMapEntry(this, path, fun);
  }

  /**Iterate through each entry in this object
   */
  default void _forEachEntry(@SuppressWarnings({"rawtypes"})BiConsumer fun) {
    Utils.forEachMapEntry(this, fun);
  }

  /**
   * Get a child object value using json path
   *
   * @param path the full path to that object such as ["a","b","c[4]","d"] etc
   * @param def  the default
   * @return the found value or default
   */
  default Object _get(List<String> path, Object def) {
    Object v = Utils.getObjectByPath(this, false, path);
    return v == null ? def : v;
  }

  default String _getStr(List<String> path, String def) {
    Object v = Utils.getObjectByPath(this, false, path);
    return v == null ? def : String.valueOf(v);
  }

  default int _size() {
    int[] size = new int[1];
    _forEachEntry((k, v) -> size[0]++);
    return size[0];
  }
}
