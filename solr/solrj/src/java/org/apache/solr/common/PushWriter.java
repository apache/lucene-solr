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


import java.io.Closeable;
import java.io.IOException;

/**This is an interface to stream data out using a push API
 *
 */
public interface PushWriter extends Closeable {

  /**Write a Map. The map is opened in the beginning of the method
   * and closed at the end. All map entries MUST be written before this
   * method returns
   */
  void writeMap(MapWriter mw) throws IOException;

  /**Write an array. The array is opened at the beginning of this method
   * and closed at the end. All array entries must be written before this
   * method returns
   *
   */
  void writeIterator(IteratorWriter iw) throws IOException;

}
