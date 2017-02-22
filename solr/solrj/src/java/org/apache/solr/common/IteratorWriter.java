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


import java.io.IOException;

/**
 * Interface to help do push writing to an array
 */
public interface IteratorWriter {
  /**
   * @param iw after this method returns , the EntryWriter Object is invalid
   *          Do not hold a reference to this object
   */
  void writeIter(ItemWriter iw) throws IOException;

  interface ItemWriter {
    /**The item could be any supported type
     */
    ItemWriter add(Object o) throws IOException;

    default ItemWriter add(int v) throws IOException {
      add((Integer) v);
      return this;
    }


    default ItemWriter add(long v) throws IOException {
      add((Long) v);
      return this;
    }


    default ItemWriter add(float v) throws IOException {
      add((Float) v);
      return this;
    }

    default ItemWriter add(double v) throws IOException {
      add((Double) v);
      return this;
    }

    default ItemWriter add(boolean v) throws IOException {
      add((Boolean) v);
      return this;
    }
  }
}
