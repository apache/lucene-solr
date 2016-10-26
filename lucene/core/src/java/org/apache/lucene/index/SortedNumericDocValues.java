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
package org.apache.lucene.index;

import java.io.IOException;


/**
 * A list of per-document numeric values, sorted 
 * according to {@link Long#compare(long, long)}.
 */
public abstract class SortedNumericDocValues extends DocValuesIterator {
  
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected SortedNumericDocValues() {}

  /** 
   * Iterates to the next value in the current document.  Do not call this more than {@link #docValueCount} times
   * for the document.
   */
  public abstract long nextValue() throws IOException;
  
  /** 
   * Retrieves the number of values for the current document.  This must always
   * be greater than zero.
   * It is illegal to call this method after {@link #advanceExact(int)}
   * returned {@code false}.
   */
  public abstract int docValueCount();
}
