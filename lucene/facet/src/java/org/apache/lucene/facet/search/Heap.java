package org.apache.lucene.facet.search;

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

/** 
 * Declares an interface for heap (and heap alike) structures, 
 * handling a given type T
 * 
 * @lucene.experimental
 */
public interface Heap<T> {
  /**
   * Get and remove the top of the Heap <BR>
   * NOTE: Once {@link #pop()} is called no other {@link #add(Object)} or
   * {@link #insertWithOverflow(Object)} should be called.
   */
  public T pop();
  
  /** Get (But not remove) the top of the Heap */ 
  public T top();
  
  /**
   * Insert a new value, returning the overflowen object <br>
   * NOTE: This method should not be called after invoking {@link #pop()}
   */
  public T insertWithOverflow(T value);
  
  /** 
   * Add a new value to the heap, return the new top(). <br>
   * Some implementations may choose to not implement this functionality. 
   * In such a case <code>null</code> should be returned. <BR> 
   * NOTE: This method should not be called after invoking {@link #pop()}
   */
  public T add(T frn);
  
  /** Clear the heap */ 
  public void clear();
  
  /** Return the amount of objects currently in the heap */
  public int size();
}
