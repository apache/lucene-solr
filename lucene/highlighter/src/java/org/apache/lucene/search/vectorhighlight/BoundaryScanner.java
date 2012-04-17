package org.apache.lucene.search.vectorhighlight;

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
 *
 */
public interface BoundaryScanner {

  /**
   * Scan backward to find end offset.
   * @param buffer scanned object
   * @param start start offset to begin
   * @return the found start offset
   */
  public int findStartOffset( StringBuilder buffer, int start );

  /**
   * Scan forward to find start offset.
   * @param buffer scanned object
   * @param start start offset to begin
   * @return the found end offset
   */
  public int findEndOffset( StringBuilder buffer, int start );
}
