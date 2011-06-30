package org.apache.lucene.store;

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
 * <p>A FlushInfo provides information required for a FLUSH context and other optimization operations.
 *  It is used as part of an {@link IOContext} in case of FLUSH context.</p>
 */


public class FlushInfo {
  
  public final int numDocs;
  
  public final long estimatedSegmentSize;
  
  /**
   * <p>Creates a new {@link FlushInfo} instance from
   * the values required for a FLUSH {@link IOContext} context.
   * 
   * These values are only estimates and are not the actual values.
   * 
   */
  
  public FlushInfo(int numDocs, long estimatedSegmentSize) {
    this.numDocs = numDocs;
    this.estimatedSegmentSize = estimatedSegmentSize;
  }

}
