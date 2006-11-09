package org.apache.lucene.index;

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

/** Extends <code>TermFreqVector</code> to provide additional information about
 *  positions in which each of the terms is found. A TermPositionVector not necessarily
 * contains both positions and offsets, but at least one of these arrays exists.
 */
public interface TermPositionVector extends TermFreqVector {
  
    /** Returns an array of positions in which the term is found.
     *  Terms are identified by the index at which its number appears in the
     *  term String array obtained from the <code>indexOf</code> method.
     *  May return null if positions have not been stored.
     */
    public int[] getTermPositions(int index);
  
    /**
     * Returns an array of TermVectorOffsetInfo in which the term is found.
     * May return null if offsets have not been stored.
     * 
     * @see org.apache.lucene.analysis.Token
     * 
     * @param index The position in the array to get the offsets from
     * @return An array of TermVectorOffsetInfo objects or the empty list
     */ 
    public TermVectorOffsetInfo [] getOffsets(int index);
}