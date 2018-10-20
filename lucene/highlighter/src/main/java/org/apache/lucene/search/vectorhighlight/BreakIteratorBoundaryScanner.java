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
package org.apache.lucene.search.vectorhighlight;

import java.text.BreakIterator;

/**
 * A {@link BoundaryScanner} implementation that uses {@link BreakIterator} to find
 * boundaries in the text.
 * @see BreakIterator
 */
public class BreakIteratorBoundaryScanner implements BoundaryScanner {
  
  final BreakIterator bi;

  public BreakIteratorBoundaryScanner(BreakIterator bi){
    this.bi = bi;
  }

  @Override
  public int findStartOffset(StringBuilder buffer, int start) {
    // avoid illegal start offset
    if( start > buffer.length() || start < 1 ) return start;
    bi.setText(buffer.substring(0, start));
    bi.last();
    return bi.previous();
  }

  @Override
  public int findEndOffset(StringBuilder buffer, int start) {
    // avoid illegal start offset
    if( start > buffer.length() || start < 0 ) return start;
    bi.setText(buffer.substring(start));
    return bi.next() + start;
  }
}
