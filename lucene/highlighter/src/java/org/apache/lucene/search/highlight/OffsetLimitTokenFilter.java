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
package org.apache.lucene.search.highlight;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/**
 * This TokenFilter limits the number of tokens while indexing by adding up the
 * current offset.
 */
public final class OffsetLimitTokenFilter extends TokenFilter {
  
  private int offsetCount;
  private OffsetAttribute offsetAttrib = getAttribute(OffsetAttribute.class);
  private int offsetLimit;
  
  public OffsetLimitTokenFilter(TokenStream input, int offsetLimit) {
    super(input);
    this.offsetLimit = offsetLimit;
  }
  
  @Override
  public boolean incrementToken() throws IOException {
    if (offsetCount < offsetLimit && input.incrementToken()) {
      int offsetLength = offsetAttrib.endOffset() - offsetAttrib.startOffset();
      offsetCount += offsetLength;
      return true;
    }
    return false;
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    offsetCount = 0;
  }
  
}
