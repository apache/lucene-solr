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
 * This is a simplified version of org.apache.lucene.analysis.miscellaneous.LimitTokenOffsetFilter
 * to prevent a dependency on analysis-common.jar.
 */
final class LimitTokenOffsetFilter extends TokenFilter {

  private final OffsetAttribute offsetAttrib = addAttribute(OffsetAttribute.class);
  private int maxStartOffset;

  LimitTokenOffsetFilter(TokenStream input, int maxStartOffset) {
    super(input);
    if (maxStartOffset < 0) {
      throw new IllegalArgumentException("maxStartOffset must be >= zero");
    }
    this.maxStartOffset = maxStartOffset;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }
    if (offsetAttrib.startOffset() <= maxStartOffset) {
      return true;
    }
    return false;
  }
}
