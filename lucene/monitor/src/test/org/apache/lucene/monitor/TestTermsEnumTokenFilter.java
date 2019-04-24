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

package org.apache.lucene.monitor;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.LuceneTestCase;

public class TestTermsEnumTokenFilter extends LuceneTestCase {

  final class LeapfrogTokenFilter extends TokenFilter {

    final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

    LeapfrogTokenFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      posIncAtt.setPositionIncrement(100000000);
      return input.incrementToken();
    }
  }

  public void testPosIncAttributeOverflow() throws IOException {

    final BytesRef foo = new BytesRef("foo");
    final BytesRef bar = new BytesRef("bar");

    BytesRefIterator terms = new BytesRefIterator() {

      long count = 1000;

      @Override
      public BytesRef next() throws IOException {
        if (count-- > 100)
          return foo;
        if (count-- > 0)
          return bar;
        return null;
      }
    };

    try (TokenStream ts = new LeapfrogTokenFilter(new TermsEnumTokenStream(terms))) {
      while (ts.incrementToken()) {
        // This tight loop will throw an exception if clearAttributes() is not called
        // by TermsEnumTokenStream.  See issue #46
      }
    }
  }

}
