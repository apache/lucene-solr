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

package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;

public class TestProtectedTermFilter extends BaseTokenStreamTestCase {

  public void testBasic() throws IOException {

    CannedTokenStream cts = new CannedTokenStream(
        new Token("Alice", 1, 0, 5),
        new Token("Bob", 1, 6, 9),
        new Token("Clara", 1, 10, 15),
        new Token("David", 1, 16, 21)
    );

    CharArraySet protectedTerms = new CharArraySet(5, true);
    protectedTerms.add("bob");

    TokenStream ts = new ProtectedTermFilter(protectedTerms, cts, LowerCaseFilter::new);
    assertTokenStreamContents(ts, new String[]{ "alice", "Bob", "clara", "david" });

  }

}
