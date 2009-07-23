package org.apache.lucene.analysis.miscellaneous;

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

import junit.framework.TestCase;

import java.io.IOException;

import org.apache.lucene.analysis.Token;

public class TestSingleTokenTokenFilter extends TestCase {

  public void test() throws IOException {
    Token token = new Token();

    SingleTokenTokenStream ts = new SingleTokenTokenStream(token);

    final Token reusableToken = new Token();
    assertEquals(token, ts.next(reusableToken));
    assertNull(ts.next(reusableToken));
  }
}
