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
import java.util.Collections;
import java.util.HashMap;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.TokenFilterFactory;

public class TestAsciiFoldingFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testMultiTermAnalysis() throws IOException {
    TokenFilterFactory factory = new ASCIIFoldingFilterFactory(Collections.emptyMap());
    TokenStream stream = new CannedTokenStream(new Token("Été", 0, 3));
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] { "Ete" });

    stream = new CannedTokenStream(new Token("Été", 0, 3));
    stream = factory.normalize(stream);
    assertTokenStreamContents(stream, new String[] { "Ete" });

    factory = new ASCIIFoldingFilterFactory(new HashMap<>(Collections.singletonMap("preserveOriginal", "true")));
    stream = new CannedTokenStream(new Token("Été", 0, 3));
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] { "Ete", "Été" });

    stream = new CannedTokenStream(new Token("Été", 0, 3));
    stream = factory.normalize(stream);
    assertTokenStreamContents(stream, new String[] { "Ete" });
  }

}
