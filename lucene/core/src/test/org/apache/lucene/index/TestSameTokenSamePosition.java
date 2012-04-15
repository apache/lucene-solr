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

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestSameTokenSamePosition extends LuceneTestCase {

  /**
   * Attempt to reproduce an assertion error that happens
   * only with the trunk version around April 2011.
   */
  public void test() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new TextField("eng", new BugReproTokenStream()));
    riw.addDocument(doc);
    riw.close();
    dir.close();
  }
  
  /**
   * Same as the above, but with more docs
   */
  public void testMoreDocs() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new TextField("eng", new BugReproTokenStream()));
      riw.addDocument(doc);
    }
    riw.close();
    dir.close();
  }
}

final class BugReproTokenStream extends TokenStream {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final int tokenCount = 4;
  private int nextTokenIndex = 0;
  private final String terms[] = new String[]{"six", "six", "drunken", "drunken"};
  private final int starts[] = new int[]{0, 0, 4, 4};
  private final int ends[] = new int[]{3, 3, 11, 11};
  private final int incs[] = new int[]{1, 0, 1, 0};

  @Override
  public boolean incrementToken() throws IOException {
    if (nextTokenIndex < tokenCount) {
      termAtt.setEmpty().append(terms[nextTokenIndex]);
      offsetAtt.setOffset(starts[nextTokenIndex], ends[nextTokenIndex]);
      posIncAtt.setPositionIncrement(incs[nextTokenIndex]);
      nextTokenIndex++;
      return true;			
    } else {
      return false;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    this.nextTokenIndex = 0;
  }
}
