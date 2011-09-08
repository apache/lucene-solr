package org.apache.lucene.analysis;

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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;

public class TestCachingTokenFilter extends BaseTokenStreamTestCase {
  private String[] tokens = new String[] {"term1", "term2", "term3", "term2"};
  
  public void testCaching() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir);

    Document doc = new Document();
    TokenStream stream = new TokenStream() {
      private int index = 0;
      private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      private OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
      
      @Override
      public boolean incrementToken() throws IOException {
        if (index == tokens.length) {
          return false;
        } else {
          clearAttributes();
          termAtt.append(tokens[index++]);
          offsetAtt.setOffset(0,0);
          return true;
        }        
      }
      
    };
    
    stream = new CachingTokenFilter(stream);
    
    doc.add(new Field("preanalyzed", stream, TermVector.NO));
    
    // 1) we consume all tokens twice before we add the doc to the index
    checkTokens(stream);
    stream.reset();  
    checkTokens(stream);
    
    // 2) now add the document to the index and verify if all tokens are indexed
    //    don't reset the stream here, the DocumentWriter should do that implicitly
    writer.addDocument(doc);
    
    IndexReader reader = writer.getReader();
    TermPositions termPositions = reader.termPositions(new Term("preanalyzed", "term1"));
    assertTrue(termPositions.next());
    assertEquals(1, termPositions.freq());
    assertEquals(0, termPositions.nextPosition());

    termPositions.seek(new Term("preanalyzed", "term2"));
    assertTrue(termPositions.next());
    assertEquals(2, termPositions.freq());
    assertEquals(1, termPositions.nextPosition());
    assertEquals(3, termPositions.nextPosition());
    
    termPositions.seek(new Term("preanalyzed", "term3"));
    assertTrue(termPositions.next());
    assertEquals(1, termPositions.freq());
    assertEquals(2, termPositions.nextPosition());
    reader.close();
    writer.close();
    // 3) reset stream and consume tokens again
    stream.reset();
    checkTokens(stream);
    dir.close();
  }
  
  private void checkTokens(TokenStream stream) throws IOException {
    int count = 0;
    
    CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
    while (stream.incrementToken()) {
      assertTrue(count < tokens.length);
      assertEquals(tokens[count], termAtt.toString());
      count++;
    }
    
    assertEquals(tokens.length, count);
  }
}
