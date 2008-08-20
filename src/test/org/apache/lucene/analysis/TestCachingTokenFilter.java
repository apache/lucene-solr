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

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class TestCachingTokenFilter extends LuceneTestCase {
  private String[] tokens = new String[] {"term1", "term2", "term3", "term2"};
  
  public void testCaching() throws IOException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    TokenStream stream = new TokenStream() {
      private int index = 0;
      
      public Token next(final Token reusableToken) throws IOException {
        assert reusableToken != null;
        if (index == tokens.length) {
          return null;
        } else {
          return reusableToken.reinit(tokens[index++], 0, 0);
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
    writer.close();
    
    IndexReader reader = IndexReader.open(dir);
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
    
    // 3) reset stream and consume tokens again
    stream.reset();
    checkTokens(stream);
  }
  
  private void checkTokens(TokenStream stream) throws IOException {
    int count = 0;
    final Token reusableToken = new Token();
    for (Token nextToken = stream.next(reusableToken); nextToken != null; nextToken = stream.next(reusableToken)) {
      assertTrue(count < tokens.length);
      assertEquals(tokens[count], nextToken.term());
      count++;
    }
    
    assertEquals(tokens.length, count);
  }
}
