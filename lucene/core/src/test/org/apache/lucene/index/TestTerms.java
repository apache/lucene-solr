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
package org.apache.lucene.index;

import org.apache.lucene.analysis.CannedBinaryTokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestTerms extends LuceneTestCase {

  public void testTermMinMaxBasic() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "a b c cc ddd", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    Terms terms = MultiTerms.getTerms(r, "field");
    assertEquals(new BytesRef("a"), terms.getMin());
    assertEquals(new BytesRef("ddd"), terms.getMax());
    r.close();
    w.close();
    dir.close();
  }

  public void testTermMinMaxRandom() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(100);
    BytesRef minTerm = null;
    BytesRef maxTerm = null;
    for(int i=0;i<numDocs;i++ ){
      Document doc = new Document();
      Field field = new TextField("field", "", Field.Store.NO);
      doc.add(field);
      //System.out.println("  doc " + i);
      CannedBinaryTokenStream.BinaryToken[] tokens = new CannedBinaryTokenStream.BinaryToken[atLeast(10)];
      for(int j=0;j<tokens.length;j++) {
        byte[] bytes = new byte[TestUtil.nextInt(random(), 1, 20)];
        random().nextBytes(bytes);
        BytesRef tokenBytes = new BytesRef(bytes);
        //System.out.println("    token " + tokenBytes);
        if (minTerm == null || tokenBytes.compareTo(minTerm) < 0) {
          //System.out.println("      ** new min");
          minTerm = tokenBytes;
        }
        if (maxTerm == null || tokenBytes.compareTo(maxTerm) > 0) {
          //System.out.println("      ** new max");
          maxTerm = tokenBytes;
        }
        tokens[j] = new CannedBinaryTokenStream.BinaryToken(tokenBytes);
      }
      field.setTokenStream(new CannedBinaryTokenStream(tokens));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    Terms terms = MultiTerms.getTerms(r, "field");
    assertEquals(minTerm, terms.getMin());
    assertEquals(maxTerm, terms.getMax());
    
    r.close();
    w.close();
    dir.close();
  }
}
