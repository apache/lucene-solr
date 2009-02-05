package org.apache.lucene.index;

/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.Reader;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * @version $Id$
 */

class RepeatingTokenStream extends TokenStream {
  public int num;
  TermAttribute termAtt;
  String value;

   public RepeatingTokenStream(String val) {
     this.value = val;
     this.termAtt = (TermAttribute) addAttribute(TermAttribute.class);
   }

   public boolean incrementToken() throws IOException {
     num--;
     if (num >= 0) {
       termAtt.setTermBuffer(value);
       return true;
     }
     return false;
   }
}


public class TestTermdocPerf extends LuceneTestCase {

  void addDocs(Directory dir, final int ndocs, String field, final String val, final int maxTF, final float percentDocs) throws IOException {
    final Random random = newRandom();
    final RepeatingTokenStream ts = new RepeatingTokenStream(val);

    Analyzer analyzer = new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        if (random.nextFloat() < percentDocs) ts.num = random.nextInt(maxTF)+1;
        else ts.num=0;
        return ts;
      }
    };

    Document doc = new Document();
    doc.add(new Field(field,val, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
    IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(100);
    writer.setMergeFactor(100);

    for (int i=0; i<ndocs; i++) {
      writer.addDocument(doc);
    }

    writer.optimize();
    writer.close();
  }


  public int doTest(int iter, int ndocs, int maxTF, float percentDocs) throws IOException {
    Directory dir = new RAMDirectory();

    long start = System.currentTimeMillis();
    addDocs(dir, ndocs, "foo", "val", maxTF, percentDocs);
    long end = System.currentTimeMillis();
    System.out.println("milliseconds for creation of " + ndocs + " docs = " + (end-start));

    IndexReader reader = IndexReader.open(dir);
    TermEnum tenum = reader.terms(new Term("foo","val"));
    TermDocs tdocs = reader.termDocs();

    start = System.currentTimeMillis();

    int ret=0;
    for (int i=0; i<iter; i++) {
      tdocs.seek(tenum);
      while (tdocs.next()) {
        ret += tdocs.doc();
      }
    }

    end = System.currentTimeMillis();
    System.out.println("milliseconds for " + iter + " TermDocs iteration: " + (end-start));

    return ret;
  }

  public void testTermDocPerf() throws IOException {
    // performance test for 10% of documents containing a term
    // doTest(100000, 10000,3,.1f);
  }


}
