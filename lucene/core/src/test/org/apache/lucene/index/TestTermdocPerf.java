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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

class RepeatingTokenizer extends Tokenizer {
  
  private final Random random;
  private final float percentDocs;
  private final int maxTF;
  private int num;
  CharTermAttribute termAtt;
  String value;

   public RepeatingTokenizer(Reader reader, String val, Random random, float percentDocs, int maxTF) {
     super(reader);
     this.value = val;
     this.random = random;
     this.percentDocs = percentDocs;
     this.maxTF = maxTF;
     this.termAtt = addAttribute(CharTermAttribute.class);
   }

   @Override
   public boolean incrementToken() throws IOException {
     num--;
     if (num >= 0) {
       clearAttributes();
       termAtt.append(value);
       return true;
     }
     return false;
   }

  @Override
  public void reset() throws IOException {
    super.reset();
    if (random.nextFloat() < percentDocs) {
      num = random.nextInt(maxTF) + 1;
    } else {
      num = 0;
    }
  }
}


public class TestTermdocPerf extends LuceneTestCase {

  void addDocs(final Random random, Directory dir, final int ndocs, String field, final String val, final int maxTF, final float percentDocs) throws IOException {

    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        return new TokenStreamComponents(new RepeatingTokenizer(reader, val, random, percentDocs, maxTF));
      }
    };

    Document doc = new Document();
    
    doc.add(newStringField(field, val, Field.Store.NO));
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer).
            setOpenMode(OpenMode.CREATE).
            setMaxBufferedDocs(100).
            setMergePolicy(newLogMergePolicy(100))
    );

    for (int i=0; i<ndocs; i++) {
      writer.addDocument(doc);
    }

    writer.forceMerge(1);
    writer.close();
  }


  public int doTest(int iter, int ndocs, int maxTF, float percentDocs) throws IOException {
    Directory dir = newDirectory();

    long start = System.currentTimeMillis();
    addDocs(random(), dir, ndocs, "foo", "val", maxTF, percentDocs);
    long end = System.currentTimeMillis();
    if (VERBOSE) System.out.println("milliseconds for creation of " + ndocs + " docs = " + (end-start));

    IndexReader reader = DirectoryReader.open(dir);

    TermsEnum tenum = MultiFields.getTerms(reader, "foo").iterator(null);

    start = System.currentTimeMillis();

    int ret=0;
    DocsEnum tdocs = null;
    final Random random = new Random(random().nextLong());
    for (int i=0; i<iter; i++) {
      tenum.seekCeil(new BytesRef("val"));
      tdocs = _TestUtil.docs(random, tenum, MultiFields.getLiveDocs(reader), tdocs, DocsEnum.FLAG_NONE);
      while (tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        ret += tdocs.docID();
      }
    }

    end = System.currentTimeMillis();
    if (VERBOSE) System.out.println("milliseconds for " + iter + " TermDocs iteration: " + (end-start));

    return ret;
  }

  public void testTermDocPerf() throws IOException {
    // performance test for 10% of documents containing a term
    // doTest(100000, 10000,3,.1f);
  }


}
