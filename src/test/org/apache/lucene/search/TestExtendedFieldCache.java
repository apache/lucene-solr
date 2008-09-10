package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class TestExtendedFieldCache extends LuceneTestCase {
  protected IndexReader reader;
  private static final int NUM_DOCS = 1000;

  public TestExtendedFieldCache(String s) {
    super(s);
  }

  protected void setUp() throws Exception {
    super.setUp();
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer= new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    long theLong = Long.MAX_VALUE;
    double theDouble = Double.MAX_VALUE;
    for (int i = 0; i < NUM_DOCS; i++){
      Document doc = new Document();
      doc.add(new Field("theLong", String.valueOf(theLong--), Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add(new Field("theDouble", String.valueOf(theDouble--), Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add(new Field("text", English.intToEnglish(i), Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    writer.close();
    reader = IndexReader.open(directory);
  }


  public void test() throws IOException {
    ExtendedFieldCache cache = new ExtendedFieldCacheImpl();
    double [] doubles = cache.getDoubles(reader, "theDouble");
    assertTrue("doubles Size: " + doubles.length + " is not: " + NUM_DOCS, doubles.length == NUM_DOCS);
    for (int i = 0; i < doubles.length; i++) {
      assertTrue(doubles[i] + " does not equal: " + (Double.MAX_VALUE - i), doubles[i] == (Double.MAX_VALUE - i));

    }
    long [] longs = cache.getLongs(reader, "theLong");
    assertTrue("longs Size: " + longs.length + " is not: " + NUM_DOCS, longs.length == NUM_DOCS);
    for (int i = 0; i < longs.length; i++) {
      assertTrue(longs[i] + " does not equal: " + (Long.MAX_VALUE - i), longs[i] == (Long.MAX_VALUE - i));

    }
  }
}