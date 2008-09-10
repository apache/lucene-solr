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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;


public class TestIndexWriterMerging extends LuceneTestCase
{

  /**
   * Tests that index merging (specifically addIndexes()) doesn't
   * change the index order of documents.
   */
  public void testLucene() throws IOException
  {

    int num=100;

    Directory indexA = new MockRAMDirectory();
    Directory indexB = new MockRAMDirectory();

    fillIndex(indexA, 0, num);
    boolean fail = verifyIndex(indexA, 0);
    if (fail)
    {
      fail("Index a is invalid");
    }

    fillIndex(indexB, num, num);
    fail = verifyIndex(indexB, num);
    if (fail)
    {
      fail("Index b is invalid");
    }

    Directory merged = new MockRAMDirectory();

    IndexWriter writer = new IndexWriter(merged, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMergeFactor(2);

    writer.addIndexes(new Directory[]{indexA, indexB});
    writer.close();

    fail = verifyIndex(merged, 0);
    merged.close();

    assertFalse("The merged index is invalid", fail);
  }

  private boolean verifyIndex(Directory directory, int startAt) throws IOException
  {
    boolean fail = false;
    IndexReader reader = IndexReader.open(directory);

    int max = reader.maxDoc();
    for (int i = 0; i < max; i++)
    {
      Document temp = reader.document(i);
      //System.out.println("doc "+i+"="+temp.getField("count").stringValue());
      //compare the index doc number to the value that it should be
      if (!temp.getField("count").stringValue().equals((i + startAt) + ""))
      {
        fail = true;
        System.out.println("Document " + (i + startAt) + " is returning document " + temp.getField("count").stringValue());
      }
    }
    reader.close();
    return fail;
  }

  private void fillIndex(Directory dir, int start, int numDocs) throws IOException
  {

    IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMergeFactor(2);
    writer.setMaxBufferedDocs(2);

    for (int i = start; i < (start + numDocs); i++)
    {
      Document temp = new Document();
      temp.add(new Field("count", (""+i), Field.Store.YES, Field.Index.NOT_ANALYZED));

      writer.addDocument(temp);
    }
    writer.close();
  }
}
