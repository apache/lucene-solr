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


import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.*;
import org.apache.lucene.store.*;

import org.junit.Test;

import java.io.IOException;

public class TestIsCurrent extends LuceneTestCase {

  private RandomIndexWriter writer;

  private Directory directory;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // initialize directory
    directory = newDirectory();
    writer = new RandomIndexWriter(random(), directory);

    // write document
    Document doc = new Document();
    doc.add(newTextField("UUID", "1", Field.Store.YES));
    writer.addDocument(doc);
    writer.commit();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    writer.close();
    directory.close();
  }

  /**
   * Failing testcase showing the trouble
   */
  @Test
  public void testDeleteByTermIsCurrent() throws IOException {

    // get reader
    DirectoryReader reader = writer.getReader();

    // assert index has a document and reader is up2date 
    assertEquals("One document should be in the index", 1, writer.getDocStats().numDocs);
    assertTrue("One document added, reader should be current", reader.isCurrent());

    // remove document
    Term idTerm = new Term("UUID", "1");
    writer.deleteDocuments(idTerm);
    writer.commit();

    // assert document has been deleted (index changed), reader is stale
    assertEquals("Document should be removed", 0, writer.getDocStats().numDocs);
    assertFalse("Reader should be stale", reader.isCurrent());

    reader.close();
  }

  /**
   * Testcase for example to show that writer.deleteAll() is working as expected
   */
  @Test
  public void testDeleteAllIsCurrent() throws IOException {

    // get reader
    DirectoryReader reader = writer.getReader();

    // assert index has a document and reader is up2date 
    assertEquals("One document should be in the index", 1, writer.getDocStats().numDocs);
    assertTrue("Document added, reader should be stale ", reader.isCurrent());

    // remove all documents
    writer.deleteAll();
    writer.commit();

    // assert document has been deleted (index changed), reader is stale
    assertEquals("Document should be removed", 0, writer.getDocStats().numDocs);
    assertFalse("Reader should be stale", reader.isCurrent());

    reader.close();
  }
}
