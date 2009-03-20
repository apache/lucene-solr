package org.apache.lucene.document;

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.MockRAMDirectory;

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

/**
 * Tests {@link Document} class.
 *
 *
 * @version $Id$
 */
public class TestBinaryDocument extends LuceneTestCase
{

  String binaryValStored = "this text will be stored as a byte array in the index";
  String binaryValCompressed = "this text will be also stored and compressed as a byte array in the index";
  
  public void testBinaryFieldInIndex()
    throws Exception
  {
    Fieldable binaryFldStored = new Field("binaryStored", binaryValStored.getBytes(), Field.Store.YES);
    Fieldable binaryFldCompressed = new Field("binaryCompressed", binaryValCompressed.getBytes(), Field.Store.COMPRESS);
    Fieldable stringFldStored = new Field("stringStored", binaryValStored, Field.Store.YES, Field.Index.NO, Field.TermVector.NO);
    Fieldable stringFldCompressed = new Field("stringCompressed", binaryValCompressed, Field.Store.COMPRESS, Field.Index.NO, Field.TermVector.NO);

    try {
      // binary fields with store off are not allowed
      new Field("fail", binaryValCompressed.getBytes(), Field.Store.NO);
      fail();
    }
    catch (IllegalArgumentException iae) {
      ;
    }
    
    Document doc = new Document();
    
    doc.add(binaryFldStored);
    doc.add(binaryFldCompressed);
    
    doc.add(stringFldStored);
    doc.add(stringFldCompressed);

    /** test for field count */
    assertEquals(4, doc.fields.size());
    
    /** add the doc to a ram index */
    MockRAMDirectory dir = new MockRAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.addDocument(doc);
    writer.close();
    
    /** open a reader and fetch the document */ 
    IndexReader reader = IndexReader.open(dir);
    Document docFromReader = reader.document(0);
    assertTrue(docFromReader != null);
    
    /** fetch the binary stored field and compare it's content with the original one */
    String binaryFldStoredTest = new String(docFromReader.getBinaryValue("binaryStored"));
    assertTrue(binaryFldStoredTest.equals(binaryValStored));
    
    /** fetch the binary compressed field and compare it's content with the original one */
    String binaryFldCompressedTest = new String(docFromReader.getBinaryValue("binaryCompressed"));
    assertTrue(binaryFldCompressedTest.equals(binaryValCompressed));
    
    /** fetch the string field and compare it's content with the original one */
    String stringFldStoredTest = docFromReader.get("stringStored");
    assertTrue(stringFldStoredTest.equals(binaryValStored));
    
    /** fetch the compressed string field and compare it's content with the original one */
    String stringFldCompressedTest = docFromReader.get("stringCompressed");
    assertTrue(stringFldCompressedTest.equals(binaryValCompressed));

    /** delete the document from index */
    reader.deleteDocument(0);
    assertEquals(0, reader.numDocs());
    
    reader.close();
    dir.close();
  }
  
  public void testCompressionTools()
    throws Exception
  {
    Fieldable binaryFldCompressed = new Field("binaryCompressed", CompressionTools.compress(binaryValCompressed.getBytes()), Field.Store.YES);
    Fieldable stringFldCompressed = new Field("stringCompressed", CompressionTools.compressString(binaryValCompressed), Field.Store.YES);
    
    Document doc = new Document();
    
    doc.add(binaryFldCompressed);
    doc.add(stringFldCompressed);
    
    /** add the doc to a ram index */
    MockRAMDirectory dir = new MockRAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.addDocument(doc);
    writer.close();
    
    /** open a reader and fetch the document */ 
    IndexReader reader = IndexReader.open(dir);
    Document docFromReader = reader.document(0);
    assertTrue(docFromReader != null);
    
    /** fetch the binary compressed field and compare it's content with the original one */
    String binaryFldCompressedTest = new String(CompressionTools.decompress(docFromReader.getBinaryValue("binaryCompressed")));
    assertTrue(binaryFldCompressedTest.equals(binaryValCompressed));
    assertTrue(CompressionTools.decompressString(docFromReader.getBinaryValue("stringCompressed")).equals(binaryValCompressed));

    reader.close();
    dir.close();
  }
}
