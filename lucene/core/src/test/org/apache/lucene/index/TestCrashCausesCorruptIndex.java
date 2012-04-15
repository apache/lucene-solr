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

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestCrashCausesCorruptIndex extends LuceneTestCase  {

  File path;
    
  /**
   * LUCENE-3627: This test fails.
   * 
   * @throws Exception
   */
  public void testCrashCorruptsIndexing() throws Exception {
    path = _TestUtil.getTempDir("testCrashCorruptsIndexing");
        
    indexAndCrashOnCreateOutputSegments2();

    searchForFleas(2);

    indexAfterRestart();
        
    searchForFleas(3);
  }
    
  /**
   * index 1 document and commit.
   * prepare for crashing.
   * index 1 more document, and upon commit, creation of segments_2 will crash.
   * 
   * @throws IOException
   */
  private void indexAndCrashOnCreateOutputSegments2() throws IOException {
    Directory realDirectory = FSDirectory.open(path);
    CrashAfterCreateOutput crashAfterCreateOutput = new CrashAfterCreateOutput(realDirectory);
            
    // NOTE: cannot use RandomIndexWriter because it
    // sometimes commits:
    IndexWriter indexWriter = new IndexWriter(crashAfterCreateOutput,
                                              newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
            
    indexWriter.addDocument(getDocument());
    // writes segments_1:
    indexWriter.commit();
            
    crashAfterCreateOutput.setCrashAfterCreateOutput("segments_2");
    indexWriter.addDocument(getDocument());
    try {
      // tries to write segments_2 but hits fake exc:
      indexWriter.commit();
      fail("should have hit CrashingException");
    } catch (CrashingException e) {
      // expected
    }
    // writes segments_3
    indexWriter.close();
    assertFalse(realDirectory.fileExists("segments_2"));
    crashAfterCreateOutput.close();
  }
    
  /**
   * Attempts to index another 1 document.
   * 
   * @throws IOException
   */
  private void indexAfterRestart() throws IOException {
    Directory realDirectory = newFSDirectory(path);
            
    // LUCENE-3627 (before the fix): this line fails because
    // it doesn't know what to do with the created but empty
    // segments_2 file
    IndexWriter indexWriter = new IndexWriter(realDirectory,
                                              newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
            
    // currently the test fails above.
    // however, to test the fix, the following lines should pass as well.
    indexWriter.addDocument(getDocument());
    indexWriter.close();
    assertFalse(realDirectory.fileExists("segments_2"));
    realDirectory.close();
  }
    
  /**
   * Run an example search.
   * 
   * @throws IOException
   * @throws ParseException
   */
  private void searchForFleas(final int expectedTotalHits) throws IOException {
    Directory realDirectory = newFSDirectory(path);
    IndexReader indexReader = IndexReader.open(realDirectory);
    IndexSearcher indexSearcher = newSearcher(indexReader);
    TopDocs topDocs = indexSearcher.search(new TermQuery(new Term(TEXT_FIELD, "fleas")), 10);
    assertNotNull(topDocs);
    assertEquals(expectedTotalHits, topDocs.totalHits);
    indexReader.close();
    realDirectory.close();
  }

  private static final String TEXT_FIELD = "text";
    
  /**
   * Gets a document with content "my dog has fleas".
   */
  private Document getDocument() {
    Document document = new Document();
    document.add(newField(TEXT_FIELD, "my dog has fleas", TextField.TYPE_UNSTORED));
    return document;
  }
    
  /**
   * The marker RuntimeException that we use in lieu of an
   * actual machine crash.
   */
  private static class CrashingException extends RuntimeException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public CrashingException(String msg) {
      super(msg);
    }
  }
    
  /**
   * This test class provides direct access to "simulating" a crash right after 
   * realDirectory.createOutput(..) has been called on a certain specified name.
   */
  private static class CrashAfterCreateOutput extends Directory {
        
    private Directory realDirectory;
    private String crashAfterCreateOutput;

    public CrashAfterCreateOutput(Directory realDirectory) throws IOException {
      this.realDirectory = realDirectory;
      setLockFactory(realDirectory.getLockFactory());
    }
        
    public void setCrashAfterCreateOutput(String name) {
      this.crashAfterCreateOutput = name;
    }
        
    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
      realDirectory.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexOutput createOutput(String name, IOContext cxt) throws IOException {
      IndexOutput indexOutput = realDirectory.createOutput(name, cxt);
      if (null != crashAfterCreateOutput && name.equals(crashAfterCreateOutput)) {
        // CRASH!
        indexOutput.close();
        if (VERBOSE) {
          System.out.println("TEST: now crash");
          new Throwable().printStackTrace(System.out);
        }
        throw new CrashingException("crashAfterCreateOutput "+crashAfterCreateOutput);
      }
      return indexOutput;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteFile(String name) throws IOException {
      realDirectory.deleteFile(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean fileExists(String name) throws IOException {
      return realDirectory.fileExists(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long fileLength(String name) throws IOException {
      return realDirectory.fileLength(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] listAll() throws IOException {
      return realDirectory.listAll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexInput openInput(String name, IOContext cxt) throws IOException {
      return realDirectory.openInput(name, cxt);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sync(Collection<String> names) throws IOException {
      realDirectory.sync(names);
    }
  }
}
