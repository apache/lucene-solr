package org.apache.lucene.facet.taxonomy.lucene;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyReader;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyWriter;

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
 * This test case attempts to catch index "leaks" in LuceneTaxonomyReader/Writer,
 * i.e., cases where an index has been opened, but never closed; In that case,
 * Java would eventually collect this object and close the index, but leaving
 * the index open might nevertheless cause problems - e.g., on Windows it prevents
 * deleting it.
 */
public class TestIndexClose extends LuceneTestCase {

  @Test
  public void testLeaks() throws Exception {
    LeakChecker checker = new LeakChecker();
    Directory dir = newDirectory();
    LuceneTaxonomyWriter tw = checker.openWriter(dir);
    tw.close();
    assertEquals(0, checker.nopen());

    tw = checker.openWriter(dir);
    tw.addCategory(new CategoryPath("animal", "dog"));
    tw.close();
    assertEquals(0, checker.nopen());

    LuceneTaxonomyReader tr = checker.openReader(dir);
    tr.getPath(1);
    tr.refresh();
    tr.close();
    assertEquals(0, checker.nopen());

    tr = checker.openReader(dir);
    tw = checker.openWriter(dir);
    tw.addCategory(new CategoryPath("animal", "cat"));
    tr.refresh();
    tw.commit();
    tw.close();
    tr.refresh();
    tr.close();
    assertEquals(0, checker.nopen());

    tw = checker.openWriter(dir);
    for (int i=0; i<10000; i++) {
      tw.addCategory(new CategoryPath("number", Integer.toString(i)));
    }
    tw.close();
    assertEquals(0, checker.nopen());
    tw = checker.openWriter(dir);
    for (int i=0; i<10000; i++) {
      tw.addCategory(new CategoryPath("number", Integer.toString(i*2)));
    }
    tw.close();
    assertEquals(0, checker.nopen());
    dir.close();
  }

  private static class LeakChecker {
    int ireader=0;
    Set<Integer> openReaders = new HashSet<Integer>();

    int iwriter=0;
    Set<Integer> openWriters = new HashSet<Integer>();

    LeakChecker() { }
    
    public LuceneTaxonomyWriter openWriter(Directory dir) throws CorruptIndexException, LockObtainFailedException, IOException {
      return new InstrumentedTaxonomyWriter(dir);
    }

    public LuceneTaxonomyReader openReader(Directory dir) throws CorruptIndexException, LockObtainFailedException, IOException {
      return new InstrumentedTaxonomyReader(dir);
    }

    public int nopen() {
      int ret=0;
      for (int i: openReaders) {
        System.err.println("reader "+i+" still open");
        ret++;
      }
      for (int i: openWriters) {
        System.err.println("writer "+i+" still open");
        ret++;
      }
      return ret;
    }

    private class InstrumentedTaxonomyWriter extends LuceneTaxonomyWriter {
      public InstrumentedTaxonomyWriter(Directory dir) throws CorruptIndexException, LockObtainFailedException, IOException {
        super(dir);
      }    
      @Override
      protected IndexReader openReader() throws IOException {
        return new InstrumentedIndexReader(super.openReader()); 
      }
      @Override
      protected void openLuceneIndex (Directory directory, OpenMode openMode)
      throws CorruptIndexException, LockObtainFailedException, IOException {
        indexWriter = new InstrumentedIndexWriter(directory,
            newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.KEYWORD, false))
                .setOpenMode(openMode));
      }

    }

    private class InstrumentedTaxonomyReader extends LuceneTaxonomyReader {
      public InstrumentedTaxonomyReader(Directory dir) throws CorruptIndexException, LockObtainFailedException, IOException {
        super(dir);
      }  
      @Override
      protected IndexReader openIndexReader(Directory dir) throws CorruptIndexException, IOException {
        return new InstrumentedIndexReader(IndexReader.open(dir,true)); 
      }

    }

    private class InstrumentedIndexReader extends FilterIndexReader {
      int mynum;
      public InstrumentedIndexReader(IndexReader in) {
        super(in);
        this.in = in;
        mynum = ireader++;
        openReaders.add(mynum);
        //        System.err.println("opened "+mynum);
      }
      @Override
      public synchronized IndexReader reopen() throws CorruptIndexException, IOException {
        IndexReader n = in.reopen();
        if (n==in) {
          return this;
        }
        return new InstrumentedIndexReader(n);
      }

      // Unfortunately, IndexReader.close() is marked final so we can't
      // change it! Fortunately, close() calls (if the object wasn't
      // already closed) doClose() so we can override it to do our thing -
      // just like FilterIndexReader does.
      @Override
      public void doClose() throws IOException {
        in.close();
        if (!openReaders.contains(mynum)) { // probably can't happen...
          fail("Reader #"+mynum+" was closed twice!");
        }
        openReaders.remove(mynum);
        //        System.err.println("closed "+mynum);
      }
    }
    private class InstrumentedIndexWriter extends IndexWriter {
      int mynum;
      public InstrumentedIndexWriter(Directory d, IndexWriterConfig conf) throws CorruptIndexException, LockObtainFailedException, IOException {
        super(d, conf);
        mynum = iwriter++;
        openWriters.add(mynum);
        //        System.err.println("openedw "+mynum);
      }

      @Override
      public void close() throws IOException {
        super.close();
        if (!openWriters.contains(mynum)) { // probably can't happen...
          fail("Writer #"+mynum+" was closed twice!");
        }
        openWriters.remove(mynum);
        //        System.err.println("closedw "+mynum);
      }
    }
  }
}
