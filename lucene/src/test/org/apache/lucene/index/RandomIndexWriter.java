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

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCaseJ4;
import org.apache.lucene.util.Version;
import org.apache.lucene.util._TestUtil;

/** Silly class that randomizes the indexing experience.  EG
 *  it may swap in a different merge policy/scheduler; may
 *  commit periodically; may or may not optimize in the end,
 *  may flush by doc count instead of RAM, etc. 
 */

public class RandomIndexWriter implements Closeable {

  public IndexWriter w;
  private final Random r;
  int docCount;
  int flushAt;

  // Randomly calls Thread.yield so we mixup thread scheduling
  private static final class MockIndexWriter extends IndexWriter {

    private final Random r;

    public MockIndexWriter(Random r,Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
      this.r = r;
    }

    @Override
    boolean testPoint(String name) {
      if (r.nextInt(4) == 2)
        Thread.yield();
      return true;
    }
  }

  /** create a RandomIndexWriter with a random config: Uses TEST_VERSION_CURRENT and MockAnalyzer */
  public RandomIndexWriter(Random r, Directory dir) throws IOException {
    this(r, dir, LuceneTestCaseJ4.newIndexWriterConfig(r, LuceneTestCaseJ4.TEST_VERSION_CURRENT, new MockAnalyzer()));
  }
  
  /** create a RandomIndexWriter with a random config: Uses TEST_VERSION_CURRENT */
  public RandomIndexWriter(Random r, Directory dir, Analyzer a) throws IOException {
    this(r, dir, LuceneTestCaseJ4.newIndexWriterConfig(r, LuceneTestCaseJ4.TEST_VERSION_CURRENT, a));
  }
  
  /** create a RandomIndexWriter with a random config */
  public RandomIndexWriter(Random r, Directory dir, Version v, Analyzer a) throws IOException {
    this(r, dir, LuceneTestCaseJ4.newIndexWriterConfig(r, v, a));
  }
  
  /** create a RandomIndexWriter with the provided config */
  public RandomIndexWriter(Random r, Directory dir, IndexWriterConfig c) throws IOException {
    this.r = r;
    w = new MockIndexWriter(r, dir, c);
    flushAt = _TestUtil.nextInt(r, 10, 1000);
    if (LuceneTestCaseJ4.VERBOSE) {
      System.out.println("RIW config=" + w.getConfig());
      System.out.println("codec default=" + CodecProvider.getDefaultCodec());
    }
  } 

  public void addDocument(Document doc) throws IOException {
    w.addDocument(doc);
    if (docCount++ == flushAt) {
      if (LuceneTestCaseJ4.VERBOSE) {
        System.out.println("RIW.addDocument: now doing a commit");
      }
      w.commit();
      flushAt += _TestUtil.nextInt(r, 10, 1000);
    }
  }
  
  public void addIndexes(Directory... dirs) throws CorruptIndexException, IOException {
    w.addIndexes(dirs);
  }
  
  public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
    w.deleteDocuments(term);
  }
  
  public void commit() throws CorruptIndexException, IOException {
    w.commit();
  }
  
  public int maxDoc() {
    return w.maxDoc();
  }

  public IndexReader getReader() throws IOException {
    // If we are writing with PreFlexRW, force a full
    // IndexReader.open so terms are sorted in codepoint
    // order during searching:
    if (!w.codecs.getWriter(null).name.equals("PreFlex") && r.nextBoolean()) {
      if (LuceneTestCaseJ4.VERBOSE) {
        System.out.println("RIW.getReader: use NRT reader");
      }
      return w.getReader();
    } else {
      if (LuceneTestCaseJ4.VERBOSE) {
        System.out.println("RIW.getReader: open new reader");
      }
      w.commit();
      return IndexReader.open(w.getDirectory(), new KeepOnlyLastCommitDeletionPolicy(), r.nextBoolean(), _TestUtil.nextInt(r, 1, 10));
    }
  }

  public void close() throws IOException {
    if (r.nextInt(4) == 2) {
      w.optimize();
    }
    w.close();
  }

  public void optimize() throws IOException {
    w.optimize();
  }
}
