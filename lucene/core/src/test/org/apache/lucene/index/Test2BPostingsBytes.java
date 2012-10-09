package org.apache.lucene.index;

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

import java.util.Arrays;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.Ignore;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

/**
 * Test indexes 2B docs with 65k freqs each, 
 * so you get > Integer.MAX_VALUE postings data for the term
 * @lucene.experimental
 */
@SuppressCodecs({ "SimpleText", "Memory", "Direct", "Lucene3x" })
// disable Lucene3x: older lucene formats always had this issue.
@TimeoutSuite(millis = 4 * TimeUnits.HOUR)
public class Test2BPostingsBytes extends LuceneTestCase {

  // @Absurd @Ignore takes ~20GB-30GB of space and 10 minutes.
  // with some codecs needs more heap space as well.
  @Ignore("Very slow. Enable manually by removing @Ignore.")
  public void test() throws Exception {
    BaseDirectoryWrapper dir = newFSDirectory(_TestUtil.getTempDir("2BPostingsBytes1"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    
    IndexWriter w = new IndexWriter(dir,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setRAMBufferSizeMB(256.0)
        .setMergeScheduler(new ConcurrentMergeScheduler())
        .setMergePolicy(newLogMergePolicy(false, 10))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE));

    MergePolicy mp = w.getConfig().getMergePolicy();
    if (mp instanceof LogByteSizeMergePolicy) {
     // 1 petabyte:
     ((LogByteSizeMergePolicy) mp).setMaxMergeMB(1024*1024*1024);
    }

    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft.setOmitNorms(true);
    MyTokenStream tokenStream = new MyTokenStream();
    Field field = new Field("field", tokenStream, ft);
    doc.add(field);
    
    final int numDocs = 1000;
    for (int i = 0; i < numDocs; i++) {
      if (i % 2 == 1) { // trick blockPF's little optimization
        tokenStream.n = 65536;
      } else {
        tokenStream.n = 65537;
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    w.close();
    
    DirectoryReader oneThousand = DirectoryReader.open(dir);
    IndexReader subReaders[] = new IndexReader[1000];
    Arrays.fill(subReaders, oneThousand);
    MultiReader mr = new MultiReader(subReaders);
    BaseDirectoryWrapper dir2 = newFSDirectory(_TestUtil.getTempDir("2BPostingsBytes2"));
    if (dir2 instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir2).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    IndexWriter w2 = new IndexWriter(dir2,
        new IndexWriterConfig(TEST_VERSION_CURRENT, null));
    w2.addIndexes(mr);
    w2.forceMerge(1);
    w2.close();
    oneThousand.close();
    
    DirectoryReader oneMillion = DirectoryReader.open(dir2);
    subReaders = new IndexReader[2000];
    Arrays.fill(subReaders, oneMillion);
    mr = new MultiReader(subReaders);
    BaseDirectoryWrapper dir3 = newFSDirectory(_TestUtil.getTempDir("2BPostingsBytes3"));
    if (dir3 instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir3).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    IndexWriter w3 = new IndexWriter(dir3,
        new IndexWriterConfig(TEST_VERSION_CURRENT, null));
    w3.addIndexes(mr);
    w3.forceMerge(1);
    w3.close();
    oneMillion.close();
    
    dir.close();
    dir2.close();
    dir3.close();
  }
  
  public static final class MyTokenStream extends TokenStream {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    int index;
    int n;

    public MyTokenStream() {
      termAtt.setLength(1);
      termAtt.buffer()[0] = 'a';
    }
    
    @Override
    public boolean incrementToken() {
      if (index < n) {
        index++;
        return true;
      }
      return false;
    }
    
    @Override
    public void reset() {
      index = 0;
    }
  }
}
