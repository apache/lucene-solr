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


import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.compressing.CompressingCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.LuceneTestCase.Monster;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

/**
 * Test indexes 2B docs with 65k freqs each, 
 * so you get &gt; Integer.MAX_VALUE postings data for the term
 * @lucene.experimental
 */
@SuppressCodecs({ "SimpleText", "Direct" })
@Monster("takes ~20GB-30GB of space and 10 minutes")
public class Test2BPostingsBytes extends LuceneTestCase {

  public void test() throws Exception {
    IndexWriterConfig defaultConfig = new IndexWriterConfig(null);
    Codec defaultCodec = defaultConfig.getCodec();
    if ((new IndexWriterConfig(null)).getCodec() instanceof CompressingCodec) {
      Pattern regex = Pattern.compile("maxDocsPerChunk=(\\d+), blockSize=(\\d+)");
      Matcher matcher = regex.matcher(defaultCodec.toString());
      assertTrue("Unexpected CompressingCodec toString() output: " + defaultCodec.toString(), matcher.find());
      int maxDocsPerChunk = Integer.parseInt(matcher.group(1));
      int blockSize = Integer.parseInt(matcher.group(2));
      int product = maxDocsPerChunk * blockSize;
      assumeTrue(defaultCodec.getName() + " maxDocsPerChunk (" + maxDocsPerChunk
          + ") * blockSize (" + blockSize + ") < 16 - this can trigger OOM with -Dtests.heapsize=30g",
          product >= 16);
    }

    BaseDirectoryWrapper dir = newFSDirectory(createTempDir("2BPostingsBytes1"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    
    IndexWriter w = new IndexWriter(dir,
        new IndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setRAMBufferSizeMB(256.0)
        .setMergeScheduler(new ConcurrentMergeScheduler())
        .setMergePolicy(newLogMergePolicy(false, 10))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        .setCodec(TestUtil.getDefaultCodec()));

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
    DirectoryReader subReaders[] = new DirectoryReader[1000];
    Arrays.fill(subReaders, oneThousand);
    BaseDirectoryWrapper dir2 = newFSDirectory(createTempDir("2BPostingsBytes2"));
    if (dir2 instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir2).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    IndexWriter w2 = new IndexWriter(dir2,
        new IndexWriterConfig(null));
    TestUtil.addIndexesSlowly(w2, subReaders);
    w2.forceMerge(1);
    w2.close();
    oneThousand.close();
    
    DirectoryReader oneMillion = DirectoryReader.open(dir2);
    subReaders = new DirectoryReader[2000];
    Arrays.fill(subReaders, oneMillion);
    BaseDirectoryWrapper dir3 = newFSDirectory(createTempDir("2BPostingsBytes3"));
    if (dir3 instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir3).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    IndexWriter w3 = new IndexWriter(dir3,
        new IndexWriterConfig(null));
    TestUtil.addIndexesSlowly(w3, subReaders);
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

    @Override
    public boolean incrementToken() {
      if (index < n) {
        clearAttributes();
        termAtt.buffer()[0] = 'a';
        termAtt.setLength(1);
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
