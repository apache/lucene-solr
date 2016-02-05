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


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
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
 * Test indexes ~82M docs with 52 positions each, so you get &gt; Integer.MAX_VALUE positions
 * @lucene.experimental
 */
@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
@Monster("uses lots of space and takes a few minutes")
public class Test2BPositions extends LuceneTestCase {

  public void test() throws Exception {
    BaseDirectoryWrapper dir = newFSDirectory(createTempDir("2BPositions"));
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
    ft.setOmitNorms(true);
    Field field = new Field("field", new MyTokenStream(), ft);
    doc.add(field);
    
    final int numDocs = (Integer.MAX_VALUE / 26) + 1;
    for (int i = 0; i < numDocs; i++) {
      w.addDocument(doc);
      if (VERBOSE && i % 100000 == 0) {
        System.out.println(i + " of " + numDocs + "...");
      }
    }
    w.forceMerge(1);
    w.close();
    dir.close();
  }
  
  public static final class MyTokenStream extends TokenStream {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    int index;

    @Override
    public boolean incrementToken() {
      if (index < 52) {
        clearAttributes();
        termAtt.setLength(1);
        termAtt.buffer()[0] = 'a';
        posIncAtt.setPositionIncrement(1+index);
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
