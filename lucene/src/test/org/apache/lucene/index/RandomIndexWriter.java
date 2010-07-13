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

import java.util.Random;
import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.util._TestUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.intblock.IntBlockCodec;
import org.apache.lucene.index.codecs.preflex.PreFlexCodec;
import org.apache.lucene.index.codecs.pulsing.PulsingCodec;
import org.apache.lucene.index.codecs.sep.SepCodec;
import org.apache.lucene.index.codecs.standard.StandardCodec;

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

  public RandomIndexWriter(Random r, Directory dir, IndexWriterConfig c) throws IOException {
    this.r = r;
    if (r.nextBoolean()) {
      c.setMergePolicy(new LogDocMergePolicy());
    }
    if (r.nextBoolean()) {
      c.setMergeScheduler(new SerialMergeScheduler());
    }
    if (r.nextBoolean()) {
      c.setMaxBufferedDocs(_TestUtil.nextInt(r, 2, 1000));
    }
    if (r.nextBoolean()) {
      c.setTermIndexInterval(_TestUtil.nextInt(r, 1, 1000));
    }
    
    if (c.getMergePolicy() instanceof LogMergePolicy) {
      LogMergePolicy logmp = (LogMergePolicy) c.getMergePolicy();
      logmp.setUseCompoundDocStore(r.nextBoolean());
      logmp.setUseCompoundFile(r.nextBoolean());
      logmp.setCalibrateSizeByDeletes(r.nextBoolean());
    }
    
    c.setReaderPooling(r.nextBoolean());
    c.setCodecProvider(new RandomCodecProvider(r));
    w = new IndexWriter(dir, c);
    flushAt = _TestUtil.nextInt(r, 10, 1000);
  } 

  public void addDocument(Document doc) throws IOException {
    w.addDocument(doc);
    if (docCount++ == flushAt) {
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
  
  public int maxDoc() {
    return w.maxDoc();
  }

  public IndexReader getReader() throws IOException {
    if (r.nextBoolean()) {
      return w.getReader();
    } else {
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
  
  class RandomCodecProvider extends CodecProvider {
    final String codec;
    
    RandomCodecProvider(Random random) {
      register(new StandardCodec());
      register(new IntBlockCodec());
      register(new PreFlexCodec());
      register(new PulsingCodec());
      register(new SepCodec());
      codec = CodecProvider.CORE_CODECS[random.nextInt(CodecProvider.CORE_CODECS.length)];
    }
    
    @Override
    public Codec getWriter(SegmentWriteState state) {
      return lookup(codec);
    }
  }
}
