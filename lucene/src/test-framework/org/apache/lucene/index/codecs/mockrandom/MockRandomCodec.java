package org.apache.lucene.index.codecs.mockrandom;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.BlockTermsReader;
import org.apache.lucene.index.codecs.BlockTermsWriter;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.DocValuesConsumer;
import org.apache.lucene.index.codecs.DefaultDocValuesProducer;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.FixedGapTermsIndexReader;
import org.apache.lucene.index.codecs.FixedGapTermsIndexWriter;
import org.apache.lucene.index.codecs.PerDocConsumer;
import org.apache.lucene.index.codecs.DefaultDocValuesConsumer;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.TermStats;
import org.apache.lucene.index.codecs.TermsIndexReaderBase;
import org.apache.lucene.index.codecs.TermsIndexWriterBase;
import org.apache.lucene.index.codecs.VariableGapTermsIndexReader;
import org.apache.lucene.index.codecs.VariableGapTermsIndexWriter;
import org.apache.lucene.index.codecs.mockintblock.MockFixedIntBlockCodec;
import org.apache.lucene.index.codecs.mockintblock.MockVariableIntBlockCodec;
import org.apache.lucene.index.codecs.mocksep.MockSingleIntFactory;
import org.apache.lucene.index.codecs.pulsing.PulsingPostingsReaderImpl;
import org.apache.lucene.index.codecs.pulsing.PulsingPostingsWriterImpl;
import org.apache.lucene.index.codecs.sep.IntIndexInput;
import org.apache.lucene.index.codecs.sep.IntIndexOutput;
import org.apache.lucene.index.codecs.sep.IntStreamFactory;
import org.apache.lucene.index.codecs.sep.SepPostingsReaderImpl;
import org.apache.lucene.index.codecs.sep.SepPostingsWriterImpl;
import org.apache.lucene.index.codecs.standard.StandardPostingsReader;
import org.apache.lucene.index.codecs.standard.StandardPostingsWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Randomly combines terms index impl w/ postings impls.
 */

public class MockRandomCodec extends Codec {

  private final Random seedRandom;
  private final String SEED_EXT = "sd";

  public MockRandomCodec(Random random) {
    name = "MockRandom";
    this.seedRandom = new Random(random.nextLong());
  }

  // Chooses random IntStreamFactory depending on file's extension
  private static class MockIntStreamFactory extends IntStreamFactory {
    private final int salt;
    private final List<IntStreamFactory> delegates = new ArrayList<IntStreamFactory>();

    public MockIntStreamFactory(Random random) {
      salt = random.nextInt();
      delegates.add(new MockSingleIntFactory());
      final int blockSize = _TestUtil.nextInt(random, 1, 2000);
      delegates.add(new MockFixedIntBlockCodec.MockIntFactory(blockSize));
      final int baseBlockSize = _TestUtil.nextInt(random, 1, 127);
      delegates.add(new MockVariableIntBlockCodec.MockIntFactory(baseBlockSize));
      // TODO: others
    }

    private static String getExtension(String fileName) {
      final int idx = fileName.indexOf('.');
      assert idx != -1;
      return fileName.substring(idx);
    }

    @Override
    public IntIndexInput openInput(Directory dir, String fileName, int readBufferSize) throws IOException {
      // Must only use extension, because IW.addIndexes can
      // rename segment!
      final IntStreamFactory f = delegates.get((Math.abs(salt ^ getExtension(fileName).hashCode())) % delegates.size());
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: read using int factory " + f + " from fileName=" + fileName);
      }
      return f.openInput(dir, fileName, readBufferSize);
    }

    @Override
    public IntIndexOutput createOutput(Directory dir, String fileName) throws IOException {
      final IntStreamFactory f = delegates.get((Math.abs(salt ^ getExtension(fileName).hashCode())) % delegates.size());
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: write using int factory " + f + " to fileName=" + fileName);
      }
      return f.createOutput(dir, fileName);
    }
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    // we pull this before the seed intentionally: because its not consumed at runtime
    // (the skipInterval is written into postings header)
    int skipInterval = _TestUtil.nextInt(seedRandom, 2, 10);
    
    if (LuceneTestCase.VERBOSE) {
      System.out.println("MockRandomCodec: skipInterval=" + skipInterval);
    }
    
    final long seed = seedRandom.nextLong();

    if (LuceneTestCase.VERBOSE) {
      System.out.println("MockRandomCodec: writing to seg=" + state.segmentName + " seed=" + seed);
    }

    final String seedFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecIdAsString(), SEED_EXT);
    final IndexOutput out = state.directory.createOutput(seedFileName);
    try {
      out.writeLong(seed);
    } finally {
      out.close();
    }

    final Random random = new Random(seed);
    
    random.nextInt(); // consume a random for buffersize
    
    PostingsWriterBase postingsWriter;

    if (random.nextBoolean()) {
      postingsWriter = new SepPostingsWriterImpl(state, new MockIntStreamFactory(random), skipInterval);
    } else {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: writing Standard postings");
      }
      postingsWriter = new StandardPostingsWriter(state, skipInterval);
    }

    if (random.nextBoolean()) {
      final int totTFCutoff = _TestUtil.nextInt(random, 1, 20);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: pulsing postings with totTFCutoff=" + totTFCutoff);
      }
      postingsWriter = new PulsingPostingsWriterImpl(totTFCutoff, postingsWriter);
    }

    final TermsIndexWriterBase indexWriter;
    boolean success = false;

    try {
      if (random.nextBoolean()) {
        state.termIndexInterval = _TestUtil.nextInt(random, 1, 100);
        if (LuceneTestCase.VERBOSE) {
          System.out.println("MockRandomCodec: fixed-gap terms index (tii=" + state.termIndexInterval + ")");
        }
        indexWriter = new FixedGapTermsIndexWriter(state);
      } else {
        final VariableGapTermsIndexWriter.IndexTermSelector selector;
        final int n2 = random.nextInt(3);
        if (n2 == 0) {
          final int tii = _TestUtil.nextInt(random, 1, 100);
          selector = new VariableGapTermsIndexWriter.EveryNTermSelector(tii);
          if (LuceneTestCase.VERBOSE) {
            System.out.println("MockRandomCodec: variable-gap terms index (tii=" + tii + ")");
          }
        } else if (n2 == 1) {
          final int docFreqThresh = _TestUtil.nextInt(random, 2, 100);
          final int tii = _TestUtil.nextInt(random, 1, 100);
          selector = new VariableGapTermsIndexWriter.EveryNOrDocFreqTermSelector(docFreqThresh, tii);
        } else {
          final long seed2 = random.nextLong();
          final int gap = _TestUtil.nextInt(random, 2, 40);
          if (LuceneTestCase.VERBOSE) {
            System.out.println("MockRandomCodec: random-gap terms index (max gap=" + gap + ")");
          }
          selector = new VariableGapTermsIndexWriter.IndexTermSelector() {
              final Random rand = new Random(seed2);

              @Override
              public boolean isIndexTerm(BytesRef term, TermStats stats) {
                return rand.nextInt(gap) == gap/2;
              }

              @Override
              public void newField(FieldInfo fieldInfo) {
              }
            };
        }
        indexWriter = new VariableGapTermsIndexWriter(state, selector);
      }
      success = true;
    } finally {
      if (!success) {
        postingsWriter.close();
      }
    }

    success = false;
    try {
      FieldsConsumer ret = new BlockTermsWriter(indexWriter, state, postingsWriter);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          postingsWriter.close();
        } finally {
          indexWriter.close();
        }
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {

    final String seedFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.codecIdAsString(), SEED_EXT);
    final IndexInput in = state.dir.openInput(seedFileName);
    final long seed = in.readLong();
    if (LuceneTestCase.VERBOSE) {
      System.out.println("MockRandomCodec: reading from seg=" + state.segmentInfo.name + " seed=" + seed);
    }
    in.close();

    final Random random = new Random(seed);
    
    int readBufferSize = _TestUtil.nextInt(random, 1, 4096);
    if (LuceneTestCase.VERBOSE) {
      System.out.println("MockRandomCodec: readBufferSize=" + readBufferSize);
    }

    PostingsReaderBase postingsReader;

    if (random.nextBoolean()) {
      postingsReader = new SepPostingsReaderImpl(state.dir, state.segmentInfo,
                                                 readBufferSize, new MockIntStreamFactory(random), state.codecId);
    } else {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading Standard postings");
      }
      postingsReader = new StandardPostingsReader(state.dir, state.segmentInfo, readBufferSize, state.codecId);
    }

    if (random.nextBoolean()) {
      final int totTFCutoff = _TestUtil.nextInt(random, 1, 20);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading pulsing postings with totTFCutoff=" + totTFCutoff);
      }
      postingsReader = new PulsingPostingsReaderImpl(postingsReader);
    }

    final TermsIndexReaderBase indexReader;
    boolean success = false;

    try {
      if (random.nextBoolean()) {
        // if termsIndexDivisor is set to -1, we should not touch it. It means a
        // test explicitly instructed not to load the terms index.
        if (state.termsIndexDivisor != -1) {
          state.termsIndexDivisor = _TestUtil.nextInt(random, 1, 10);
        }
        if (LuceneTestCase.VERBOSE) {
          System.out.println("MockRandomCodec: fixed-gap terms index (divisor=" + state.termsIndexDivisor + ")");
        }
        indexReader = new FixedGapTermsIndexReader(state.dir,
                                                   state.fieldInfos,
                                                   state.segmentInfo.name,
                                                   state.termsIndexDivisor,
                                                   BytesRef.getUTF8SortedAsUnicodeComparator(),
                                                   state.codecId);
      } else {
        final int n2 = random.nextInt(3);
        if (n2 == 1) {
          random.nextInt();
        } else if (n2 == 2) {
          random.nextLong();
        }
        if (LuceneTestCase.VERBOSE) {
          System.out.println("MockRandomCodec: variable-gap terms index (divisor=" + state.termsIndexDivisor + ")");
        }
        if (state.termsIndexDivisor != -1) {
          state.termsIndexDivisor = _TestUtil.nextInt(random, 1, 10);
        }
        indexReader = new VariableGapTermsIndexReader(state.dir,
                                                      state.fieldInfos,
                                                      state.segmentInfo.name,
                                                      state.termsIndexDivisor,
                                                      state.codecId);
      }
      success = true;
    } finally {
      if (!success) {
        postingsReader.close();
      }
    }

    final int termsCacheSize = _TestUtil.nextInt(random, 1, 1024);

    success = false;
    try {
      FieldsProducer ret = new BlockTermsReader(indexReader,
                                                state.dir,
                                                state.fieldInfos,
                                                state.segmentInfo.name,
                                                postingsReader,
                                                readBufferSize,
                                                termsCacheSize,
                                                state.codecId);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          postingsReader.close();
        } finally {
          indexReader.close();
        }
      }
    }
  }

  @Override
  public void files(Directory dir, SegmentInfo segmentInfo, int codecId, Set<String> files) throws IOException {
    final String codecIdAsString = codecId + "";
    final String seedFileName = IndexFileNames.segmentFileName(segmentInfo.name, codecIdAsString, SEED_EXT);    
    files.add(seedFileName);
    SepPostingsReaderImpl.files(segmentInfo, codecIdAsString, files);
    StandardPostingsReader.files(dir, segmentInfo, codecIdAsString, files);
    BlockTermsReader.files(dir, segmentInfo, codecIdAsString, files);
    FixedGapTermsIndexReader.files(dir, segmentInfo, codecIdAsString, files);
    VariableGapTermsIndexReader.files(dir, segmentInfo, codecIdAsString, files);
    DefaultDocValuesConsumer.files(dir, segmentInfo, codecId, files);
    // hackish!
    Iterator<String> it = files.iterator();
    while(it.hasNext()) {
      final String file = it.next();
      if (!dir.fileExists(file)) {
        it.remove();
      }
    }
    //System.out.println("MockRandom.files return " + files);
  }

  @Override
  public void getExtensions(Set<String> extensions) {
    SepPostingsWriterImpl.getExtensions(extensions);
    BlockTermsReader.getExtensions(extensions);
    FixedGapTermsIndexReader.getIndexExtensions(extensions);
    VariableGapTermsIndexReader.getIndexExtensions(extensions);
    DefaultDocValuesConsumer.getDocValuesExtensions(extensions);
    extensions.add(SEED_EXT);
    //System.out.println("MockRandom.getExtensions return " + extensions);
  }
  
  // can we make this more evil?
  @Override
  public PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException {
    return new DefaultDocValuesConsumer(state, BytesRef.getUTF8SortedAsUnicodeComparator());
  }

  @Override
  public PerDocValues docsProducer(SegmentReadState state) throws IOException {
    return new DefaultDocValuesProducer(state.segmentInfo, state.dir, state.fieldInfos, state.codecId);
  }
}
