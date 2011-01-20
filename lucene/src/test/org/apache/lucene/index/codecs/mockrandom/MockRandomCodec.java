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
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.codecs.BlockTermsReader;
import org.apache.lucene.index.codecs.BlockTermsWriter;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.FixedGapTermsIndexReader;
import org.apache.lucene.index.codecs.FixedGapTermsIndexWriter;
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

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {

    final long seed = seedRandom.nextLong();

    final String seedFileName = IndexFileNames.segmentFileName(state.segmentName, state.codecId, SEED_EXT);
    final IndexOutput out = state.directory.createOutput(seedFileName);
    out.writeLong(seed);
    out.close();

    final Random random = new Random(seed);
    PostingsWriterBase postingsWriter;
    final int n = random.nextInt(4);

    if (n == 0) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: writing MockSep postings");
      }
      postingsWriter = new SepPostingsWriterImpl(state, new MockSingleIntFactory());
    } else if (n == 1) {
      final int blockSize = _TestUtil.nextInt(random, 1, 2000);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: writing MockFixedIntBlock(" + blockSize + ") postings");
      }
      postingsWriter = new SepPostingsWriterImpl(state, new MockFixedIntBlockCodec.MockIntFactory(blockSize));
    } else if (n == 2) {
      final int baseBlockSize = _TestUtil.nextInt(random, 1, 127);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: writing MockVariableIntBlock(" + baseBlockSize + ") postings");
      }
      postingsWriter = new SepPostingsWriterImpl(state, new MockVariableIntBlockCodec.MockIntFactory(baseBlockSize));
    } else {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: writing Standard postings");
      }
      postingsWriter = new StandardPostingsWriter(state);
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
                return random.nextInt(gap) == 17;
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
      FieldsConsumer ret = new BlockTermsWriter(indexWriter, state, postingsWriter, BytesRef.getUTF8SortedAsUnicodeComparator());
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

    final String seedFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.codecId, SEED_EXT);
    final IndexInput in = state.dir.openInput(seedFileName);
    final long seed = in.readLong();
    in.close();

    final Random random = new Random(seed);
    PostingsReaderBase postingsReader;
    final int n = random.nextInt(4);

    if (n == 0) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading MockSep postings");
      }
      postingsReader = new SepPostingsReaderImpl(state.dir, state.segmentInfo,
                                                 state.readBufferSize, new MockSingleIntFactory(), state.codecId);
    } else if (n == 1) {
      final int blockSize = _TestUtil.nextInt(random, 1, 2000);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading MockFixedIntBlock(" + blockSize + ") postings");
      }
      postingsReader = new SepPostingsReaderImpl(state.dir, state.segmentInfo,
                                                 state.readBufferSize, new MockFixedIntBlockCodec.MockIntFactory(blockSize), state.codecId);
    } else if (n == 2) {
      final int baseBlockSize = _TestUtil.nextInt(random, 1, 127);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading MockVariableIntBlock(" + baseBlockSize + ") postings");
      }
      postingsReader = new SepPostingsReaderImpl(state.dir, state.segmentInfo,
                                                 state.readBufferSize, new MockVariableIntBlockCodec.MockIntFactory(baseBlockSize), state.codecId);
    } else {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading Standard postings");
      }
      postingsReader = new StandardPostingsReader(state.dir, state.segmentInfo, state.readBufferSize, state.codecId);
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
        state.termsIndexDivisor = _TestUtil.nextInt(random, 1, 10);
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
        state.termsIndexDivisor = _TestUtil.nextInt(random, 1, 10);
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
                                                state.readBufferSize,
                                                BytesRef.getUTF8SortedAsUnicodeComparator(),
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
  public void files(Directory dir, SegmentInfo segmentInfo, String codecId, Set<String> files) throws IOException {
    final String seedFileName = IndexFileNames.segmentFileName(segmentInfo.name, codecId, SEED_EXT);    
    files.add(seedFileName);
    SepPostingsReaderImpl.files(segmentInfo, codecId, files);
    StandardPostingsReader.files(dir, segmentInfo, codecId, files);
    BlockTermsReader.files(dir, segmentInfo, codecId, files);
    FixedGapTermsIndexReader.files(dir, segmentInfo, codecId, files);
    VariableGapTermsIndexReader.files(dir, segmentInfo, codecId, files);
    
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
    extensions.add(SEED_EXT);
    //System.out.println("MockRandom.getExtensions return " + extensions);
  }
}
