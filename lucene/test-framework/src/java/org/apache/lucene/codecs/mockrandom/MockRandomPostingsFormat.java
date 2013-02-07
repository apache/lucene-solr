package org.apache.lucene.codecs.mockrandom;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.blockterms.BlockTermsReader;
import org.apache.lucene.codecs.blockterms.BlockTermsWriter;
import org.apache.lucene.codecs.blockterms.FixedGapTermsIndexReader;
import org.apache.lucene.codecs.blockterms.FixedGapTermsIndexWriter;
import org.apache.lucene.codecs.blockterms.TermsIndexReaderBase;
import org.apache.lucene.codecs.blockterms.TermsIndexWriterBase;
import org.apache.lucene.codecs.blockterms.VariableGapTermsIndexReader;
import org.apache.lucene.codecs.blockterms.VariableGapTermsIndexWriter;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsReader;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsWriter;
import org.apache.lucene.codecs.mockintblock.MockFixedIntBlockPostingsFormat;
import org.apache.lucene.codecs.mockintblock.MockVariableIntBlockPostingsFormat;
import org.apache.lucene.codecs.mocksep.MockSingleIntFactory;
import org.apache.lucene.codecs.pulsing.PulsingPostingsReader;
import org.apache.lucene.codecs.pulsing.PulsingPostingsWriter;
import org.apache.lucene.codecs.sep.IntIndexInput;
import org.apache.lucene.codecs.sep.IntIndexOutput;
import org.apache.lucene.codecs.sep.IntStreamFactory;
import org.apache.lucene.codecs.sep.SepPostingsReader;
import org.apache.lucene.codecs.sep.SepPostingsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Randomly combines terms index impl w/ postings impls.
 */

public final class MockRandomPostingsFormat extends PostingsFormat {
  private final Random seedRandom;
  private final String SEED_EXT = "sd";
  
  public MockRandomPostingsFormat() {
    // This ctor should *only* be used at read-time: get NPE if you use it!
    this(null);
  }
  
  public MockRandomPostingsFormat(Random random) {
    super("MockRandom");
    if (random == null) {
      this.seedRandom = new Random(0L) {
        @Override
        protected int next(int arg0) {
          throw new IllegalStateException("Please use MockRandomPostingsFormat(Random)");
        }
      };
    } else {
      this.seedRandom = new Random(random.nextLong());
    }
  }

  // Chooses random IntStreamFactory depending on file's extension
  private static class MockIntStreamFactory extends IntStreamFactory {
    private final int salt;
    private final List<IntStreamFactory> delegates = new ArrayList<IntStreamFactory>();

    public MockIntStreamFactory(Random random) {
      salt = random.nextInt();
      delegates.add(new MockSingleIntFactory());
      final int blockSize = _TestUtil.nextInt(random, 1, 2000);
      delegates.add(new MockFixedIntBlockPostingsFormat.MockIntFactory(blockSize));
      final int baseBlockSize = _TestUtil.nextInt(random, 1, 127);
      delegates.add(new MockVariableIntBlockPostingsFormat.MockIntFactory(baseBlockSize));
      // TODO: others
    }

    private static String getExtension(String fileName) {
      final int idx = fileName.indexOf('.');
      assert idx != -1;
      return fileName.substring(idx);
    }

    @Override
    public IntIndexInput openInput(Directory dir, String fileName, IOContext context) throws IOException {
      // Must only use extension, because IW.addIndexes can
      // rename segment!
      final IntStreamFactory f = delegates.get((Math.abs(salt ^ getExtension(fileName).hashCode())) % delegates.size());
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: read using int factory " + f + " from fileName=" + fileName);
      }
      return f.openInput(dir, fileName, context);
    }

    @Override
    public IntIndexOutput createOutput(Directory dir, String fileName, IOContext context) throws IOException {
      final IntStreamFactory f = delegates.get((Math.abs(salt ^ getExtension(fileName).hashCode())) % delegates.size());
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: write using int factory " + f + " to fileName=" + fileName);
      }
      return f.createOutput(dir, fileName, context);
    }
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    int minSkipInterval;
    if (state.segmentInfo.getDocCount() > 1000000) {
      // Test2BPostings can OOME otherwise:
      minSkipInterval = 3;
    } else {
      minSkipInterval = 2;
    }

    // we pull this before the seed intentionally: because its not consumed at runtime
    // (the skipInterval is written into postings header)
    int skipInterval = _TestUtil.nextInt(seedRandom, minSkipInterval, 10);
    
    if (LuceneTestCase.VERBOSE) {
      System.out.println("MockRandomCodec: skipInterval=" + skipInterval);
    }
    
    final long seed = seedRandom.nextLong();

    if (LuceneTestCase.VERBOSE) {
      System.out.println("MockRandomCodec: writing to seg=" + state.segmentInfo.name + " formatID=" + state.segmentSuffix + " seed=" + seed);
    }

    final String seedFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, SEED_EXT);
    final IndexOutput out = state.directory.createOutput(seedFileName, state.context);
    try {
      out.writeLong(seed);
    } finally {
      out.close();
    }

    final Random random = new Random(seed);
    
    random.nextInt(); // consume a random for buffersize

    PostingsWriterBase postingsWriter;
    if (random.nextBoolean()) {
      postingsWriter = new SepPostingsWriter(state, new MockIntStreamFactory(random), skipInterval);
    } else {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: writing Standard postings");
      }
      // TODO: randomize variables like acceptibleOverHead?!
      postingsWriter = new Lucene41PostingsWriter(state, skipInterval);
    }

    if (random.nextBoolean()) {
      final int totTFCutoff = _TestUtil.nextInt(random, 1, 20);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: writing pulsing postings with totTFCutoff=" + totTFCutoff);
      }
      postingsWriter = new PulsingPostingsWriter(totTFCutoff, postingsWriter);
    }

    final FieldsConsumer fields;

    if (random.nextBoolean()) {
      // Use BlockTree terms dict

      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: writing BlockTree terms dict");
      }

      // TODO: would be nice to allow 1 but this is very
      // slow to write
      final int minTermsInBlock = _TestUtil.nextInt(random, 2, 100);
      final int maxTermsInBlock = Math.max(2, (minTermsInBlock-1)*2 + random.nextInt(100));

      boolean success = false;
      try {
        fields = new BlockTreeTermsWriter(state, postingsWriter, minTermsInBlock, maxTermsInBlock);
        success = true;
      } finally {
        if (!success) {
          postingsWriter.close();
        }
      }
    } else {

      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: writing Block terms dict");
      }

      boolean success = false;

      final TermsIndexWriterBase indexWriter;
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
        fields = new BlockTermsWriter(indexWriter, state, postingsWriter);
        success = true;
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

    return fields;
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {

    final String seedFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, SEED_EXT);
    final IndexInput in = state.directory.openInput(seedFileName, state.context);
    final long seed = in.readLong();
    if (LuceneTestCase.VERBOSE) {
      System.out.println("MockRandomCodec: reading from seg=" + state.segmentInfo.name + " formatID=" + state.segmentSuffix + " seed=" + seed);
    }
    in.close();

    final Random random = new Random(seed);
    
    int readBufferSize = _TestUtil.nextInt(random, 1, 4096);
    if (LuceneTestCase.VERBOSE) {
      System.out.println("MockRandomCodec: readBufferSize=" + readBufferSize);
    }

    PostingsReaderBase postingsReader;

    if (random.nextBoolean()) {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading Sep postings");
      }
      postingsReader = new SepPostingsReader(state.directory, state.fieldInfos, state.segmentInfo,
                                             state.context, new MockIntStreamFactory(random), state.segmentSuffix);
    } else {
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading Standard postings");
      }
      postingsReader = new Lucene41PostingsReader(state.directory, state.fieldInfos, state.segmentInfo, state.context, state.segmentSuffix);
    }

    if (random.nextBoolean()) {
      final int totTFCutoff = _TestUtil.nextInt(random, 1, 20);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading pulsing postings with totTFCutoff=" + totTFCutoff);
      }
      postingsReader = new PulsingPostingsReader(postingsReader);
    }

    final FieldsProducer fields;

    if (random.nextBoolean()) {
      // Use BlockTree terms dict
      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading BlockTree terms dict");
      }

      boolean success = false;
      try {
        fields = new BlockTreeTermsReader(state.directory,
                                          state.fieldInfos,
                                          state.segmentInfo,
                                          postingsReader,
                                          state.context,
                                          state.segmentSuffix,
                                          state.termsIndexDivisor);
        success = true;
      } finally {
        if (!success) {
          postingsReader.close();
        }
      }
    } else {

      if (LuceneTestCase.VERBOSE) {
        System.out.println("MockRandomCodec: reading Block terms dict");
      }
      final TermsIndexReaderBase indexReader;
      boolean success = false;
      try {
        final boolean doFixedGap = random.nextBoolean();

        // randomness diverges from writer, here:
        if (state.termsIndexDivisor != -1) {
          state.termsIndexDivisor = _TestUtil.nextInt(random, 1, 10);
        }

        if (doFixedGap) {
          // if termsIndexDivisor is set to -1, we should not touch it. It means a
          // test explicitly instructed not to load the terms index.
          if (LuceneTestCase.VERBOSE) {
            System.out.println("MockRandomCodec: fixed-gap terms index (divisor=" + state.termsIndexDivisor + ")");
          }
          indexReader = new FixedGapTermsIndexReader(state.directory,
                                                     state.fieldInfos,
                                                     state.segmentInfo.name,
                                                     state.termsIndexDivisor,
                                                     BytesRef.getUTF8SortedAsUnicodeComparator(),
                                                     state.segmentSuffix, state.context);
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
          indexReader = new VariableGapTermsIndexReader(state.directory,
                                                        state.fieldInfos,
                                                        state.segmentInfo.name,
                                                        state.termsIndexDivisor,
                                                        state.segmentSuffix, state.context);

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
        fields = new BlockTermsReader(indexReader,
                                      state.directory,
                                      state.fieldInfos,
                                      state.segmentInfo,
                                      postingsReader,
                                      state.context,
                                      termsCacheSize,
                                      state.segmentSuffix);
        success = true;
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

    return fields;
  }
}
