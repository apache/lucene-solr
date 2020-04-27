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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntConsumer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.codecs.mockrandom.MockRandomPostingsFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RamUsageTester;
import org.apache.lucene.util.Rethrow;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

/**
 * Common tests to all index formats.
 */
public abstract class BaseIndexFileFormatTestCase extends LuceneTestCase {

  // metadata or Directory-level objects
  private static final Set<Class<?>> EXCLUDED_CLASSES = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());

  static {
    // Directory objects, don't take into account eg. the NIO buffers
    EXCLUDED_CLASSES.add(Directory.class);
    EXCLUDED_CLASSES.add(IndexInput.class);

    // used for thread management, not by the index
    EXCLUDED_CLASSES.add(CloseableThreadLocal.class);
    EXCLUDED_CLASSES.add(ThreadLocal.class);

    // don't follow references to the top-level reader
    EXCLUDED_CLASSES.add(IndexReader.class);
    EXCLUDED_CLASSES.add(IndexReaderContext.class);

    // usually small but can bump memory usage for
    // memory-efficient things like stored fields
    EXCLUDED_CLASSES.add(FieldInfos.class);
    EXCLUDED_CLASSES.add(SegmentInfo.class);
    EXCLUDED_CLASSES.add(SegmentCommitInfo.class);
    EXCLUDED_CLASSES.add(FieldInfo.class);

    // constant overhead is typically due to strings
    // TODO: can we remove this and still pass the test consistently
    EXCLUDED_CLASSES.add(String.class);
  }

  static class Accumulator extends RamUsageTester.Accumulator {

    private final Object root;

    Accumulator(Object root) {
      this.root = root;
    }

    public long accumulateObject(Object o, long shallowSize, Map<java.lang.reflect.Field, Object> fieldValues, Collection<Object> queue) {
      for (Class<?> clazz = o.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
        if (EXCLUDED_CLASSES.contains(clazz) && o != root) {
          return 0;
        }
      }
      // we have no way to estimate the size of these things in codecs although
      // something like a Collections.newSetFromMap(new HashMap<>()) uses quite
      // some memory... So for now the test ignores the overhead of such
      // collections but can we do better?
      long v;
      if (o instanceof Collection) {
        Collection<?> coll = (Collection<?>) o;
        queue.addAll((Collection<?>) o);
        v = (long) coll.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      } else if (o instanceof Map) {
        final Map<?, ?> map = (Map<?,?>) o;
        queue.addAll(map.keySet());
        queue.addAll(map.values());
        v = 2L * map.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      } else {
        List<Object> references = new ArrayList<>();
        v = super.accumulateObject(o, shallowSize, fieldValues, references);
        for (Object r : references) {
          // AssertingCodec adds Thread references to make sure objects are consumed in the right thread
          if (r instanceof Thread == false) {
            queue.add(r);
          }
        }
      }
      return v;
    }

    @Override
    public long accumulateArray(Object array, long shallowSize,
        List<Object> values, Collection<Object> queue) {
      long v = super.accumulateArray(array, shallowSize, values, queue);
      // System.out.println(array.getClass() + "=" + v);
      return v;
    }

  };

  /** Returns the codec to run tests against */
  protected abstract Codec getCodec();

  /** Returns the major version that this codec is compatible with. */
  protected int getCreatedVersionMajor() {
    return Version.LATEST.major;
  }

  /** Set the created version of the given {@link Directory} and return it. */
  protected final <D extends Directory> D applyCreatedVersionMajor(D d) throws IOException {
    if (SegmentInfos.getLastCommitGeneration(d) != -1) {
      throw new IllegalArgumentException("Cannot set the created version on a Directory that already has segments");
    }
    if (getCreatedVersionMajor() != Version.LATEST.major || random().nextBoolean()) {
      new SegmentInfos(getCreatedVersionMajor()).commit(d);
    }
    return d;
  }

  private Codec savedCodec;

  public void setUp() throws Exception {
    super.setUp();
    // set the default codec, so adding test cases to this isn't fragile
    savedCodec = Codec.getDefault();
    Codec.setDefault(getCodec());
  }

  public void tearDown() throws Exception {
    Codec.setDefault(savedCodec); // restore
    super.tearDown();
  }

  /** Add random fields to the provided document. */
  protected abstract void addRandomFields(Document doc);

  private Map<String, Long> bytesUsedByExtension(Directory d) throws IOException {
    Map<String, Long> bytesUsedByExtension = new HashMap<>();
    for (String file : d.listAll()) {
      if (IndexFileNames.CODEC_FILE_PATTERN.matcher(file).matches()) {
        final String ext = IndexFileNames.getExtension(file);
        final long previousLength = bytesUsedByExtension.containsKey(ext) ? bytesUsedByExtension.get(ext) : 0;
        bytesUsedByExtension.put(ext, previousLength + d.fileLength(file));
      }
    }
    bytesUsedByExtension.keySet().removeAll(excludedExtensionsFromByteCounts());

    return bytesUsedByExtension;
  }

  /**
   * Return the list of extensions that should be excluded from byte counts when
   * comparing indices that store the same content.
   */
  protected Collection<String> excludedExtensionsFromByteCounts() {
    return new HashSet<String>(Arrays.asList(new String[] {
    // segment infos store various pieces of information that don't solely depend
    // on the content of the index in the diagnostics (such as a timestamp) so we
    // exclude this file from the bytes counts
                        "si",
    // lock files are 0 bytes (one directory in the test could be RAMDir, the other FSDir)
                        "lock" }));
  }

  /** The purpose of this test is to make sure that bulk merge doesn't accumulate useless data over runs. */
  public void testMergeStability() throws Exception {
    assumeTrue("merge is not stable", mergeIsStable());
    Directory dir = applyCreatedVersionMajor(newDirectory());

    // do not use newMergePolicy that might return a MockMergePolicy that ignores the no-CFS ratio
    // do not use RIW which will change things up!
    MergePolicy mp = newTieredMergePolicy();
    mp.setNoCFSRatio(0);
    IndexWriterConfig cfg = new IndexWriterConfig(new MockAnalyzer(random())).setUseCompoundFile(false).setMergePolicy(mp);
    IndexWriter w = new IndexWriter(dir, cfg);
    final int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; ++i) {
      Document d = new Document();
      addRandomFields(d);
      w.addDocument(d);
    }
    w.forceMerge(1);
    w.commit();
    w.close();
    DirectoryReader reader = DirectoryReader.open(dir);

    Directory dir2 = applyCreatedVersionMajor(newDirectory());
    mp = newTieredMergePolicy();
    mp.setNoCFSRatio(0);
    cfg = new IndexWriterConfig(new MockAnalyzer(random())).setUseCompoundFile(false).setMergePolicy(mp);
    w = new IndexWriter(dir2, cfg);
    TestUtil.addIndexesSlowly(w, reader);

    w.commit();
    w.close();

    assertEquals(bytesUsedByExtension(dir), bytesUsedByExtension(dir2));

    reader.close();
    dir.close();
    dir2.close();
  }

  protected boolean mergeIsStable() {
    return true;
  }

  /** Test the accuracy of the ramBytesUsed estimations. */
  @Nightly
  public void testRamBytesUsed() throws IOException {
    if (Codec.getDefault() instanceof RandomCodec) {
      // this test relies on the fact that two segments will be written with
      // the same codec so we need to disable MockRandomPF
      final Set<String> avoidCodecs = new HashSet<>(((RandomCodec) Codec.getDefault()).avoidCodecs);
      avoidCodecs.add(new MockRandomPostingsFormat().getName());
      Codec.setDefault(new RandomCodec(random(), avoidCodecs));
    }
    Directory dir = applyCreatedVersionMajor(newDirectory());
    IndexWriterConfig cfg = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, cfg);
    // we need to index enough documents so that constant overhead doesn't dominate
    final int numDocs = atLeast(10000);
    LeafReader reader1 = null;
    for (int i = 0; i < numDocs; ++i) {
      Document d = new Document();
      addRandomFields(d);
      w.addDocument(d);
      if (i == 100) {
        w.forceMerge(1);
        w.commit();
        reader1 = getOnlyLeafReader(DirectoryReader.open(dir));
      }
    }
    w.forceMerge(1);
    w.commit();
    w.close();

    LeafReader reader2 = getOnlyLeafReader(DirectoryReader.open(dir));

    for (LeafReader reader : Arrays.asList(reader1, reader2)) {
      new SimpleMergedSegmentWarmer(InfoStream.NO_OUTPUT).warm(reader);
    }

    long act1 = RamUsageTester.sizeOf(reader2, new Accumulator(reader2));
    long act2 = RamUsageTester.sizeOf(reader1, new Accumulator(reader1));
    final long measuredBytes = act1 - act2;

    long reported1 = ((SegmentReader) reader2).ramBytesUsed();
    long reported2 = ((SegmentReader) reader1).ramBytesUsed();
    final long reportedBytes = reported1 - reported2;

    final long absoluteError = Math.abs(measuredBytes - reportedBytes);
    final double relativeError = (double) absoluteError / measuredBytes;
    final String message = String.format(Locale.ROOT,
        "RamUsageTester reports %d bytes but ramBytesUsed() returned %d (%.1f error). " +
        " [Measured: %d, %d. Reported: %d, %d]",
        measuredBytes,
        reportedBytes,
        (100 * relativeError),
        act1, act2,
        reported1, reported2);

    assertTrue(message, relativeError < 0.20d || absoluteError < 1000);

    reader1.close();
    reader2.close();
    dir.close();
  }
  
  /** Calls close multiple times on closeable codec apis */
  public void testMultiClose() throws IOException {
    // first make a one doc index
    Directory oneDocIndex = applyCreatedVersionMajor(newDirectory());
    IndexWriter iw = new IndexWriter(oneDocIndex, new IndexWriterConfig(new MockAnalyzer(random())));
    Document oneDoc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    Field customField = new Field("field", "contents", customType);
    oneDoc.add(customField);
    oneDoc.add(new NumericDocValuesField("field", 5));
    iw.addDocument(oneDoc);
    LeafReader oneDocReader = getOnlyLeafReader(DirectoryReader.open(iw));
    iw.close();
    
    // now feed to codec apis manually
    // we use FSDir, things like ramdir are not guaranteed to cause fails if you write to them after close(), etc
    Directory dir = newFSDirectory(createTempDir("justSoYouGetSomeChannelErrors"));
    Codec codec = getCodec();
    
    SegmentInfo segmentInfo = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "_0", 1, false, codec, Collections.emptyMap(), StringHelper.randomId(), Collections.emptyMap(), null);
    FieldInfo proto = oneDocReader.getFieldInfos().fieldInfo("field");
    FieldInfo field = new FieldInfo(proto.name, proto.number, proto.hasVectors(), proto.omitsNorms(), proto.hasPayloads(), 
                                    proto.getIndexOptions(), proto.getDocValuesType(), proto.getDocValuesGen(), new HashMap<>(),
                                    proto.getPointDimensionCount(), proto.getPointIndexDimensionCount(), proto.getPointNumBytes(), proto.isSoftDeletesField());

    FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { field } );

    SegmentWriteState writeState = new SegmentWriteState(null, dir,
                                                         segmentInfo, fieldInfos,
                                                         null, new IOContext(new FlushInfo(1, 20)));
    
    SegmentReadState readState = new SegmentReadState(dir, segmentInfo, fieldInfos, IOContext.READ);

    // PostingsFormat
    NormsProducer fakeNorms = new NormsProducer() {

      @Override
      public void close() throws IOException {}

      @Override
      public long ramBytesUsed() {
        return 0;
      }

      @Override
      public NumericDocValues getNorms(FieldInfo field) throws IOException {
        if (field.hasNorms() == false) {
          return null;
        }
        return oneDocReader.getNormValues(field.name);
      }

      @Override
      public void checkIntegrity() throws IOException {}
      
    };
    try (FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(writeState)) {
      final Fields fields = new Fields() {
        TreeSet<String> indexedFields = new TreeSet<>(FieldInfos.getIndexedFields(oneDocReader));

        @Override
        public Iterator<String> iterator() {
          return indexedFields.iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
          return oneDocReader.terms(field);
        }

        @Override
        public int size() {
          return indexedFields.size();
        }
      };
      consumer.write(fields, fakeNorms);
      IOUtils.close(consumer);
      IOUtils.close(consumer);
    }
    try (FieldsProducer producer = codec.postingsFormat().fieldsProducer(readState)) {
      IOUtils.close(producer);
      IOUtils.close(producer);
    }
    
    // DocValuesFormat
    try (DocValuesConsumer consumer = codec.docValuesFormat().fieldsConsumer(writeState)) {
      consumer.addNumericField(field,
                               new EmptyDocValuesProducer() {
                                 @Override
                                 public NumericDocValues getNumeric(FieldInfo field) {
                                   return new NumericDocValues() {
                                     int docID = -1;
                                 
                                     @Override
                                     public int docID() {
                                       return docID;
                                     }
                                 
                                     @Override
                                     public int nextDoc() {
                                       docID++;
                                       if (docID == 1) {
                                         docID = NO_MORE_DOCS;
                                       }
                                       return docID;
                                     }

                                     @Override
                                     public int advance(int target) {
                                       if (docID <= 0 && target == 0) {
                                         docID = 0;
                                       } else {
                                         docID = NO_MORE_DOCS;
                                       }
                                       return docID;
                                     }

                                     @Override
                                    public boolean advanceExact(int target) throws IOException {
                                      docID = target;
                                      return target == 0;
                                    }

                                     @Override
                                     public long cost() {
                                       return 1;
                                     }

                                     @Override
                                     public long longValue() {
                                       return 5;
                                     }
                                   };
                                 }
                               });
      IOUtils.close(consumer);
      IOUtils.close(consumer);
    }
    try (DocValuesProducer producer = codec.docValuesFormat().fieldsProducer(readState)) {
      IOUtils.close(producer);
      IOUtils.close(producer);
    }
    
    // NormsFormat
    try (NormsConsumer consumer = codec.normsFormat().normsConsumer(writeState)) {
      consumer.addNormsField(field,
                             new NormsProducer() {
                                 @Override
                                 public NumericDocValues getNorms(FieldInfo field) {
                                   return new NumericDocValues() {
                                     int docID = -1;
                                 
                                     @Override
                                     public int docID() {
                                       return docID;
                                     }
                                 
                                     @Override
                                     public int nextDoc() {
                                       docID++;
                                       if (docID == 1) {
                                         docID = NO_MORE_DOCS;
                                       }
                                       return docID;
                                     }

                                     @Override
                                     public int advance(int target) {
                                       if (docID <= 0 && target == 0) {
                                         docID = 0;
                                       } else {
                                         docID = NO_MORE_DOCS;
                                       }
                                       return docID;
                                     }

                                     @Override
                                    public boolean advanceExact(int target) throws IOException {
                                      docID = target;
                                      return target == 0;
                                    }

                                     @Override
                                     public long cost() {
                                       return 1;
                                     }

                                     @Override
                                     public long longValue() {
                                       return 5;
                                     }
                                   };
                                 }

                               @Override
                               public void checkIntegrity() {
                               }

                               @Override
                               public void close() {
                               }

                               @Override
                               public long ramBytesUsed() {
                                 return 0;
                               }
                             });
      IOUtils.close(consumer);
      IOUtils.close(consumer);
    }
    try (NormsProducer producer = codec.normsFormat().normsProducer(readState)) {
      IOUtils.close(producer);
      IOUtils.close(producer);
    }
    
    // TermVectorsFormat
    try (TermVectorsWriter consumer = codec.termVectorsFormat().vectorsWriter(dir, segmentInfo, writeState.context)) {
      consumer.startDocument(1);
      consumer.startField(field, 1, false, false, false);
      consumer.startTerm(new BytesRef("testing"), 2);
      consumer.finishTerm();
      consumer.finishField();
      consumer.finishDocument();
      consumer.finish(fieldInfos, 1);
      IOUtils.close(consumer);
      IOUtils.close(consumer);
    }
    try (TermVectorsReader producer = codec.termVectorsFormat().vectorsReader(dir, segmentInfo, fieldInfos, readState.context)) {
      IOUtils.close(producer);
      IOUtils.close(producer);
    }
    
    // StoredFieldsFormat
    try (StoredFieldsWriter consumer = codec.storedFieldsFormat().fieldsWriter(dir, segmentInfo, writeState.context)) {
      consumer.startDocument();
      consumer.writeField(field, customField);
      consumer.finishDocument();
      consumer.finish(fieldInfos, 1);
      IOUtils.close(consumer);
      IOUtils.close(consumer);
    }
    try (StoredFieldsReader producer = codec.storedFieldsFormat().fieldsReader(dir, segmentInfo, fieldInfos, readState.context)) {
      IOUtils.close(producer);
      IOUtils.close(producer);
    }
            
    IOUtils.close(oneDocReader, oneDocIndex, dir);
  }
  
  /** Tests exception handling on write and openInput/createOutput */
  // TODO: this is really not ideal. each BaseXXXTestCase should have unit tests doing this.
  // but we use this shotgun approach to prevent bugs in the meantime: it just ensures the
  // codec does not corrupt the index or leak file handles.
  public void testRandomExceptions() throws Exception {
    // disable slow things: we don't rely upon sleeps here.
    MockDirectoryWrapper dir = applyCreatedVersionMajor(newMockDirectory());
    dir.setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    dir.setUseSlowOpenClosers(false);
    dir.setRandomIOExceptionRate(0.001); // more rare
    
    // log all exceptions we hit, in case we fail (for debugging)
    ByteArrayOutputStream exceptionLog = new ByteArrayOutputStream();
    PrintStream exceptionStream = new PrintStream(exceptionLog, true, "UTF-8");
    //PrintStream exceptionStream = System.out;
    
    Analyzer analyzer = new MockAnalyzer(random());
    
    IndexWriterConfig conf = newIndexWriterConfig(analyzer);
    // just for now, try to keep this test reproducible
    conf.setMergeScheduler(new SerialMergeScheduler());
    conf.setCodec(getCodec());
    
    int numDocs = atLeast(500);
    
    IndexWriter iw = new IndexWriter(dir, conf);
    try {
      boolean allowAlreadyClosed = false;
      for (int i = 0; i < numDocs; i++) {
        dir.setRandomIOExceptionRateOnOpen(0.02); // turn on exceptions for openInput/createOutput
        
        Document doc = new Document();
        doc.add(newStringField("id", Integer.toString(i), Field.Store.NO));
        addRandomFields(doc);
        
        // single doc
        try {
          iw.addDocument(doc);
          // we made it, sometimes delete our doc
          iw.deleteDocuments(new Term("id", Integer.toString(i)));
        } catch (AlreadyClosedException ace) {
          // OK: writer was closed by abort; we just reopen now:
          dir.setRandomIOExceptionRateOnOpen(0.0); // disable exceptions on openInput until next iteration
          assertTrue(iw.isDeleterClosed());
          assertTrue(allowAlreadyClosed);
          allowAlreadyClosed = false;
          conf = newIndexWriterConfig(analyzer);
          // just for now, try to keep this test reproducible
          conf.setMergeScheduler(new SerialMergeScheduler());
          conf.setCodec(getCodec());
          iw = new IndexWriter(dir, conf);            
        } catch (IOException e) {
          handleFakeIOException(e, exceptionStream);
          allowAlreadyClosed = true;
        }

        if (random().nextInt(10) == 0) {
          // trigger flush:
          try {
            if (random().nextBoolean()) {
              DirectoryReader ir = null;
              try {
                ir = DirectoryReader.open(iw, random().nextBoolean(), false);
                dir.setRandomIOExceptionRateOnOpen(0.0); // disable exceptions on openInput until next iteration
                TestUtil.checkReader(ir);
              } finally {
                IOUtils.closeWhileHandlingException(ir);
              }
            } else {
              dir.setRandomIOExceptionRateOnOpen(0.0); // disable exceptions on openInput until next iteration: 
                                                       // or we make slowExists angry and trip a scarier assert!
              iw.commit();
            }
            if (DirectoryReader.indexExists(dir)) {
              TestUtil.checkIndex(dir);
            }
          } catch (AlreadyClosedException ace) {
            // OK: writer was closed by abort; we just reopen now:
            dir.setRandomIOExceptionRateOnOpen(0.0); // disable exceptions on openInput until next iteration
            assertTrue(iw.isDeleterClosed());
            assertTrue(allowAlreadyClosed);
            allowAlreadyClosed = false;
            conf = newIndexWriterConfig(analyzer);
            // just for now, try to keep this test reproducible
            conf.setMergeScheduler(new SerialMergeScheduler());
            conf.setCodec(getCodec());
            iw = new IndexWriter(dir, conf);            
          } catch (IOException e) {
            handleFakeIOException(e, exceptionStream);
            allowAlreadyClosed = true;
          }
        }
      }
      
      try {
        dir.setRandomIOExceptionRateOnOpen(0.0); // disable exceptions on openInput until next iteration: 
                                                 // or we make slowExists angry and trip a scarier assert!
        iw.close();
      } catch (IOException e) {
        handleFakeIOException(e, exceptionStream);
        try {
          iw.rollback();
        } catch (Throwable t) {}
      }
      dir.close();
    } catch (Throwable t) {
      System.out.println("Unexpected exception: dumping fake-exception-log:...");
      exceptionStream.flush();
      System.out.println(exceptionLog.toString("UTF-8"));
      System.out.flush();
      Rethrow.rethrow(t);
    }
    
    if (VERBOSE) {
      System.out.println("TEST PASSED: dumping fake-exception-log:...");
      System.out.println(exceptionLog.toString("UTF-8"));
    }
  }
  
  private void handleFakeIOException(IOException e, PrintStream exceptionStream) {
    Throwable ex = e;
    while (ex != null) {
      if (ex.getMessage() != null && ex.getMessage().startsWith("a random IOException")) {
        exceptionStream.println("\nTEST: got expected fake exc:" + ex.getMessage());
        ex.printStackTrace(exceptionStream);
        return;
      }
      ex = ex.getCause();
    }
    
    Rethrow.rethrow(e);
  }

  /**
   * Returns {@code false} if only the regular fields reader should be tested,
   * and {@code true} if only the merge instance should be tested.
   */
  protected boolean shouldTestMergeInstance() {
    return false;
  }

  protected final DirectoryReader maybeWrapWithMergingReader(DirectoryReader r) throws IOException {
    if (shouldTestMergeInstance()) {
      r = new MergingDirectoryReaderWrapper(r);
    }
    return r;
  }

  /**
   * A directory that tracks created files that haven't been deleted.
   */
  protected static class FileTrackingDirectoryWrapper extends FilterDirectory {

    private final Set<String> files = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

    /** Sole constructor. */
    FileTrackingDirectoryWrapper(Directory in) {
      super(in);
    }

    /** Get the set of created files. */
    public Set<String> getFiles() {
      return Collections.unmodifiableSet(new HashSet<>(files));
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      files.add(name);
      return super.createOutput(name, context);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
      files.remove(source);
      files.add(dest);
      super.rename(source, dest);
    }

    @Override
    public void deleteFile(String name) throws IOException {
      files.remove(name);
      super.deleteFile(name);
    }

  }

  private static class ReadBytesIndexInputWrapper extends IndexInput {

    private final IndexInput in;
    private final IntConsumer readByte;

    ReadBytesIndexInputWrapper(IndexInput in, IntConsumer readByte) {
      super(in.toString());
      this.in = in;
      this.readByte = readByte;
    }

    @Override
    public IndexInput clone() {
      return new ReadBytesIndexInputWrapper(in.clone(), readByte);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public long getFilePointer() {
      return in.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
      in.seek(pos);
    }

    @Override
    public long length() {
      return in.length();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      IndexInput slice = in.slice(sliceDescription, offset, length);
      return new ReadBytesIndexInputWrapper(slice, o -> readByte.accept(Math.toIntExact(offset + o)));
    }

    @Override
    public byte readByte() throws IOException {
      readByte.accept(Math.toIntExact(getFilePointer()));
      return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      final int fp = Math.toIntExact(getFilePointer());
      for (int i = 0; i < len; ++i) {
        readByte.accept(Math.addExact(fp, i));
      }
      in.readBytes(b, offset, len);
    }

  }

  /** A directory that tracks read bytes. */
  protected static class ReadBytesDirectoryWrapper extends FilterDirectory {

    /** Sole constructor. */
    public ReadBytesDirectoryWrapper(Directory in) {
      super(in);
    }

    private final Map<String, FixedBitSet> readBytes = new ConcurrentHashMap<>();

    /** Get information about which bytes have been read. */
    public Map<String, FixedBitSet> getReadBytes() {
      return Collections.unmodifiableMap(new HashMap<>(readBytes));
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      IndexInput in = super.openInput(name, context);
      final FixedBitSet set = readBytes.computeIfAbsent(name, n -> new FixedBitSet(Math.toIntExact(in.length())));
      if (set.length() != in.length()) {
        throw new IllegalStateException();
      }
      return new ReadBytesIndexInputWrapper(in, set::set);
    }

    @Override
    public ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
      ChecksumIndexInput in = super.openChecksumInput(name, context);
      final FixedBitSet set = readBytes.computeIfAbsent(name, n -> new FixedBitSet(Math.toIntExact(in.length())));
      if (set.length() != in.length()) {
        throw new IllegalStateException();
      }
      return new ChecksumIndexInput(in.toString()) {

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
          final int fp = Math.toIntExact(getFilePointer());
          set.set(fp, Math.addExact(fp, len));
          in.readBytes(b, offset, len);
        }

        @Override
        public byte readByte() throws IOException {
          set.set(Math.toIntExact(getFilePointer()));
          return in.readByte();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public long length() {
          return in.length();
        }

        @Override
        public long getFilePointer() {
          return in.getFilePointer();
        }

        @Override
        public void close() throws IOException {
          in.close();
        }

        @Override
        public long getChecksum() throws IOException {
          return in.getChecksum();
        }
      };
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /** This test is a best effort at verifying that checkIntegrity doesn't miss any files. It tests that the
   *  combination of opening a reader and calling checkIntegrity on it reads all bytes of all files. */
  public void testCheckIntegrityReadsAllBytes() throws Exception {
    assumeFalse("SimpleText doesn't store checksums of its files", getCodec() instanceof SimpleTextCodec);
    FileTrackingDirectoryWrapper dir = new FileTrackingDirectoryWrapper(newDirectory());
    applyCreatedVersionMajor(dir);

    IndexWriterConfig cfg = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, cfg);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document d = new Document();
      addRandomFields(d);
      w.addDocument(d);
    }
    w.forceMerge(1);
    w.commit();
    w.close();

    ReadBytesDirectoryWrapper readBytesWrapperDir = new ReadBytesDirectoryWrapper(dir);
    IndexReader reader = DirectoryReader.open(readBytesWrapperDir);
    LeafReader leafReader = getOnlyLeafReader(reader);
    leafReader.checkIntegrity();

    Map<String, FixedBitSet> readBytesMap = readBytesWrapperDir.getReadBytes();

    Set<String> unreadFiles = new HashSet<>(dir.getFiles());System.out.println(Arrays.toString(dir.listAll()));
    unreadFiles.removeAll(readBytesMap.keySet());
    unreadFiles.remove(IndexWriter.WRITE_LOCK_NAME);
    assertTrue("Some files have not been open: " + unreadFiles, unreadFiles.isEmpty());

    List<String> messages = new ArrayList<>();
    for (Map.Entry<String, FixedBitSet> entry : readBytesMap.entrySet()) {
      String name = entry.getKey();
      FixedBitSet unreadBytes = entry.getValue().clone();
      unreadBytes.flip(0, unreadBytes.length());
      int unread = unreadBytes.nextSetBit(0);
      if (unread != Integer.MAX_VALUE) {
        messages.add("Offset " + unread + " of file " + name + "(" + unreadBytes.length() + "bytes) was not read.");
      }
    }
    assertTrue(String.join("\n", messages), messages.isEmpty());
    reader.close();
    dir.close();
  }
}
