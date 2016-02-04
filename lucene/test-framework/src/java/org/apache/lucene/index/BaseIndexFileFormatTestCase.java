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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
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
abstract class BaseIndexFileFormatTestCase extends LuceneTestCase {

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
        v = super.accumulateObject(o, shallowSize, fieldValues, queue);
      }
      // System.out.println(o.getClass() + "=" + v);
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
    Directory dir = newDirectory();
    if (dir instanceof MockDirectoryWrapper) {
      // Else, the virus checker may prevent deletion of files and cause
      // us to see too many bytes used by extension in the end:
      ((MockDirectoryWrapper) dir).setEnableVirusScanner(false);
    }
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

    Directory dir2 = newDirectory();
    if (dir2 instanceof MockDirectoryWrapper) {
      // Else, the virus checker may prevent deletion of files and cause
      // us to see too many bytes used by extension in the end:
      ((MockDirectoryWrapper) dir2).setEnableVirusScanner(false);
    }
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

  /** Test the accuracy of the ramBytesUsed estimations. */
  @Slow
  public void testRamBytesUsed() throws IOException {
    if (Codec.getDefault() instanceof RandomCodec) {
      // this test relies on the fact that two segments will be written with
      // the same codec so we need to disable MockRandomPF
      final Set<String> avoidCodecs = new HashSet<>(((RandomCodec) Codec.getDefault()).avoidCodecs);
      avoidCodecs.add(new MockRandomPostingsFormat().getName());
      Codec.setDefault(new RandomCodec(random(), avoidCodecs));
    }
    Directory dir = newDirectory();
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
        reader1 = getOnlySegmentReader(DirectoryReader.open(dir));
      }
    }
    w.forceMerge(1);
    w.commit();
    w.close();

    LeafReader reader2 = getOnlySegmentReader(DirectoryReader.open(dir));

    for (LeafReader reader : Arrays.asList(reader1, reader2)) {
      new SimpleMergedSegmentWarmer(InfoStream.NO_OUTPUT).warm(reader);
    }

    final long actualBytes = RamUsageTester.sizeOf(reader2, new Accumulator(reader2)) - RamUsageTester.sizeOf(reader1, new Accumulator(reader1));
    final long expectedBytes = ((SegmentReader) reader2).ramBytesUsed() - ((SegmentReader) reader1).ramBytesUsed();
    final long absoluteError = actualBytes - expectedBytes;
    final double relativeError = (double) absoluteError / actualBytes;
    final String message = "Actual RAM usage " + actualBytes + ", but got " + expectedBytes + ", " + 100*relativeError + "% error";
    assertTrue(message, Math.abs(relativeError) < 0.20d || Math.abs(absoluteError) < 1000);

    reader1.close();
    reader2.close();
    dir.close();
  }
  
  /** Calls close multiple times on closeable codec apis */
  public void testMultiClose() throws IOException {
    // first make a one doc index
    Directory oneDocIndex = newDirectory();
    IndexWriter iw = new IndexWriter(oneDocIndex, new IndexWriterConfig(new MockAnalyzer(random())));
    Document oneDoc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    Field customField = new Field("field", "contents", customType);
    oneDoc.add(customField);
    oneDoc.add(new NumericDocValuesField("field", 5));
    iw.addDocument(oneDoc);
    LeafReader oneDocReader = getOnlySegmentReader(DirectoryReader.open(iw));
    iw.close();
    
    // now feed to codec apis manually
    // we use FSDir, things like ramdir are not guaranteed to cause fails if you write to them after close(), etc
    Directory dir = newFSDirectory(createTempDir("justSoYouGetSomeChannelErrors"));
    Codec codec = getCodec();
    
    SegmentInfo segmentInfo = new SegmentInfo(dir, Version.LATEST, "_0", 1, false, codec, Collections.<String,String>emptyMap(), StringHelper.randomId(), new HashMap<String,String>());
    FieldInfo proto = oneDocReader.getFieldInfos().fieldInfo("field");
    FieldInfo field = new FieldInfo(proto.name, proto.number, proto.hasVectors(), proto.omitsNorms(), proto.hasPayloads(), 
                                    proto.getIndexOptions(), proto.getDocValuesType(), proto.getDocValuesGen(), new HashMap<String,String>());

    FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { field } );

    SegmentWriteState writeState = new SegmentWriteState(null, dir,
                                                         segmentInfo, fieldInfos,
                                                         null, new IOContext(new FlushInfo(1, 20)));
    
    SegmentReadState readState = new SegmentReadState(dir, segmentInfo, fieldInfos, IOContext.READ);

    // PostingsFormat
    try (FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(writeState)) {
      consumer.write(oneDocReader.fields());
      IOUtils.close(consumer);
      IOUtils.close(consumer);
    }
    try (FieldsProducer producer = codec.postingsFormat().fieldsProducer(readState)) {
      IOUtils.close(producer);
      IOUtils.close(producer);
    }
    
    // DocValuesFormat
    try (DocValuesConsumer consumer = codec.docValuesFormat().fieldsConsumer(writeState)) {
      consumer.addNumericField(field, Collections.<Number>singleton(5));
      IOUtils.close(consumer);
      IOUtils.close(consumer);
    }
    try (DocValuesProducer producer = codec.docValuesFormat().fieldsProducer(readState)) {
      IOUtils.close(producer);
      IOUtils.close(producer);
    }
    
    // NormsFormat
    try (NormsConsumer consumer = codec.normsFormat().normsConsumer(writeState)) {
      consumer.addNormsField(field, Collections.<Number>singleton(5));
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
    MockDirectoryWrapper dir = newMockDirectory();
    dir.setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    dir.setUseSlowOpenClosers(false);
    dir.setPreventDoubleWrite(false);
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
          assertTrue(iw.deleter.isClosed());
          assertTrue(allowAlreadyClosed);
          allowAlreadyClosed = false;
          conf = newIndexWriterConfig(analyzer);
          // just for now, try to keep this test reproducible
          conf.setMergeScheduler(new SerialMergeScheduler());
          conf.setCodec(getCodec());
          iw = new IndexWriter(dir, conf);            
        } catch (Exception e) {
          if (e.getMessage() != null && e.getMessage().startsWith("a random IOException")) {
            exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
            e.printStackTrace(exceptionStream);
            allowAlreadyClosed = true;
          } else {
            Rethrow.rethrow(e);
          }
        }
        
        if (random().nextInt(10) == 0) {
          // trigger flush:
          try {
            if (random().nextBoolean()) {
              DirectoryReader ir = null;
              try {
                ir = DirectoryReader.open(iw, random().nextBoolean());
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
            assertTrue(iw.deleter.isClosed());
            assertTrue(allowAlreadyClosed);
            allowAlreadyClosed = false;
            conf = newIndexWriterConfig(analyzer);
            // just for now, try to keep this test reproducible
            conf.setMergeScheduler(new SerialMergeScheduler());
            conf.setCodec(getCodec());
            iw = new IndexWriter(dir, conf);            
          } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().startsWith("a random IOException")) {
              exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
              e.printStackTrace(exceptionStream);
              allowAlreadyClosed = true;
            } else {
              Rethrow.rethrow(e);
            }
          }
        }
      }
      
      try {
        dir.setRandomIOExceptionRateOnOpen(0.0); // disable exceptions on openInput until next iteration: 
                                                 // or we make slowExists angry and trip a scarier assert!
        iw.close();
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().startsWith("a random IOException")) {
          exceptionStream.println("\nTEST: got expected fake exc:" + e.getMessage());
          e.printStackTrace(exceptionStream);
          try {
            iw.rollback();
          } catch (Throwable t) {}
        } else {
          Rethrow.rethrow(e);
        }
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
}
