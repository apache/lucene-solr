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

import org.apache.lucene.store.Directory;
import org.apache.lucene.index.values.Ints;
import org.apache.lucene.index.values.Floats;
import org.apache.lucene.index.values.Bytes;
import org.apache.lucene.index.values.ValuesAttribute;
import org.apache.lucene.index.values.Writer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FloatsRef;
import org.apache.lucene.util.LongsRef;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;


/**
 * This is a DocConsumer that gathers all fields under the
 * same name, and calls per-field consumers to process field
 * by field.  This class doesn't doesn't do any "real" work
 * of its own: it just forwards the fields to a
 * DocFieldConsumer.
 */

final class DocFieldProcessor extends DocConsumer {

  final DocumentsWriter docWriter;
  final FieldInfos fieldInfos = new FieldInfos();
  final DocFieldConsumer consumer;
  final StoredFieldsWriter fieldsWriter;
  final private Map<String,IndexValuesProcessor> indexValues = new HashMap<String,IndexValuesProcessor>();

  synchronized IndexValuesProcessor getProcessor(Directory dir, String segment, String name, ValuesAttribute attr, FieldInfo fieldInfo)
    throws IOException {
    if(attr == null)
      return null;
    IndexValuesProcessor p = indexValues.get(name);
    if (p == null) {
        org.apache.lucene.index.values.Values v = attr.type();
        final String id = segment + "_" + fieldInfo.number;
        switch(v) {
        case PACKED_INTS:
          p = new IntValuesProcessor(dir, id, false);
          break;
        case PACKED_INTS_FIXED:
          p = new IntValuesProcessor(dir, id, true);
          break;
        case SIMPLE_FLOAT_4BYTE:
          p = new FloatValuesProcessor(dir, id, 4);
          break;
        case SIMPLE_FLOAT_8BYTE:
          p = new FloatValuesProcessor(dir, id, 8);
          break;
        case BYTES_FIXED_STRAIGHT:
          p = new BytesValuesProcessor(dir, id, true, null, Bytes.Mode.STRAIGHT);
          break;
        case BYTES_FIXED_DEREF:
          p = new BytesValuesProcessor(dir, id, true, null, Bytes.Mode.DEREF);
          break;
        case BYTES_FIXED_SORTED:
          p = new BytesValuesProcessor(dir, id, true, attr.bytesComparator(), Bytes.Mode.SORTED);
          break;
        case BYTES_VAR_STRAIGHT:
          p = new BytesValuesProcessor(dir, id, false, null, Bytes.Mode.STRAIGHT);
          break;
        case BYTES_VAR_DEREF:
          p = new BytesValuesProcessor(dir, id, false, null, Bytes.Mode.DEREF);
          break;
        case BYTES_VAR_SORTED:
          p = new BytesValuesProcessor(dir, id, false, attr.bytesComparator(), Bytes.Mode.SORTED);
          break;
        }
        fieldInfo.setIndexValues(v);
        indexValues.put(name, p);
    }

    return p;
  }

  static abstract class IndexValuesProcessor {
    public abstract void add(int docID, String name, ValuesAttribute attr) throws IOException;
    public abstract void finish(int docCount) throws IOException;
    public abstract void files(Collection<String> files) throws IOException;
  }

  static class FloatValuesProcessor extends IndexValuesProcessor {
    private final Writer writer;
    private final String id;

    public FloatValuesProcessor(Directory dir, String id, int precision) throws IOException {
      this.id = id;
      writer = Floats.getWriter(dir, id, precision);
    }

    @Override
    public void add(int docID, String name, ValuesAttribute attr) throws IOException {
        final FloatsRef floats = attr.floats();
        if(floats != null) {
          writer.add(docID, floats.get());
          return;
        }
      throw new IllegalArgumentException("could not extract float/double from field " + name);
    }

    @Override
    public void finish(int docCount) throws IOException {
      writer.finish(docCount);
    }

    @Override
    public void files(Collection<String> files) {
      Floats.files(id, files);
    }
  }

  static class IntValuesProcessor extends IndexValuesProcessor {
    private final Writer writer;
    private final String id;

    public IntValuesProcessor(Directory dir, String id, boolean fixedArray) throws IOException {
      this.id = id;
      writer = Ints.getWriter(dir, id, fixedArray);
    }

    @Override
      public void add(int docID, String name, ValuesAttribute attr) throws IOException {
        final LongsRef ints = attr.ints();
        if(ints != null) {
          writer.add(docID, ints.get());
          return;
        }
      throw new IllegalArgumentException("could not extract int/long from field " + name);
    }

    @Override
    public void finish(int docCount) throws IOException {
      writer.finish(docCount);
    }

    @Override
    public void files(Collection<String> files) throws IOException {
      Ints.files(id, files);
    }
  }

  static class BytesValuesProcessor extends IndexValuesProcessor {
    private final Writer writer;
    private final String id;
    private final Directory dir;

    public BytesValuesProcessor(Directory dir, String id, boolean fixedSize, Comparator<BytesRef> comp, Bytes.Mode mode) throws IOException {
      this.id = id;
      writer = Bytes.getWriter(dir, id, mode,comp, fixedSize);
      this.dir = dir;
    }

    // nocommit -- make this thread private and not sync'd
    @Override
    public synchronized void add(int docID, String name, ValuesAttribute attr) throws IOException {
      final BytesRef bytes = attr.bytes();
      if(bytes != null) {
        writer.add(docID, bytes);
        return;
      }
      throw new IllegalArgumentException("could not extract byte[] from field " + name);
    }

    @Override
    public void finish(int docCount) throws IOException {
      writer.finish(docCount);
    }

    @Override
    public void files(Collection<String> files) throws IOException {
      Bytes.files(dir, id, files);
    }
  }

  public DocFieldProcessor(DocumentsWriter docWriter, DocFieldConsumer consumer) {
    this.docWriter = docWriter;
    this.consumer = consumer;
    consumer.setFieldInfos(fieldInfos);
    fieldsWriter = new StoredFieldsWriter(docWriter, fieldInfos);
  }

  @Override
  public void closeDocStore(SegmentWriteState state) throws IOException {
    consumer.closeDocStore(state);
    fieldsWriter.closeDocStore(state);
  }

  @Override
  public void flush(Collection<DocConsumerPerThread> threads, SegmentWriteState state) throws IOException {

    Map<DocFieldConsumerPerThread, Collection<DocFieldConsumerPerField>> childThreadsAndFields = new HashMap<DocFieldConsumerPerThread, Collection<DocFieldConsumerPerField>>();
    for ( DocConsumerPerThread thread : threads) {
      DocFieldProcessorPerThread perThread = (DocFieldProcessorPerThread) thread;
      childThreadsAndFields.put(perThread.consumer, perThread.fields());
      perThread.trimFields(state);
    }
    fieldsWriter.flush(state);
    consumer.flush(childThreadsAndFields, state);

    for(IndexValuesProcessor p : indexValues.values()) {
      if (p != null) {
        p.finish(state.numDocs);
        p.files(state.flushedFiles);
      }
    }
    indexValues.clear();

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    final String fileName = IndexFileNames.segmentFileName(state.segmentName, "", IndexFileNames.FIELD_INFOS_EXTENSION);
    fieldInfos.write(state.directory, fileName);
    state.flushedFiles.add(fileName);
  }

  @Override
  public void abort() {
    fieldsWriter.abort();
    consumer.abort();
  }

  @Override
  public boolean freeRAM() {
    return consumer.freeRAM();
  }

  @Override
  public DocConsumerPerThread addThread(DocumentsWriterThreadState threadState) throws IOException {
    return new DocFieldProcessorPerThread(threadState, this);
  }
}
