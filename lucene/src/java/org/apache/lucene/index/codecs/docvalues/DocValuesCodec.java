package org.apache.lucene.index.codecs.docvalues;

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
import java.util.Comparator;
import java.util.Set;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.PerDocConsumer;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.index.values.Writer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/**
 * A codec that adds DocValues support to a given codec transparently.
 * @lucene.experimental
 */
public class DocValuesCodec extends Codec {
  private final Codec other;
  private final Comparator<BytesRef> comparator;

  public DocValuesCodec(Codec other, Comparator<BytesRef> comparator) {
    this.name = other.name;
    this.other = other;
    this.comparator = comparator;
  }

  public DocValuesCodec(Codec other) {
    this(other, null);
  }

  @Override
  public PerDocConsumer docsConsumer(final PerDocWriteState state)
      throws IOException {
    return new PerDocConsumer() {

      @Override
      public void close() throws IOException {
      }

      @Override
      public DocValuesConsumer addValuesField(FieldInfo field)
          throws IOException {
        final DocValuesConsumer consumer = Writer.create(field.getDocValues(),
            docValuesId(state.segmentName, state.codecId, field.number),
            // TODO can we have a compound file per segment and codec for
            // docvalues?
            state.directory, comparator, state.bytesUsed);
        return consumer;
      }
    };
  }

  @Override
  public PerDocValues docsProducer(SegmentReadState state) throws IOException {
    try {
    return new DocValuesProducerBase(state.segmentInfo, state.dir, state.fieldInfos, state.codecId);
    }catch (IOException e) {
      return new DocValuesProducerBase(state.segmentInfo, state.dir, state.fieldInfos, state.codecId);
    }
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    return other.fieldsConsumer(state);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    return other.fieldsProducer(state);
  }
  
  static String docValuesId(String segmentsName, int codecID, int fieldId) {
    return segmentsName + "_" + codecID + "-" + fieldId;
  }

  @Override
  public void files(Directory dir, SegmentInfo segmentInfo, int codecId,
      Set<String> files) throws IOException {
    FieldInfos fieldInfos = segmentInfo.getFieldInfos();
    boolean indexed = false;
    for (FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.getCodecId() == codecId) {
        indexed |= fieldInfo.isIndexed;
        if (fieldInfo.hasDocValues()) {
          String filename = docValuesId(segmentInfo.name, codecId, fieldInfo.number);
          switch (fieldInfo.getDocValues()) {
          case BYTES_FIXED_DEREF:
          case BYTES_VAR_DEREF:
          case BYTES_VAR_SORTED:
          case BYTES_FIXED_SORTED:
          case BYTES_VAR_STRAIGHT:
            files.add(IndexFileNames.segmentFileName(filename, "",
                Writer.INDEX_EXTENSION));
            assert dir.fileExists(IndexFileNames.segmentFileName(filename, "",
                Writer.INDEX_EXTENSION));
          case BYTES_FIXED_STRAIGHT:
          case FLOAT_32:
          case FLOAT_64:
          case INTS:
            files.add(IndexFileNames.segmentFileName(filename, "",
                Writer.DATA_EXTENSION));
            assert dir.fileExists(IndexFileNames.segmentFileName(filename, "",
                Writer.DATA_EXTENSION));
            break;
           default:
             assert false;
          }
        }

      }
    }
    if (indexed) {
      other.files(dir, segmentInfo, codecId, files);
    }
  }

  @Override
  public void getExtensions(Set<String> extensions) {
    other.getExtensions(extensions);
    extensions.add(Writer.DATA_EXTENSION);
    extensions.add(Writer.INDEX_EXTENSION);
  }
}
