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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.TermsConsumer;
import org.apache.lucene.index.values.DocValues;
import org.apache.lucene.index.values.Writer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeSource;

/**
 * A codec that adds DocValues support to a given codec transparently.
 */
public class DocValuesCodec extends Codec {
  private final Map<String, WrappingFieldsConsumer> consumers = new HashMap<String, WrappingFieldsConsumer>();
  private final Codec other;

  public DocValuesCodec(Codec other) {
    this.name = "docvalues_" + other.name;
    this.other = other;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    WrappingFieldsConsumer consumer;
    if ((consumer = consumers.get(state.segmentName)) == null) {
      consumer = new WrappingFieldsConsumer(other);
    }
    consumer.state = state; // nocommit this is a hack and only necessary since
                            // we want to initialized the wrapped
    // fieldsConsumer lazily with a SegmentWriteState created after the docvalue
    // ones is. We should fix this in DocumentWriter I guess. See
    // DocFieldProcessor too!
    return consumer;
  }

  private static class WrappingFieldsConsumer extends FieldsConsumer {
    SegmentWriteState state;
    private final List<DocValuesConsumer> docValuesConsumers = new ArrayList<DocValuesConsumer>();
    private FieldsConsumer wrappedConsumer;
    private final Codec other;

    public WrappingFieldsConsumer(Codec other) {
      this.other = other;
    }

    @Override
    public void close() throws IOException {
      synchronized (this) {
        if (wrappedConsumer != null)
          wrappedConsumer.close();
      }
    }

    @Override
    public synchronized DocValuesConsumer addValuesField(FieldInfo field)
        throws IOException {
      DocValuesConsumer consumer = DocValuesConsumer.create(state.segmentName,
      // TODO: set comparator here
      //TODO can we have a compound file per segment and codec for docvalues?
          state.directory, field, state.codecId +"-"+ field.number, null);
      docValuesConsumers.add(consumer);
      return consumer;
    }

    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
      synchronized (this) {
        if (wrappedConsumer == null)
          wrappedConsumer = other.fieldsConsumer(state);
      }
      return wrappedConsumer.addField(field);
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    Directory dir = state.dir;
    Set<String> files = new HashSet<String>();

    other.files(dir, state.segmentInfo, state.codecId, files);
    for (String string : files) {
      if (dir.fileExists(string))
        return new WrappingFielsdProducer(state, other.fieldsProducer(state));
    }
    return new WrappingFielsdProducer(state, FieldsProducer.EMPTY);

  }

  @Override
  public void files(Directory dir, SegmentInfo segmentInfo, String codecId,
      Set<String> files) throws IOException {
    Set<String> otherFiles = new HashSet<String>();
    other.files(dir, segmentInfo, codecId, otherFiles);
    for (String string : otherFiles) { // under some circumstances we only write
                                       // DocValues
                                       // so other files will be added even if
                                       // they don't exist
      if (dir.fileExists(string))
        files.add(string);
    }
    //TODO can we have a compound file per segment and codec for docvalues?
    for (String file : dir.listAll()) {
      if (file.startsWith(segmentInfo.name+"_" + codecId)
          && (file.endsWith(Writer.DATA_EXTENSION) || file
              .endsWith(Writer.INDEX_EXTENSION))) {
        files.add(file);
      }
    }

  }

  @Override
  public void getExtensions(Set<String> extensions) {
    other.getExtensions(extensions);
    extensions.add(Writer.DATA_EXTENSION);
    extensions.add(Writer.INDEX_EXTENSION);
  }

  static class WrappingFielsdProducer extends DocValuesProducerBase {

    private final FieldsProducer other;

    WrappingFielsdProducer(SegmentReadState state, FieldsProducer other)
        throws IOException {
      super(state.segmentInfo, state.dir, state.fieldInfos, state.codecId);
      this.other = other;
    }

    @Override
    public void close() throws IOException {
      try {
        other.close();
      } finally {
        super.close();
      }
    }

    @Override
    public void loadTermsIndex(int indexDivisor) throws IOException {
      other.loadTermsIndex(indexDivisor);
    }

    @Override
    public FieldsEnum iterator() throws IOException {
      return new WrappingFieldsEnum(other.iterator(), docValues.entrySet()
          .iterator());
    }

    @Override
    public Terms terms(String field) throws IOException {
      return other.terms(field);
    }
  }

  static abstract class NameValue<V> {
    String name;
    V value;

    NameValue<?> smaller(NameValue<?> other) throws IOException {
      if (other.name == null) {
        if (this.name == null) {
          return null;
        }
        return this;
      } else if (this.name == null) {
        return other;
      }
      final int res = this.name.compareTo(other.name);
      if (res < 0)
        return this;
      if (res == 0)
        other.name = this.name;
      return other;
    }

    abstract NameValue<V> next() throws IOException;
  }

  static class FieldsEnumNameValue extends NameValue<FieldsEnum> {
    @Override
    NameValue<FieldsEnum> next() throws IOException {
      name = value.next();
      return this;
    }

  }

  static class DocValueNameValue extends NameValue<DocValues> {
    Iterator<Entry<String, DocValues>> iter;

    @Override
    NameValue<DocValues> next() {
      if (iter.hasNext()) {
        Entry<String, DocValues> next = iter.next();
        value = next.getValue();
        name = next.getKey();
      } else {
        name = null;
      }
      return this;
    }

  }

  static class WrappingFieldsEnum extends FieldsEnum {
    private final DocValueNameValue docValues = new DocValueNameValue();
    private final NameValue<FieldsEnum> fieldsEnum = new FieldsEnumNameValue();
    private NameValue<?> coordinator;

    @Override
    public AttributeSource attributes() {
      return fieldsEnum.value.attributes();
    }

    public WrappingFieldsEnum(FieldsEnum wrapped,
        Iterator<Entry<String, DocValues>> docValues) {
      this.docValues.iter = docValues;
      this.fieldsEnum.value = wrapped;
      coordinator = null;

    }

    @Override
    public DocValues docValues() throws IOException {
      if (docValues.name == coordinator.name)
        return docValues.value;
      return null;
    }

    @Override
    public String next() throws IOException {
      if (coordinator == null) {
        coordinator = fieldsEnum.next().smaller(docValues.next());
        // old = coordinator.name;
      } else {
        String current = coordinator.name;
        if (current == docValues.name) {
          docValues.next();
        }
        if (current == fieldsEnum.name) {
          fieldsEnum.next();
        }
        coordinator = docValues.smaller(fieldsEnum);

      }
      return coordinator == null ? null : coordinator.name;

    }

    @Override
    public TermsEnum terms() throws IOException {
      if (fieldsEnum.name == coordinator.name)
        return fieldsEnum.value.terms();
      return null;
    }

  }

}
