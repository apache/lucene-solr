package org.apache.lucene.index.codecs;

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

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.IdentityHashMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.io.IOException;

import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;


/** Simple Codec that dispatches field-specific codecs.
 *  You must ensure every field you index has a Codec, or
 *  the defaultCodec is non null.  Also, the separate
 *  codecs cannot conflict on file names.
 *
 * @lucene.experimental */
public class PerFieldCodecWrapper extends Codec {
  private final Map<String,Codec> fields = new IdentityHashMap<String,Codec>();
  private final Codec defaultCodec;

  public PerFieldCodecWrapper(Codec defaultCodec) {
    name = "PerField";
    this.defaultCodec = defaultCodec;
  }

  public void add(String field, Codec codec) {
    fields.put(field, codec);
  }

  public Codec getCodec(String field) {
    Codec codec = fields.get(field);
    if (codec != null) {
      return codec;
    } else {
      return defaultCodec;
    }
  }
      
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new FieldsWriter(state);
  }

  private class FieldsWriter extends FieldsConsumer {
    private final SegmentWriteState state;
    private final Map<Codec,FieldsConsumer> codecs = new HashMap<Codec,FieldsConsumer>();
    private final Set<String> fieldsSeen = new TreeSet<String>();

    public FieldsWriter(SegmentWriteState state) {
      this.state = state;
    }

    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
      fieldsSeen.add(field.name);
      Codec codec = getCodec(field.name);

      FieldsConsumer fields = codecs.get(codec);
      if (fields == null) {
        fields = codec.fieldsConsumer(state);
        codecs.put(codec, fields);
      }
      return fields.addField(field);
    }

    @Override
    public void close() throws IOException {
      Iterator<FieldsConsumer> it = codecs.values().iterator();
      IOException err = null;
      while(it.hasNext()) {
        try {
          it.next().close();
        } catch (IOException ioe) {
          // keep first IOException we hit but keep
          // closing the rest
          if (err == null) {
            err = ioe;
          }
        }
      }
      if (err != null) {
        throw err;
      }
    }
  }

  private class FieldsReader extends FieldsProducer {

    private final Set<String> fields = new TreeSet<String>();
    private final Map<Codec,FieldsProducer> codecs = new HashMap<Codec,FieldsProducer>();

    public FieldsReader(Directory dir, FieldInfos fieldInfos,
                        SegmentInfo si, int readBufferSize,
                        int indexDivisor) throws IOException {

      final int fieldCount = fieldInfos.size();
      for(int i=0;i<fieldCount;i++) {
        FieldInfo fi = fieldInfos.fieldInfo(i);
        if (fi.isIndexed) {
          fields.add(fi.name);
          Codec codec = getCodec(fi.name);
          if (!codecs.containsKey(codec)) {
            codecs.put(codec, codec.fieldsProducer(new SegmentReadState(dir, si, fieldInfos, readBufferSize, indexDivisor)));
          }
        }
      }
    }

    private final class FieldsIterator extends FieldsEnum {
      private final Iterator<String> it;
      private String current;

      public FieldsIterator() {
        it = fields.iterator();
      }

      @Override
      public String next() {
        if (it.hasNext()) {
          current = it.next();
        } else {
          current = null;
        }

        return current;
      }

      @Override
      public TermsEnum terms() throws IOException {
        Terms terms = codecs.get(getCodec(current)).terms(current);
        if (terms != null) {
          return terms.iterator();
        } else {
          return null;
        }
      }
    }
      
    @Override
    public FieldsEnum iterator() throws IOException {
      return new FieldsIterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      Codec codec = getCodec(field);

      FieldsProducer fields = codecs.get(codec);
      assert fields != null;
      return fields.terms(field);
    }

    @Override
    public void close() throws IOException {
      Iterator<FieldsProducer> it = codecs.values().iterator();
      IOException err = null;
      while(it.hasNext()) {
        try {
          it.next().close();
        } catch (IOException ioe) {
          // keep first IOException we hit but keep
          // closing the rest
          if (err == null) {
            err = ioe;
          }
        }
      }
      if (err != null) {
        throw err;
      }
    }

    @Override
    public void loadTermsIndex(int indexDivisor) throws IOException {
      Iterator<FieldsProducer> it = codecs.values().iterator();
      while(it.hasNext()) {
        it.next().loadTermsIndex(indexDivisor);
      }
    }
  }

  public FieldsProducer fieldsProducer(SegmentReadState state)
    throws IOException {
    return new FieldsReader(state.dir, state.fieldInfos, state.segmentInfo, state.readBufferSize, state.termsIndexDivisor);
  }

  @Override
  public void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    Iterator<Codec> it = fields.values().iterator();
    Set<Codec> seen = new HashSet<Codec>();
    while(it.hasNext()) {
      final Codec codec = it.next();
      if (!seen.contains(codec)) {
        seen.add(codec);
        codec.files(dir, info, files);
      }
    }
  }

  @Override
  public void getExtensions(Set<String> extensions) {
    Iterator<Codec> it = fields.values().iterator();
    while(it.hasNext()) {
      final Codec codec = it.next();
      codec.getExtensions(extensions);
    }
  }
}
