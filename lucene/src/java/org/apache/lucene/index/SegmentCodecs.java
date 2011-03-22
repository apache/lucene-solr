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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * SegmentCodecs maintains an ordered list of distinct codecs used within a
 * segment. Within a segment on codec is used to write multiple fields while
 * each field could be written by a different codec. To enable codecs per field
 * within a single segment we need to record the distinct codecs and map them to
 * each field present in the segment. SegmentCodecs is created together with
 * {@link SegmentWriteState} for each flush and is maintained in the
 * corresponding {@link SegmentInfo} until it is committed.
 * <p>
 * {@link SegmentCodecs#build(FieldInfos, CodecProvider)} should be used to
 * create a {@link SegmentCodecs} instance during {@link IndexWriter} sessions
 * which creates the ordering of distinct codecs and assigns the
 * {@link FieldInfo#codecId} or in other words, the ord of the codec maintained
 * inside {@link SegmentCodecs}, to the {@link FieldInfo}. This ord is valid
 * only until the current segment is flushed and {@link FieldInfos} for that
 * segment are written including the ord for each field. This ord is later used
 * to get the right codec when the segment is opened in a reader. The
 * {@link Codec} returned from {@link SegmentCodecs#codec()} in turn uses
 * {@link SegmentCodecs} internal structure to select and initialize the right
 * codec for a fields when it is written.
 * <p>
 * Once a flush succeeded the {@link SegmentCodecs} is maintained inside the
 * {@link SegmentInfo} for the flushed segment it was created for.
 * {@link SegmentInfo} writes the name of each codec in {@link SegmentCodecs}
 * for each segment and maintains the order. Later if a segment is opened by a
 * reader this mapping is deserialized and used to create the codec per field.
 * 
 * 
 * @lucene.internal
 */
final class SegmentCodecs implements Cloneable {
  /**
   * internal structure to map codecs to fields - don't modify this from outside
   * of this class!
   */
  Codec[] codecs;
  final CodecProvider provider;
  private final Codec codec = new PerFieldCodecWrapper(this);

  SegmentCodecs(CodecProvider provider, Codec... codecs) {
    this.provider = provider;
    this.codecs = codecs;
  }

  static SegmentCodecs build(FieldInfos infos, CodecProvider provider) {
    final Map<Codec, Integer> codecRegistry = new IdentityHashMap<Codec, Integer>();
    final ArrayList<Codec> codecs = new ArrayList<Codec>();

    for (FieldInfo fi : infos) {
      if (fi.isIndexed) {
        final Codec fieldCodec = provider.lookup(provider
            .getFieldCodec(fi.name));
        Integer ord = codecRegistry.get(fieldCodec);
        if (ord == null) {
          ord = Integer.valueOf(codecs.size());
          codecRegistry.put(fieldCodec, ord);
          codecs.add(fieldCodec);
        }
        fi.setCodecId(ord.intValue());
      }
    }
    return new SegmentCodecs(provider, codecs.toArray(Codec.EMPTY));

  }

  Codec codec() {
    return codec;
  }

  void write(IndexOutput out) throws IOException {
    out.writeVInt(codecs.length);
    for (Codec codec : codecs) {
      out.writeString(codec.name);
    }
  }

  void read(IndexInput in) throws IOException {
    final int size = in.readVInt();
    final ArrayList<Codec> list = new ArrayList<Codec>();
    for (int i = 0; i < size; i++) {
      final String codecName = in.readString();
      final Codec lookup = provider.lookup(codecName);
      list.add(i, lookup);
    }
    codecs = list.toArray(Codec.EMPTY);
  }

  void files(Directory dir, SegmentInfo info, Set<String> files)
      throws IOException {
    final Codec[] codecArray = codecs;
    for (int i = 0; i < codecArray.length; i++) {
      codecArray[i].files(dir, info, ""+i, files);
    }      
      
  }

  @Override
  public String toString() {
    return "SegmentCodecs [codecs=" + Arrays.toString(codecs) + ", provider=" + provider + "]";
  }
}