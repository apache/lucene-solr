package org.apache.lucene.index.codecs.perfield;

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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.lucene3x.Lucene3xPostingsFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * SegmentFormats maintains an ordered list of distinct formats used within a
 * segment. Within a segment a format is used to write multiple fields while
 * each field could be written by a different format. To enable formats per field
 * within a single segment we need to record the distinct formats and map them to
 * each field present in the segment. SegmentCodecs is created together with
 * {@link SegmentWriteState} for each flush and is maintained in the
 * corresponding {@link SegmentInfo} until it is committed.
 * <p>
 * During indexing {@link FieldInfos} uses {@link SegmentFormatsBuilder} to incrementally
 * build the {@link SegmentFormats} mapping. Once a segment is flushed
 * DocumentsWriter creates a {@link SegmentFormats} instance from
 * {@link FieldInfos#buildSegmentCodecs(boolean)} The {@link FieldInfo#formatId}
 * assigned by {@link SegmentFormatsBuilder} refers to the formats ordinal
 * maintained inside {@link SegmentFormats}. This ord is later used to get the
 * right format when the segment is opened in a reader.The {@link PostingsFormat} returned
 * from {@link SegmentFormats#format()} in turn uses {@link SegmentFormats}
 * internal structure to select and initialize the right format for a fields when
 * it is written.
 * <p>
 * Once a flush succeeded the {@link SegmentFormats} is maintained inside the
 * {@link SegmentInfo} for the flushed segment it was created for.
 * {@link SegmentInfo} writes the name of each format in {@link SegmentFormats}
 * for each segment and maintains the order. Later if a segment is opened by a
 * reader this mapping is deserialized and used to create the format per field.
 * 
 * 
 * @lucene.internal
 */
public final class SegmentFormats implements Cloneable {
  /**
   * internal structure to map formats to fields - don't modify this from outside
   * of this class!
   */
  public final PostingsFormat[] formats;
  public final PerFieldCodec provider;
  private final PostingsFormat format;
  
  public SegmentFormats(PerFieldCodec provider, IndexInput input) throws IOException {
    this(provider, read(input, provider));
  }
  
  public SegmentFormats(PerFieldCodec provider, PostingsFormat... formats) {
    this.provider = provider;
    this.formats = formats;
    if (formats.length == 1 && formats[0] instanceof Lucene3xPostingsFormat) {
      this.format = formats[0]; // hack for backwards break... don't wrap the codec in preflex
    } else {
      this.format = new PerFieldPostingsFormat(this);
    }
  }

  public PostingsFormat format() {
    return format;
  }

  public void write(IndexOutput out) throws IOException {
    out.writeVInt(formats.length);
    for (PostingsFormat format : formats) {
      out.writeString(format.name);
    }
  }

  private static PostingsFormat[] read(IndexInput in, PerFieldCodec provider) throws IOException {
    final int size = in.readVInt();
    final ArrayList<PostingsFormat> list = new ArrayList<PostingsFormat>();
    for (int i = 0; i < size; i++) {
      final String codecName = in.readString();
      final PostingsFormat lookup = provider.lookup(codecName);
      list.add(i, lookup);
    }
    return list.toArray(PostingsFormat.EMPTY);
  }

  public void files(Directory dir, SegmentInfo info, Set<String> files)
      throws IOException {
    final PostingsFormat[] codecArray = formats;
    for (int i = 0; i < codecArray.length; i++) {
      codecArray[i].files(dir, info, i, files);
    }      
      
  }

  @Override
  public String toString() {
    return "SegmentFormats [formats=" + Arrays.toString(formats) + ", provider=" + provider + "]";
  }
  
  /**
   * Used in {@link FieldInfos} to incrementally build the format ID mapping for
   * {@link FieldInfo} instances.
   * <p>
   * Note: this class is not thread-safe
   * </p>
   * @see FieldInfo#getFormatId()
   */
  public final static class SegmentFormatsBuilder {
    private final Map<PostingsFormat, Integer> formatRegistry = new IdentityHashMap<PostingsFormat, Integer>();
    private final ArrayList<PostingsFormat> formats = new ArrayList<PostingsFormat>();
    private final PerFieldCodec provider;

    private SegmentFormatsBuilder(PerFieldCodec provider) {
      this.provider = provider;
    }
    
    public static SegmentFormatsBuilder create(PerFieldCodec provider) {
      return new SegmentFormatsBuilder(provider);
    }
    
    public SegmentFormatsBuilder tryAddAndSet(FieldInfo fi) {
      if (fi.getFormatId() == FieldInfo.UNASSIGNED_FORMAT_ID) {
        final PostingsFormat fieldCodec = provider.lookup(provider
            .getPostingsFormat(fi.name));
        Integer ord = formatRegistry.get(fieldCodec);
        if (ord == null) {
          ord = Integer.valueOf(formats.size());
          formatRegistry.put(fieldCodec, ord);
          formats.add(fieldCodec);
        }
        fi.setFormatId(ord.intValue());
      }
      return this;
    }
    
    public SegmentFormats build() {
      return new SegmentFormats(provider, formats.toArray(PostingsFormat.EMPTY));
    }
    
    public SegmentFormatsBuilder clear() {
      formatRegistry.clear();
      formats.clear();
      return this;
    }
  }
}