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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsBaseFormat;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.index.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.index.codecs.pulsing.PulsingPostingsFormat;
import org.apache.lucene.index.codecs.simpletext.SimpleTextPostingsFormat;
import org.apache.lucene.store.Directory;

/** @lucene.experimental */
public abstract class PostingsFormat {
  public static final PostingsFormat[] EMPTY = new PostingsFormat[0];
  /** Unique name that's used to retrieve this codec when
   *  reading the index */
  public final String name;
  
  protected PostingsFormat(String name) {
    this.name = name;
  }

  /** Writes a new segment */
  public abstract FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException;

  /** Reads a segment.  NOTE: by the time this call
   *  returns, it must hold open any files it will need to
   *  use; else, those files may be deleted. */
  public abstract FieldsProducer fieldsProducer(SegmentReadState state) throws IOException;

  /**
   * Gathers files associated with this segment
   * 
   * @param dir the {@link Directory} this segment was written to
   * @param segmentInfo the {@link SegmentInfo} for this segment 
   * @param id the codec id within this segment
   * @param files the of files to add the codec files to.
   */
  public abstract void files(Directory dir, SegmentInfo segmentInfo, int id, Set<String> files) throws IOException;

  @Override
  public String toString() {
    return "PostingsFormat(name=" + name + ")";
  }
  
  public static PostingsFormat forName(String name) {
    // TODO: Uwe fix me!
    return CORE_FORMATS.get(name);
  }
  /** Lucene's core postings formats.
   *  @lucene.internal
   */
  @Deprecated
  public static final Map<String,PostingsFormat> CORE_FORMATS = new HashMap<String,PostingsFormat>();
  static {
    CORE_FORMATS.put("Lucene40", new Lucene40PostingsFormat());
    CORE_FORMATS.put("Pulsing", new PulsingPostingsFormat(new Lucene40PostingsBaseFormat(), 1));
    CORE_FORMATS.put("SimpleText", new SimpleTextPostingsFormat());
    CORE_FORMATS.put("Memory", new MemoryPostingsFormat());
  }
}
