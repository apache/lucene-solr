package org.apache.lucene.codecs;

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
import java.util.Set;

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.util.NamedSPILoader;

/** 
 * Encodes/decodes terms, postings, and proximity data.
 * @lucene.experimental */
public abstract class PostingsFormat implements NamedSPILoader.NamedSPI {

  private static final NamedSPILoader<PostingsFormat> loader =
    new NamedSPILoader<PostingsFormat>(PostingsFormat.class);

  public static final PostingsFormat[] EMPTY = new PostingsFormat[0];
  /** Unique name that's used to retrieve this format when
   *  reading the index.
   */
  private final String name;
  
  protected PostingsFormat(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
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
   * @param segmentInfo the {@link SegmentInfo} for this segment 
   * @param segmentSuffix the format's suffix within this segment
   * @param files the of files to add the codec files to.
   */
  public abstract void files(SegmentInfo segmentInfo, String segmentSuffix, Set<String> files) throws IOException;

  @Override
  public String toString() {
    return "PostingsFormat(name=" + name + ")";
  }
  
  /** looks up a format by name */
  public static PostingsFormat forName(String name) {
    return loader.lookup(name);
  }
  
  /** returns a list of all available format names */
  public static Set<String> availablePostingsFormats() {
    return loader.availableServices();
  }
  
}
