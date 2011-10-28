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

/** Looks up codecs by name, and specifies the default codec
 *  to use when writing new segments.  You subclass
 *  this, instantiate it, then pass this instance to 
 *  IndexReader/IndexWriter (via package private APIs) 
 *  to use different codecs when reading & writing segments. 
 *
 *  @lucene.experimental */
public abstract class CodecProvider {
  private SegmentInfosWriter infosWriter = new DefaultSegmentInfosWriter();
  private SegmentInfosReader infosReader = new DefaultSegmentInfosReader();

  public SegmentInfosWriter getSegmentInfosWriter() {
    return infosWriter;
  }
  
  public SegmentInfosReader getSegmentInfosReader() {
    return infosReader;
  }
  
  static private CodecProvider defaultCodecs = new CoreCodecProvider();

  public static CodecProvider getDefault() {
    return defaultCodecs;
  }

  /** For testing only
   *  @lucene.internal */
  public static void setDefault(CodecProvider cp) {
    defaultCodecs = cp;
  }
  
  public abstract Codec lookup(String name);
  public abstract Codec getDefaultCodec();
}
