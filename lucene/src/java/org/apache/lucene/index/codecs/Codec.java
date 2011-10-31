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
import org.apache.lucene.index.codecs.lucene3x.Lucene3xCodec;
import org.apache.lucene.index.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.store.Directory;

/**
 * Encodes/decodes an inverted index segment
 */
public abstract class Codec {
  private final String name;

  public Codec(String name) {
    this.name = name;
  }
  
  public String getName() {
    return name;
  }
  
  public void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    postingsFormat().files(dir, info, 0, files);
    //TODO: not yet fieldsFormat().files(dir, info, files);
    docValuesFormat().files(dir, info, 0, files);
  }
  
  /** Encodes/decodes postings */
  public abstract PostingsFormat postingsFormat();
  
  /** Encodes/decodes docvalues */
  public abstract DocValuesFormat docValuesFormat();
  
  /** Encodes/decodes stored fields, term vectors, fieldinfos */
  public abstract FieldsFormat fieldsFormat();
  
  // nocommit: make abstract
  public SegmentInfosFormat segmentInfosFormat() {
    return siFormat;
  }
  
  // nocommit
  private final SegmentInfosFormat siFormat = new DefaultSegmentInfosFormat();
  
  /** looks up a codec by name */
  public static Codec forName(String name) {
    // TODO: Uwe fix this crap, this is just a stub!
    return CORE_CODECS.get(name);
  }
  
  @Deprecated
  private static final Map<String,Codec> CORE_CODECS = new HashMap<String,Codec>();
  static {
    CORE_CODECS.put("Lucene40", new Lucene40Codec());
    CORE_CODECS.put("Lucene3x", new Lucene3xCodec());
  }
  
  private static Codec defaultCodec = Codec.forName("Lucene40");
  
  // nocommit: should we remove these? 
  public static Codec getDefault() {
    return defaultCodec;
  }
  
  public static void setDefault(Codec codec) {
    defaultCodec = codec;
  }
}
