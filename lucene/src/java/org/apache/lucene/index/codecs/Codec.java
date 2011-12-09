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
import java.util.Set;

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.NamedSPILoader;
import org.apache.lucene.store.Directory;

/**
 * Encodes/decodes an inverted index segment
 */
public abstract class Codec implements NamedSPILoader.NamedSPI {

  private static final NamedSPILoader<Codec> loader =
    new NamedSPILoader<Codec>(Codec.class);

  private final String name;

  public Codec(String name) {
    this.name = name;
  }
  
  @Override
  public String getName() {
    return name;
  }
  
  public void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    postingsFormat().files(dir, info, "", files);
    storedFieldsFormat().files(dir, info, files);
    termVectorsFormat().files(dir, info, files);
    fieldInfosFormat().files(dir, info, files);
    // TODO: segmentInfosFormat should be allowed to declare additional files
    // if it wants, in addition to segments_N
    docValuesFormat().files(dir, info, files);
    normsFormat().files(dir, info, files);
  }
  
  /** Encodes/decodes postings */
  public abstract PostingsFormat postingsFormat();
  
  /** Encodes/decodes docvalues */
  public abstract DocValuesFormat docValuesFormat();
  
  /** Encodes/decodes stored fields */
  public abstract StoredFieldsFormat storedFieldsFormat();
  
  /** Encodes/decodes term vectors */
  public abstract TermVectorsFormat termVectorsFormat();
  
  /** Encodes/decodes field infos file */
  public abstract FieldInfosFormat fieldInfosFormat();
  
  /** Encodes/decodes segments file */
  public abstract SegmentInfosFormat segmentInfosFormat();
  
  /** Encodes/decodes document normalization values */
  public abstract NormsFormat normsFormat();
  
  /** looks up a codec by name */
  public static Codec forName(String name) {
    return loader.lookup(name);
  }
  
  /** returns a list of all available codec names */
  public static Set<String> availableCodecs() {
    return loader.availableServices();
  }
  
  private static Codec defaultCodec = Codec.forName("Lucene40");
  
  // TODO: should we use this, or maybe a system property is better?
  public static Codec getDefault() {
    return defaultCodec;
  }
  
  public static void setDefault(Codec codec) {
    defaultCodec = codec;
  }

  @Override
  public String toString() {
    return name;
  }
}
