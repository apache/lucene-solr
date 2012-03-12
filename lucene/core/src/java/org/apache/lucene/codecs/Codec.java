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
import java.util.ServiceLoader; // javadocs

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriterConfig; // javadocs
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.NamedSPILoader;

/**
 * Encodes/decodes an inverted index segment.
 * <p>
 * Note, when extending this class, the name ({@link #getName}) is 
 * written into the index. In order for the segment to be read, the
 * name must resolve to your implementation via {@link #forName(String)}.
 * This method uses Java's 
 * {@link ServiceLoader Service Provider Interface} to resolve codec names.
 * <p>
 * @see ServiceLoader
 */
public abstract class Codec implements NamedSPILoader.NamedSPI {

  private static final NamedSPILoader<Codec> loader =
    new NamedSPILoader<Codec>(Codec.class);

  private final String name;

  public Codec(String name) {
    this.name = name;
  }
  
  /** Returns this codec's name */
  @Override
  public String getName() {
    return name;
  }
  
  /** Populates <code>files</code> with all filenames needed for 
   * the <code>info</code> segment.
   */
  public void files(SegmentInfo info, Set<String> files) throws IOException {
    if (info.getUseCompoundFile()) {
      files.add(IndexFileNames.segmentFileName(info.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
      files.add(IndexFileNames.segmentFileName(info.name, "", IndexFileNames.COMPOUND_FILE_ENTRIES_EXTENSION));
    } else {
      postingsFormat().files(info, "", files);
      storedFieldsFormat().files(info, files);
      termVectorsFormat().files(info, files);
      fieldInfosFormat().files(info, files);
      // TODO: segmentInfosFormat should be allowed to declare additional files
      // if it wants, in addition to segments_N
      docValuesFormat().files(info, files);
      normsFormat().files(info, files);
    }
    // never inside CFS
    liveDocsFormat().files(info, files);
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
  
  /** Encodes/decodes live docs */
  public abstract LiveDocsFormat liveDocsFormat();
  
  /** looks up a codec by name */
  public static Codec forName(String name) {
    return loader.lookup(name);
  }
  
  /** returns a list of all available codec names */
  public static Set<String> availableCodecs() {
    return loader.availableServices();
  }
  
  private static Codec defaultCodec = Codec.forName("Lucene40");
  
  /** expert: returns the default codec used for newly created
   *  {@link IndexWriterConfig}s.
   */
  // TODO: should we use this, or maybe a system property is better?
  public static Codec getDefault() {
    return defaultCodec;
  }
  
  /** expert: sets the default codec used for newly created
   *  {@link IndexWriterConfig}s.
   */
  public static void setDefault(Codec codec) {
    defaultCodec = codec;
  }

  @Override
  public String toString() {
    return name;
  }
}
