package org.apache.lucene.codecs.lucene40;

/*
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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.SegmentInfoReader;
import org.apache.lucene.codecs.SegmentInfoWriter;
import org.apache.lucene.index.IndexWriter; // javadocs
import org.apache.lucene.index.SegmentInfo; // javadocs
import org.apache.lucene.index.SegmentInfos; // javadocs
import org.apache.lucene.store.DataOutput; // javadocs

/**
 * Lucene 4.0 Segment info format.
 * <p>
 * Files:
 * <ul>
 *   <li><tt>.si</tt>: Header, SegVersion, SegSize, IsCompoundFile, Diagnostics, Attributes, Files
 * </ul>
 * </p>
 * Data types:
 * <p>
 * <ul>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>SegSize --&gt; {@link DataOutput#writeInt Int32}</li>
 *   <li>SegVersion --&gt; {@link DataOutput#writeString String}</li>
 *   <li>Files --&gt; {@link DataOutput#writeStringSet Set&lt;String&gt;}</li>
 *   <li>Diagnostics, Attributes --&gt; {@link DataOutput#writeStringStringMap Map&lt;String,String&gt;}</li>
 *   <li>IsCompoundFile --&gt; {@link DataOutput#writeByte Int8}</li>
 * </ul>
 * </p>
 * Field Descriptions:
 * <p>
 * <ul>
 *   <li>SegVersion is the code version that created the segment.</li>
 *   <li>SegSize is the number of documents contained in the segment index.</li>
 *   <li>IsCompoundFile records whether the segment is written as a compound file or
 *       not. If this is -1, the segment is not a compound file. If it is 1, the segment
 *       is a compound file.</li>
 *   <li>Checksum contains the CRC32 checksum of all bytes in the segments_N file up
 *       until the checksum. This is used to verify integrity of the file on opening the
 *       index.</li>
 *   <li>The Diagnostics Map is privately written by {@link IndexWriter}, as a debugging aid,
 *       for each segment it creates. It includes metadata like the current Lucene
 *       version, OS, Java version, why the segment was created (merge, flush,
 *       addIndexes), etc.</li>
 *   <li>Attributes: a key-value map of codec-private attributes.</li>
 *   <li>Files is a list of files referred to by this segment.</li>
 * </ul>
 * </p>
 * 
 * @see SegmentInfos
 * @lucene.experimental
 */
public class Lucene40SegmentInfoFormat extends SegmentInfoFormat {
  private final SegmentInfoReader reader = new Lucene40SegmentInfoReader();
  private final SegmentInfoWriter writer = new Lucene40SegmentInfoWriter();

  /** Sole constructor. */
  public Lucene40SegmentInfoFormat() {
  }
  
  @Override
  public SegmentInfoReader getSegmentInfoReader() {
    return reader;
  }

  @Override
  public SegmentInfoWriter getSegmentInfoWriter() {
    return writer;
  }

  /** File extension used to store {@link SegmentInfo}. */
  public final static String SI_EXTENSION = "si";
  static final String CODEC_NAME = "Lucene40SegmentInfo";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
}
