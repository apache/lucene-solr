package org.apache.lucene.codecs.lucene40;

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

import org.apache.lucene.codecs.Codec; // javadocs
import org.apache.lucene.codecs.LiveDocsFormat; // javadocs
import org.apache.lucene.codecs.SegmentInfosFormat;
import org.apache.lucene.codecs.SegmentInfosReader;
import org.apache.lucene.codecs.SegmentInfosWriter;
import org.apache.lucene.codecs.StoredFieldsFormat; // javadocs
import org.apache.lucene.codecs.TermVectorsFormat; // javadocs
import org.apache.lucene.index.FieldInfo.IndexOptions; // javadocs
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter; // javadocs
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos; // javadocs
import org.apache.lucene.store.DataOutput; // javadocs

/**
 * Lucene 4.0 Segments format.
 * <p>
 * Files:
 * <ul>
 *   <li><tt>segments.gen</tt>: described in {@link SegmentInfos}
 *   <li><tt>segments_N</tt>: Format, Codec, Version, NameCounter, SegCount,
 *    &lt;SegVersion, SegName, SegSize, DelGen, DocStoreOffset, [DocStoreSegment,
 *    DocStoreIsCompoundFile], NumField, NormGen<sup>NumField</sup>, 
 *    IsCompoundFile, DeletionCount, HasProx, SegCodec Diagnostics, 
 *    HasVectors&gt;<sup>SegCount</sup>, CommitUserData, Checksum
 * </ul>
 * </p>
 * Data types:
 * <p>
 * <ul>
 *   <li>Format, NameCounter, SegCount, SegSize, NumField, DocStoreOffset,
 *       DeletionCount --&gt; {@link DataOutput#writeInt Int32}</li>
 *   <li>Version, DelGen, NormGen, Checksum --&gt; 
 *       {@link DataOutput#writeLong Int64}</li>
 *   <li>SegVersion, SegName, DocStoreSegment, Codec, SegCodec --&gt; 
 *       {@link DataOutput#writeString String}</li>
 *   <li>Diagnostics, CommitUserData --&gt; 
 *       {@link DataOutput#writeStringStringMap Map&lt;String,String&gt;}</li>
 *   <li>IsCompoundFile, DocStoreIsCompoundFile, HasProx,
 *       HasVectors --&gt; {@link DataOutput#writeByte Int8}</li>
 * </ul>
 * </p>
 * Field Descriptions:
 * <p>
 * <ul>
 *   <li>Format is {@link SegmentInfos#FORMAT_4_0}.</li>
 *   <li>Codec is "Lucene40", its the {@link Codec} that wrote this particular segments file.</li>
 *   <li>Version counts how often the index has been changed by adding or deleting
 *       documents.</li>
 *   <li>NameCounter is used to generate names for new segment files.</li>
 *   <li>SegVersion is the code version that created the segment.</li>
 *   <li>SegName is the name of the segment, and is used as the file name prefix for
 *       all of the files that compose the segment's index.</li>
 *   <li>SegSize is the number of documents contained in the segment index.</li>
 *   <li>DelGen is the generation count of the deletes file. If this is -1,
 *       there are no deletes. Anything above zero means there are deletes 
 *       stored by {@link LiveDocsFormat}.</li>
 *   <li>NumField is the size of the array for NormGen, or -1 if there are no
 *       NormGens stored.</li>
 *   <li>NormGen records the generation of the separate norms files. If NumField is
 *       -1, there are no normGens stored and all assumed to be -1. The generation 
 *       then has the same meaning as delGen (above).</li>
 *   <li>IsCompoundFile records whether the segment is written as a compound file or
 *       not. If this is -1, the segment is not a compound file. If it is 1, the segment
 *       is a compound file. Else it is 0, which means we check filesystem to see if
 *       _X.cfs exists.</li>
 *   <li>DocStoreOffset, DocStoreSegment, DocStoreIsCompoundFile: If DocStoreOffset
 *       is -1, this segment has its own doc store (stored fields values and term
 *       vectors) files and DocStoreSegment and DocStoreIsCompoundFile are not stored.
 *       In this case all files for  {@link StoredFieldsFormat stored field values} and
 *       {@link TermVectorsFormat term vectors} will be stored with this segment. 
 *       Otherwise, DocStoreSegment is the name of the segment that has the shared doc 
 *       store files; DocStoreIsCompoundFile is 1 if that segment is stored in compound 
 *       file format (as a <tt>.cfx</tt> file); and DocStoreOffset is the starting document 
 *       in the shared doc store files where this segment's documents begin. In this case, 
 *       this segment does not store its own doc store files but instead shares a single 
 *       set of these files with other segments.</li>
 *   <li>Checksum contains the CRC32 checksum of all bytes in the segments_N file up
 *       until the checksum. This is used to verify integrity of the file on opening the
 *       index.</li>
 *   <li>DeletionCount records the number of deleted documents in this segment.</li>
 *   <li>HasProx is 1 if any fields in this segment have position data
 *       ({@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS DOCS_AND_FREQS_AND_POSITIONS} or 
 *       {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}); 
 *       else, it's 0.</li>
 *   <li>SegCodec is the {@link Codec#getName() name} of the Codec that encoded
 *       this segment.</li>
 *   <li>CommitUserData stores an optional user-supplied opaque
 *       Map&lt;String,String&gt; that was passed to {@link IndexWriter#commit(java.util.Map)} 
 *       or {@link IndexWriter#prepareCommit(java.util.Map)}.</li>
 *   <li>The Diagnostics Map is privately written by IndexWriter, as a debugging aid,
 *       for each segment it creates. It includes metadata like the current Lucene
 *       version, OS, Java version, why the segment was created (merge, flush,
 *       addIndexes), etc.</li>
 *   <li>HasVectors is 1 if this segment stores term vectors, else it's 0.</li>
 * </ul>
 * </p>
 * 
 * @see SegmentInfos
 * @lucene.experimental
 */
public class Lucene40SegmentInfosFormat extends SegmentInfosFormat {
  private final SegmentInfosReader reader = new Lucene40SegmentInfosReader();
  private final SegmentInfosWriter writer = new Lucene40SegmentInfosWriter();
  
  @Override
  public SegmentInfosReader getSegmentInfosReader() {
    return reader;
  }

  @Override
  public SegmentInfosWriter getSegmentInfosWriter() {
    return writer;
  }

  public final static String SI_EXTENSION = "si";
}
