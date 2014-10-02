package org.apache.lucene.codecs.lucene50;

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

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState.CheckAbort;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Lucene 5.0 compound file format
 * <p>
 * Files:
 * <ul>
 *    <li><tt>.cfs</tt>: An optional "virtual" file consisting of all the other 
 *    index files for systems that frequently run out of file handles.
 *    <li><tt>.cfe</tt>: The "virtual" compound file's entry table holding all 
 *    entries in the corresponding .cfs file.
 * </ul>
 * <p>Description:</p>
 * <ul>
 *   <li>Compound (.cfs) --&gt; Header, FileData <sup>FileCount</sup>, Footer</li>
 *   <li>Compound Entry Table (.cfe) --&gt; Header, FileCount, &lt;FileName,
 *       DataOffset, DataLength&gt; <sup>FileCount</sup></li>
 *   <li>Header --&gt; {@link CodecUtil#writeSegmentHeader SegmentHeader}</li>
 *   <li>FileCount --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>DataOffset,DataLength,Checksum --&gt; {@link DataOutput#writeLong UInt64}</li>
 *   <li>FileName --&gt; {@link DataOutput#writeString String}</li>
 *   <li>FileData --&gt; raw file data</li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *   <li>FileCount indicates how many files are contained in this compound file. 
 *       The entry table that follows has that many entries. 
 *   <li>Each directory entry contains a long pointer to the start of this file's data
 *       section, the files length, and a String with that file's name.
 * </ul>
 */
public final class Lucene50CompoundFormat extends CompoundFormat {

  @Override
  public Directory getCompoundReader(Directory dir, SegmentInfo si, IOContext context) throws IOException {
    String fileName = IndexFileNames.segmentFileName(si.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION);
    return new CompoundFileDirectory(si.getId(), dir, fileName, context, false);
  }

  @Override
  public void write(Directory dir, SegmentInfo si, Collection<String> files, CheckAbort checkAbort, IOContext context) throws IOException {
    String fileName = IndexFileNames.segmentFileName(si.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION);
    try (CompoundFileDirectory cfs = new CompoundFileDirectory(si.getId(), dir, fileName, context, true)) {
      for (String file : files) {
        dir.copy(cfs, file, file, context);
        checkAbort.work(dir.fileLength(file));
      }
    }
  }
}
