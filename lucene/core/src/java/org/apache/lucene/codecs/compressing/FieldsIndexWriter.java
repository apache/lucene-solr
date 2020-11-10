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
package org.apache.lucene.codecs.compressing;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Efficient index format for block-based {@link Codec}s.
 * <p>For each block of compressed stored fields, this stores the first document
 * of the block and the start pointer of the block in a
 * {@link DirectMonotonicWriter}. At read time, the docID is binary-searched in
 * the {@link DirectMonotonicReader} that records doc IDS, and the returned
 * index is used to look up the start pointer in the
 * {@link DirectMonotonicReader} that records start pointers.
 * @lucene.internal
 */
public final class FieldsIndexWriter implements Closeable {

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = 0;

  private final Directory dir;
  private final String name;
  private final String suffix;
  private final String extension;
  private final String codecName;
  private final byte[] id;
  private final int blockShift;
  private final IOContext ioContext;
  private IndexOutput docsOut;
  private IndexOutput filePointersOut;
  private int totalDocs;
  private int totalChunks;
  private long previousFP;

  FieldsIndexWriter(Directory dir, String name, String suffix, String extension,
      String codecName, byte[] id, int blockShift, IOContext ioContext) throws IOException {
    this.dir = dir;
    this.name = name;
    this.suffix = suffix;
    this.extension = extension;
    this.codecName = codecName;
    this.id = id;
    this.blockShift = blockShift;
    this.ioContext = ioContext;
    this.docsOut = dir.createTempOutput(name, codecName + "-doc_ids", ioContext);
    boolean success = false;
    try {
      CodecUtil.writeHeader(docsOut, codecName + "Docs", VERSION_CURRENT);
      filePointersOut = dir.createTempOutput(name, codecName + "file_pointers", ioContext);
      CodecUtil.writeHeader(filePointersOut, codecName + "FilePointers", VERSION_CURRENT);
      success = true;
    } finally {
      if (success == false) {
        close();
      }
    }
  }

  void writeIndex(int numDocs, long startPointer) throws IOException {
    assert startPointer >= previousFP;
    docsOut.writeVInt(numDocs);
    filePointersOut.writeVLong(startPointer - previousFP);
    previousFP = startPointer;
    totalDocs += numDocs;
    totalChunks++;
  }

  void finish(int numDocs, long maxPointer, IndexOutput metaOut) throws IOException {
    if (numDocs != totalDocs) {
      throw new IllegalStateException("Expected " + numDocs + " docs, but got " + totalDocs);
    }
    CodecUtil.writeFooter(docsOut);
    CodecUtil.writeFooter(filePointersOut);
    IOUtils.close(docsOut, filePointersOut);

    try (IndexOutput dataOut = dir.createOutput(IndexFileNames.segmentFileName(name, suffix, extension), ioContext)) {
      CodecUtil.writeIndexHeader(dataOut, codecName + "Idx", VERSION_CURRENT, id, suffix);

      metaOut.writeInt(numDocs);
      metaOut.writeInt(blockShift);
      metaOut.writeInt(totalChunks + 1);
      metaOut.writeLong(dataOut.getFilePointer());

      try (ChecksumIndexInput docsIn = dir.openChecksumInput(docsOut.getName(), IOContext.READONCE)) {
        CodecUtil.checkHeader(docsIn, codecName + "Docs", VERSION_CURRENT, VERSION_CURRENT);
        Throwable priorE = null;
        try {
          final DirectMonotonicWriter docs = DirectMonotonicWriter.getInstance(metaOut, dataOut, totalChunks + 1, blockShift);
          long doc = 0;
          docs.add(doc);
          for (int i = 0; i < totalChunks; ++i) {
            doc += docsIn.readVInt();
            docs.add(doc);
          }
          docs.finish();
          if (doc != totalDocs) {
            throw new CorruptIndexException("Docs don't add up", docsIn);
          }
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(docsIn, priorE);
        }
      }
      dir.deleteFile(docsOut.getName());
      docsOut = null;

      metaOut.writeLong(dataOut.getFilePointer());
      try (ChecksumIndexInput filePointersIn = dir.openChecksumInput(filePointersOut.getName(), IOContext.READONCE)) {
        CodecUtil.checkHeader(filePointersIn, codecName + "FilePointers", VERSION_CURRENT, VERSION_CURRENT);
        Throwable priorE = null;
        try {
          final DirectMonotonicWriter filePointers = DirectMonotonicWriter.getInstance(metaOut, dataOut, totalChunks + 1, blockShift);
          long fp = 0;
          for (int i = 0; i < totalChunks; ++i) {
            fp += filePointersIn.readVLong();
            filePointers.add(fp);
          }
          if (maxPointer < fp) {
            throw new CorruptIndexException("File pointers don't add up", filePointersIn);
          }
          filePointers.add(maxPointer);
          filePointers.finish();
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(filePointersIn, priorE);
        }
      }
      dir.deleteFile(filePointersOut.getName());
      filePointersOut = null;

      metaOut.writeLong(dataOut.getFilePointer());
      metaOut.writeLong(maxPointer);

      CodecUtil.writeFooter(dataOut);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(docsOut, filePointersOut);
    } finally {
      List<String> fileNames = new ArrayList<>();
      if (docsOut != null) {
        fileNames.add(docsOut.getName());
      }
      if (filePointersOut != null) {
        fileNames.add(filePointersOut.getName());
      }
      try {
        IOUtils.deleteFiles(dir, fileNames);
      } finally {
        docsOut = filePointersOut = null;
      }
    }
  }
}
