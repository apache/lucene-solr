package org.apache.lucene.index.codecs.lucene40;

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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.codecs.NormsWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

public class Lucene40NormsWriter extends NormsWriter {
  private IndexOutput out;
  private int normCount = 0;
  
  /** norms header placeholder */
  // nocommit: not public
  public static final byte[] NORMS_HEADER = new byte[]{'N','R','M',-1};
  
  public Lucene40NormsWriter(Directory directory, String segment, IOContext context) throws IOException {
    final String normsFileName = IndexFileNames.segmentFileName(segment, "", IndexFileNames.NORMS_EXTENSION);
    boolean success = false;
    try {
      out = directory.createOutput(normsFileName, context);
      out.writeBytes(NORMS_HEADER, 0, NORMS_HEADER.length);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }

  @Override
  public void startField(FieldInfo info) throws IOException {
    assert info.omitNorms == false;
    normCount++;
  }
  
  @Override
  public void writeNorm(byte norm) throws IOException {
    out.writeByte(norm);
  }
  
  @Override
  public void finish(int numDocs) throws IOException {
    if (4+normCount*(long)numDocs != out.getFilePointer()) {
      throw new RuntimeException(".nrm file size mismatch: expected=" + (4+normCount*(long)numDocs) + " actual=" + out.getFilePointer());
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(out);
    } finally {
      out = null;
    }
  }
}
