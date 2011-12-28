package org.apache.lucene.codecs.simpletext;

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

import org.apache.lucene.codecs.NormsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Writes plain-text norms
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextNormsWriter extends NormsWriter {
  private IndexOutput out;
  private int docid = 0;
    
  /** Extension of norms file */
  static final String NORMS_EXTENSION = "len";
  
  private final BytesRef scratch = new BytesRef();
  
  final static BytesRef END     = new BytesRef("END");
  final static BytesRef FIELD   = new BytesRef("field ");
  final static BytesRef DOC     = new BytesRef("  doc ");
  final static BytesRef NORM    = new BytesRef("    norm ");
  
  public SimpleTextNormsWriter(Directory directory, String segment, IOContext context) throws IOException {
    final String normsFileName = IndexFileNames.segmentFileName(segment, "", NORMS_EXTENSION);
    out = directory.createOutput(normsFileName, context);
  }

  @Override
  public void startField(FieldInfo info) throws IOException {
    assert info.omitNorms == false;
    docid = 0;
    write(FIELD);
    write(info.name);
    newLine();
  }
    
  @Override
  public void writeNorm(byte norm) throws IOException {
    write(DOC);
    write(Integer.toString(docid));
    newLine();
    
    write(NORM);
    write(norm);
    newLine();
    docid++;
  }
    
  @Override
  public void finish(int numDocs) throws IOException {
    if (docid != numDocs) {
      throw new RuntimeException("mergeNorms produced an invalid result: docCount is " + numDocs
          + " but only saw " + docid + " file=" + out.toString() + "; now aborting this merge to prevent index corruption");
    }
    write(END);
    newLine();
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(out);
    } finally {
      out = null;
    }
  }
  
  private void write(String s) throws IOException {
    SimpleTextUtil.write(out, s, scratch);
  }
  
  private void write(BytesRef bytes) throws IOException {
    SimpleTextUtil.write(out, bytes);
  }
  
  private void write(byte b) throws IOException {
    scratch.grow(1);
    scratch.bytes[scratch.offset] = b;
    scratch.length = 1;
    SimpleTextUtil.write(out, scratch);
  }
  
  private void newLine() throws IOException {
    SimpleTextUtil.writeNewline(out);
  }
}
