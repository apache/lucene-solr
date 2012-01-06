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
import java.util.Set;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.index.DocValue;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Writes plain-text norms
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * 
 * @lucene.experimental
 */
public class SimpleTextNormsConsumer extends PerDocConsumer {
  
  /** Extension of norms file */
  static final String NORMS_EXTENSION = "len";
  final static BytesRef END = new BytesRef("END");
  final static BytesRef FIELD = new BytesRef("field ");
  final static BytesRef DOC = new BytesRef("  doc ");
  final static BytesRef NORM = new BytesRef("    norm ");
  
  private NormsWriter writer;

  private final Directory directory;

  private final String segment;

  private final IOContext context;

  public SimpleTextNormsConsumer(Directory directory, String segment,
      IOContext context) throws IOException {
    this.directory = directory;
    this.segment = segment;
    this.context = context;
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.finish();
    }
  }
  
  @Override
  protected DocValues getDocValuesForMerge(IndexReader reader, FieldInfo info)
      throws IOException {
    return reader.normValues(info.name);
  }

  @Override
  protected boolean canMerge(FieldInfo info) {
    return !info.omitNorms && info.isIndexed;
  }

  @Override
  protected Type getDocValuesType(FieldInfo info) {
    return Type.BYTES_FIXED_STRAIGHT;
  }

  @Override
  public DocValuesConsumer addValuesField(Type type, FieldInfo fieldInfo)
      throws IOException {
    return new SimpleTextNormsDocValuesConsumer(fieldInfo);
  }

  @Override
  public void abort() {
    if (writer != null) {
      try {
        writer.abort();
      } catch (IOException e) {
      }
    }
  }

  private class SimpleTextNormsDocValuesConsumer extends DocValuesConsumer {
    // Holds all docID/norm pairs we've seen
    int[] docIDs = new int[1];
    byte[] norms = new byte[1];
    int upto;
    private final FieldInfo fi;

    public SimpleTextNormsDocValuesConsumer(FieldInfo fieldInfo) {
      fi = fieldInfo;
    }

    @Override
    public void add(int docID, DocValue docValue) throws IOException {
      add(docID, docValue.getBytes());
    }
    
    protected void add(int docID, BytesRef value) throws IOException {
      if (docIDs.length <= upto) {
        assert docIDs.length == upto;
        docIDs = ArrayUtil.grow(docIDs, 1 + upto);
      }
      if (norms.length <= upto) {
        assert norms.length == upto;
        norms = ArrayUtil.grow(norms, 1 + upto);
      }
      assert value.length == 1;
      norms[upto] = value.bytes[value.offset];
      docIDs[upto] = docID;
      upto++;
    }

    @Override
    public void finish(int docCount) throws IOException {
      final NormsWriter normsWriter = getNormsWriter();
      boolean success = false;
      try {
        int uptoDoc = 0;
        normsWriter.setNumTotalDocs(docCount);
        if (upto > 0) {
          normsWriter.startField(fi);
          int docID = 0;
          for (; docID < docCount; docID++) {
            if (uptoDoc < upto && docIDs[uptoDoc] == docID) {
              normsWriter.writeNorm(norms[uptoDoc]);
              uptoDoc++;
            } else {
              normsWriter.writeNorm((byte) 0);
            }
          }
          // we should have consumed every norm
          assert uptoDoc == upto;

        } else {
          // Fill entire field with default norm:
          normsWriter.startField(fi);
          for (; upto < docCount; upto++)
            normsWriter.writeNorm((byte) 0);
        }
        success = true;
      } finally {
        if (!success) {
          normsWriter.abort();
        }
      }
    }
  }

  public NormsWriter getNormsWriter() throws IOException {
    if (writer == null) {
      writer = new NormsWriter(directory, segment, context);
    }
    return writer;
  }

  private static class NormsWriter {

    private final IndexOutput output;
    private int numTotalDocs = 0;
    private int docid = 0;

    private final BytesRef scratch = new BytesRef();


    public NormsWriter(Directory directory, String segment, IOContext context)
        throws IOException {
      final String normsFileName = IndexFileNames.segmentFileName(segment, "",
          NORMS_EXTENSION);
      output = directory.createOutput(normsFileName, context);

    }

    public void startField(FieldInfo info) throws IOException {
      assert info.omitNorms == false;
      docid = 0;
      write(FIELD);
      write(info.name);
      newLine();
    }

    public void writeNorm(byte norm) throws IOException {
      write(DOC);
      write(Integer.toString(docid));
      newLine();

      write(NORM);
      write(norm);
      newLine();
      docid++;
    }

    public void finish(int numDocs) throws IOException {
      if (docid != numDocs) {
        throw new RuntimeException(
            "mergeNorms produced an invalid result: docCount is " + numDocs
                + " but only saw " + docid + " file=" + output.toString()
                + "; now aborting this merge to prevent index corruption");
      }
      write(END);
      newLine();
    }

    private void write(String s) throws IOException {
      SimpleTextUtil.write(output, s, scratch);
    }

    private void write(BytesRef bytes) throws IOException {
      SimpleTextUtil.write(output, bytes);
    }

    private void write(byte b) throws IOException {
      scratch.grow(1);
      scratch.bytes[scratch.offset] = b;
      scratch.length = 1;
      SimpleTextUtil.write(output, scratch);
    }

    private void newLine() throws IOException {
      SimpleTextUtil.writeNewline(output);
    }

    public void setNumTotalDocs(int numTotalDocs) {
      assert this.numTotalDocs == 0 || numTotalDocs == this.numTotalDocs;
      this.numTotalDocs = numTotalDocs;
    }

    public void abort() throws IOException {
      IOUtils.close(output);
    }

    public void finish() throws IOException {
      finish(numTotalDocs);
      IOUtils.close(output);
    }
  }

  public static void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    FieldInfos fieldInfos = info.getFieldInfos();
    
    for (FieldInfo fieldInfo : fieldInfos) {
      if (!fieldInfo.omitNorms && fieldInfo.isIndexed) {
        files.add(IndexFileNames.segmentFileName(info.name, "",
            NORMS_EXTENSION));  
        break;
      }
    }
  }
}
