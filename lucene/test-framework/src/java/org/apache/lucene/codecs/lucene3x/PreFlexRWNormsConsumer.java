package org.apache.lucene.codecs.lucene3x;

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
import java.util.Arrays;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * Writes and Merges Lucene 3.x norms format
 * @lucene.experimental
 */
class PreFlexRWNormsConsumer extends PerDocConsumer {
  
  /** norms header placeholder */
  private static final byte[] NORMS_HEADER = new byte[]{'N','R','M',-1};
  
  /** Extension of norms file */
  private static final String NORMS_EXTENSION = "nrm";
  
  /** Extension of separate norms file
   * @deprecated */
  @Deprecated
  private static final String SEPARATE_NORMS_EXTENSION = "s";

  private final Directory directory;

  private final String segment;

  private final IOContext context;

  private NormsWriter writer;
  
  public PreFlexRWNormsConsumer(Directory directory, String segment, IOContext context){
    this.directory = directory;
    this.segment = segment;
    this.context = context;
  }

  @Override
  public void merge(MergeState mergeState) throws IOException {
    getNormsWriter().merge(mergeState);
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.finish();
    }
  }
  
  @Override
  protected boolean canMerge(FieldInfo info) {
    return info.hasNorms();
  }

  @Override
  protected Type getDocValuesType(FieldInfo info) {
    return info.getNormType();
  }

  @Override
  public DocValuesConsumer addValuesField(Type type, FieldInfo fieldInfo)
      throws IOException {
    if (type != Type.FIXED_INTS_8) {
      throw new UnsupportedOperationException("Codec only supports single byte norm values. Type give: " + type);
    }
    return new Lucene3xNormsDocValuesConsumer(fieldInfo);
  }
  
  class Lucene3xNormsDocValuesConsumer extends DocValuesConsumer {
    // Holds all docID/norm pairs we've seen
    private int[] docIDs = new int[1];
    private byte[] norms = new byte[1];
    private int upto;
    private final FieldInfo fi;
    
    Lucene3xNormsDocValuesConsumer(FieldInfo fieldInfo) {
      fi = fieldInfo;
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
    
    @Override
    public void add(int docID, IndexableField docValue) throws IOException {
      add(docID, docValue.numericValue().longValue());
    }
    
    protected void add(int docID, long value) {
      if (docIDs.length <= upto) {
        assert docIDs.length == upto;
        docIDs = ArrayUtil.grow(docIDs, 1 + upto);
      }
      if (norms.length <= upto) {
        assert norms.length == upto;
        norms = ArrayUtil.grow(norms, 1 + upto);
      }
      norms[upto] = (byte) value;
      
      docIDs[upto] = docID;
      upto++;
    }

    @Override
    protected Type getType() {
      return Type.FIXED_INTS_8;
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
    private int normCount = 0;
    private int numTotalDocs = 0;
    
    public NormsWriter(Directory directory, String segment, IOContext context) throws IOException {
      final String normsFileName = IndexFileNames.segmentFileName(segment, "", NORMS_EXTENSION);
      boolean success = false;
      IndexOutput out = null;
      try {
        out = directory.createOutput(normsFileName, context);
        output = out;
        output.writeBytes(NORMS_HEADER, 0, NORMS_HEADER.length);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(out);
        }
      }
      
    }
    
    
    public void setNumTotalDocs(int numTotalDocs) {
      assert this.numTotalDocs == 0 || numTotalDocs == this.numTotalDocs;
      this.numTotalDocs = numTotalDocs;
    }
    
    public void startField(FieldInfo info) throws IOException {
      assert info.omitNorms == false;
      normCount++;
    }
    
    public void writeNorm(byte norm) throws IOException {
      output.writeByte(norm);
    }
    
    public void abort() throws IOException {
      IOUtils.close(output);
    }
    
    public void finish() throws IOException {
      IOUtils.close(output);
      
      if (4+normCount*(long)numTotalDocs != output.getFilePointer()) {
        throw new IOException(".nrm file size mismatch: expected=" + (4+normCount*(long)numTotalDocs) + " actual=" + output.getFilePointer());
      }
    }
    // TODO: we can actually use the defaul DV merge here and drop this specific stuff entirely
    /** we override merge and bulk-merge norms when there are no deletions */
    public void merge(MergeState mergeState) throws IOException {
      int numMergedDocs = 0;
      for (FieldInfo fi : mergeState.fieldInfos) {
        if (fi.hasNorms()) {
          startField(fi);
          int numMergedDocsForField = 0;
          for (MergeState.IndexReaderAndLiveDocs reader : mergeState.readers) {
            final int maxDoc = reader.reader.maxDoc();
            byte[] normBuffer;
            DocValues normValues = reader.reader.normValues(fi.name);
            if (normValues == null) {
              // Can be null if this segment doesn't have
              // any docs with this field
              normBuffer = new byte[maxDoc];
              Arrays.fill(normBuffer, (byte)0);
            } else {
              Source directSource = normValues.getDirectSource();
              assert directSource.hasArray();
              normBuffer = (byte[]) directSource.getArray();
            }
            if (reader.liveDocs == null) {
              //optimized case for segments without deleted docs
              output.writeBytes(normBuffer, maxDoc);
              numMergedDocsForField += maxDoc;
            } else {
              // this segment has deleted docs, so we have to
              // check for every doc if it is deleted or not
              final Bits liveDocs = reader.liveDocs;
              for (int k = 0; k < maxDoc; k++) {
                if (liveDocs.get(k)) {
                  numMergedDocsForField++;
                  output.writeByte(normBuffer[k]);
                }
              }
            }
            mergeState.checkAbort.work(maxDoc);
          }
          assert numMergedDocs == 0 || numMergedDocs == numMergedDocsForField;
          numMergedDocs = numMergedDocsForField;
        }
      }
      this.numTotalDocs = numMergedDocs;
    }
  }

  @Override
  public void abort() {
    try {
      try {
        if (writer != null) {
          writer.abort();
        }
      } finally {
        directory.deleteFile(IndexFileNames.segmentFileName(segment, "",
            NORMS_EXTENSION));
      }
    } catch (IOException e) {
      // ignore
    }
  }
}
