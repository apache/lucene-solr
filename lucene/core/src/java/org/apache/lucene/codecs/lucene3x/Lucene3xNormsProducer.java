package org.apache.lucene.codecs.lucene3x;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

/**
 * Reads Lucene 3.x norms format and exposes it via DocValues API
 * @lucene.experimental
 * @deprecated Only for reading existing 3.x indexes
 */
@Deprecated
class Lucene3xNormsProducer extends DocValuesProducer {
  
  /** norms header placeholder */
  static final byte[] NORMS_HEADER = new byte[]{'N','R','M',-1};
  
  /** Extension of norms file */
  static final String NORMS_EXTENSION = "nrm";
  
  /** Extension of separate norms file */
  static final String SEPARATE_NORMS_EXTENSION = "s";
  
  final Map<String,NormsDocValues> norms = new HashMap<String,NormsDocValues>();
  // any .nrm or .sNN files we have open at any time.
  // TODO: just a list, and double-close() separate norms files?
  final Set<IndexInput> openFiles = Collections.newSetFromMap(new IdentityHashMap<IndexInput,Boolean>());
  // points to a singleNormFile
  IndexInput singleNormStream;
  final int maxdoc;
  
  // note: just like segmentreader in 3.x, we open up all the files here (including separate norms) up front.
  // but we just don't do any seeks or reading yet.
  public Lucene3xNormsProducer(Directory dir, SegmentInfo info, FieldInfos fields, IOContext context) throws IOException {
    Directory separateNormsDir = info.dir; // separate norms are never inside CFS
    maxdoc = info.getDocCount();
    String segmentName = info.name;
    boolean success = false;
    try {
      long nextNormSeek = NORMS_HEADER.length; //skip header (header unused for now)
      for (FieldInfo fi : fields) {
        if (fi.hasNorms()) {
          String fileName = getNormFilename(info, fi.number);
          Directory d = hasSeparateNorms(info, fi.number) ? separateNormsDir : dir;
        
          // singleNormFile means multiple norms share this file
          boolean singleNormFile = IndexFileNames.matchesExtension(fileName, NORMS_EXTENSION);
          IndexInput normInput = null;
          long normSeek;

          if (singleNormFile) {
            normSeek = nextNormSeek;
            if (singleNormStream == null) {
              singleNormStream = d.openInput(fileName, context);
              openFiles.add(singleNormStream);
            }
            // All norms in the .nrm file can share a single IndexInput since
            // they are only used in a synchronized context.
            // If this were to change in the future, a clone could be done here.
            normInput = singleNormStream;
          } else {
            normInput = d.openInput(fileName, context);
            openFiles.add(normInput);
            // if the segment was created in 3.2 or after, we wrote the header for sure,
            // and don't need to do the sketchy file size check. otherwise, we check 
            // if the size is exactly equal to maxDoc to detect a headerless file.
            // NOTE: remove this check in Lucene 5.0!
            String version = info.getVersion();
            final boolean isUnversioned = 
                (version == null || StringHelper.getVersionComparator().compare(version, "3.2") < 0)
                && normInput.length() == maxdoc;
            if (isUnversioned) {
              normSeek = 0;
            } else {
              normSeek = NORMS_HEADER.length;
            }
          }
          NormsDocValues norm = new NormsDocValues(normInput, normSeek);
          norms.put(fi.name, norm);
          nextNormSeek += maxdoc; // increment also if some norms are separate
        }
      }
      // TODO: change to a real check? see LUCENE-3619
      assert singleNormStream == null || nextNormSeek == singleNormStream.length() : singleNormStream != null ? "len: " + singleNormStream.length() + " expected: " + nextNormSeek : "null";
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(openFiles);
      }
    }
  }
  
  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(openFiles);
    } finally {
      norms.clear();
      openFiles.clear();
    }
  }
  
  private static String getNormFilename(SegmentInfo info, int number) {
    if (hasSeparateNorms(info, number)) {
      long gen = Long.parseLong(info.getAttribute(Lucene3xSegmentInfoFormat.NORMGEN_PREFIX + number));
      return IndexFileNames.fileNameFromGeneration(info.name, SEPARATE_NORMS_EXTENSION + number, gen);
    } else {
      // single file for all norms
      return IndexFileNames.segmentFileName(info.name, "", NORMS_EXTENSION);
    }
  }
  
  private static boolean hasSeparateNorms(SegmentInfo info, int number) {
    String v = info.getAttribute(Lucene3xSegmentInfoFormat.NORMGEN_PREFIX + number);
    if (v == null) {
      return false;
    } else {
      assert Long.parseLong(v) != SegmentInfo.NO;
      return true;
    }
  }

  // holds a file+offset pointing to a norms, and lazy-loads it
  // to a singleton NumericDocValues instance
  private class NormsDocValues {
    private final IndexInput file;
    private final long offset;
    private NumericDocValues instance;
    
    public NormsDocValues(IndexInput normInput, long normSeek) {
      this.file = normInput;
      this.offset = normSeek;
    }
    
    synchronized NumericDocValues getInstance() throws IOException {
      if (instance == null) {
        final byte[] bytes = new byte[maxdoc];
        // some norms share fds
        synchronized(file) {
          file.seek(offset);
          file.readBytes(bytes, 0, bytes.length, false);
        }
        // we are done with this file
        if (file != singleNormStream) {
          openFiles.remove(file);
          file.close();
        }
        instance = new NumericDocValues() {
          @Override
          public long get(int docID) {
            return bytes[docID];
          }
        };
      }
      return instance;
    }    
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NormsDocValues dv = norms.get(field.name);
    assert dv != null;
    return dv.getInstance();
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    throw new AssertionError();
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    throw new AssertionError();
  }
  
  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    throw new AssertionError();
  }
}
