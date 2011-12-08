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
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.codecs.NormsReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MapBackedSet;
import org.apache.lucene.util.StringHelper;

public class Lucene40NormsReader extends NormsReader {
  // this would be replaced by Source/SourceCache in a dv impl.
  // for now we have our own mini-version
  final Map<String,Norm> norms = new HashMap<String,Norm>();
  // any .nrm or .sNN files we have open at any time.
  // TODO: just a list, and double-close() separate norms files?
  final Set<IndexInput> openFiles = new MapBackedSet<IndexInput>(new IdentityHashMap<IndexInput,Boolean>());
  // points to a singleNormFile
  IndexInput singleNormStream;
  final int maxdoc;
  
  // note: just like segmentreader in 3.x, we open up all the files here (including separate norms) up front.
  // but we just don't do any seeks or reading yet.
  public Lucene40NormsReader(Directory dir, SegmentInfo info, FieldInfos fields, IOContext context, Directory separateNormsDir) throws IOException {
    maxdoc = info.docCount;
    String segmentName = info.name;
    Map<Integer,Long> normGen = info.getNormGen();
    boolean success = false;
    try {
      long nextNormSeek = Lucene40NormsWriter.NORMS_HEADER.length; //skip header (header unused for now)
      for (FieldInfo fi : fields) {
        if (fi.isIndexed && !fi.omitNorms) {
          String fileName = getNormFilename(segmentName, normGen, fi.number);
          Directory d = hasSeparateNorms(normGen, fi.number) ? separateNormsDir : dir;
        
          // singleNormFile means multiple norms share this file
          boolean singleNormFile = IndexFileNames.matchesExtension(fileName, Lucene40NormsWriter.NORMS_EXTENSION);
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
              normSeek = Lucene40NormsWriter.NORMS_HEADER.length;
            }
          }

          Norm norm = new Norm();
          norm.file = normInput;
          norm.offset = normSeek;
          norms.put(fi.name, norm);
          nextNormSeek += maxdoc; // increment also if some norms are separate
        }
      }
      // TODO: change to a real check? see LUCENE-3619
      assert singleNormStream == null || nextNormSeek == singleNormStream.length();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(openFiles);
      }
    }
  }
  
  @Override
  public byte[] norms(String name) throws IOException {
    Norm norm = norms.get(name);
    return norm == null ? null : norm.bytes();
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
  
  private static String getNormFilename(String segmentName, Map<Integer,Long> normGen, int number) {
    if (hasSeparateNorms(normGen, number)) {
      return IndexFileNames.fileNameFromGeneration(segmentName, Lucene40NormsWriter.SEPARATE_NORMS_EXTENSION + number, normGen.get(number));
    } else {
      // single file for all norms
      return IndexFileNames.fileNameFromGeneration(segmentName, Lucene40NormsWriter.NORMS_EXTENSION, SegmentInfo.WITHOUT_GEN);
    }
  }
  
  private static boolean hasSeparateNorms(Map<Integer,Long> normGen, int number) {
    if (normGen == null) {
      return false;
    }

    Long gen = normGen.get(number);
    return gen != null && gen.longValue() != SegmentInfo.NO;
  }
  
  class Norm {
    IndexInput file;
    long offset;
    byte bytes[];
    
    synchronized byte[] bytes() throws IOException {
      if (bytes == null) {
        bytes = new byte[maxdoc];
        // some norms share fds
        synchronized(file) {
          file.seek(offset);
          file.readBytes(bytes, 0, bytes.length, false);
        }
        // we are done with this file
        if (file != singleNormStream) {
          openFiles.remove(file);
          file.close();
          file = null;
        }
      }
      return bytes;
    }
  }
  
  static void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    // TODO: This is what SI always did... but we can do this cleaner?
    // like first FI that has norms but doesn't have separate norms?
    final String normsFileName = IndexFileNames.segmentFileName(info.name, "", Lucene40NormsWriter.NORMS_EXTENSION);
    if (dir.fileExists(normsFileName)) {
      files.add(normsFileName);
    }
  }
  
  /** @deprecated */
  @Deprecated
  static void separateFiles(Directory dir, SegmentInfo info, Set<String> files) throws IOException {
    Map<Integer,Long> normGen = info.getNormGen();
    if (normGen != null) {
      for (Entry<Integer,Long> entry : normGen.entrySet()) {
        long gen = entry.getValue();
        if (gen >= SegmentInfo.YES) {
          // Definitely a separate norm file, with generation:
          files.add(IndexFileNames.fileNameFromGeneration(info.name, Lucene40NormsWriter.SEPARATE_NORMS_EXTENSION + entry.getKey(), gen));
        }
      }
    }
  }
}
