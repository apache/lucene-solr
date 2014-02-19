package org.apache.lucene.replicator;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMOutputStream;

/**
 * A {@link Revision} of a single index files which comprises the list of all
 * flushed files. Unlike {@link IndexRevision}, the list of files need not
 * belong to an {@link IndexCommit}, which allows replicating near-real-time
 * changes to the index. However, the changes to the {@link IndexWriter} are
 * {@link IndexWriter#flushAndIncRef() flushed} so all files are persisted to
 * the directory.
 * <p>
 * When this revision is {@link #release() released}, it releases the obtained
 * {@link SegmentInfos} via {@link IndexWriter#decRefDeleter(SegmentInfos)}.
 * 
 * @lucene.experimental
 */
public class NRTIndexRevision implements Revision {
  
	private static final class SegmentsRevisionFile extends RevisionFile {

		public final byte[] infosBytes;
		
    public SegmentsRevisionFile(String fileName, byte[] infosBytes) {
      super(fileName);
      this.infosBytes = infosBytes;
    }

	}
	
  private static final int RADIX = 16;
  private static final String SOURCE = "index";
  
  private final IndexWriter writer;
  private final Directory dir;
  private final SegmentInfos infos;
  private final byte[] infosBytes;
  private final String version;
  private final Map<String,List<RevisionFile>> sourceFiles;
  private final String segmentsFileName;
  
  // returns a RevisionFile with some metadata
  private static RevisionFile newRevisionFile(String file, Directory dir) throws IOException {
    RevisionFile revFile = new RevisionFile(file);
    revFile.size = dir.fileLength(file);
    return revFile;
  }
  
  /** Returns a singleton map of the revision files from the given {@link SegmentInfos}. */
  public static Map<String,List<RevisionFile>> revisionFiles(SegmentInfos infos, Directory dir) throws IOException {
  	// Convert infos to byte[], to send "on the wire":
    // nocommit there's no reason for the double byte[] copy/write - we could
    // write a simple DataOutput which exposes the underlying byte[].
    RAMOutputStream out = new RAMOutputStream();
    infos.write(out);
    byte[] infosBytes = new byte[(int) out.getFilePointer()];
    out.writeTo(infosBytes, 0);

    String segmentsFile = IndexFileNames.SEGMENTS + "_nrt_" + infos.getVersion();
    
    Collection<String> files = infos.files(dir, true);
    List<RevisionFile> revisionFiles = new ArrayList<RevisionFile>(files.size());
    // nocommit if there is a commit point, we copy that segments_N file as well.
    // We can avoid it by filtering out the segments_N file, but if we don't, we
    // may want to sync() the file on the replica side when it's copied.
    // would be good if app can replicate an NRTIndexRevision and IndexRevision
    // interchangebly... 
    for (String file : files) {
      revisionFiles.add(newRevisionFile(file, dir));
    }
    RevisionFile segmentsRevFile = new SegmentsRevisionFile(segmentsFile, infosBytes);
    segmentsRevFile.size = infosBytes.length;
    revisionFiles.add(segmentsRevFile); // segments_N must be last
    return Collections.singletonMap(SOURCE, revisionFiles);
  }
  
  /**
   * Returns a String representation of a revision's version from the given
   * {@link SegmentInfos}. The format of the version is
   * <code>nrt_&lt;version&gt;</code>.
   */
  public static String revisionVersion(SegmentInfos infos) {
    return "nrt_" + Long.toString(infos.getVersion(), RADIX);
  }
  
  /**
   * Constructor over the given {@link IndexWriter}. Uses the last
   * {@link IndexCommit} found in the {@link Directory} managed by the given
   * writer.
   */
  public NRTIndexRevision(IndexWriter writer) throws IOException {
    this.writer = writer;
    this.dir = writer.getDirectory();
    this.infos = writer.flushAndIncRef();
    this.version = revisionVersion(infos);
    this.sourceFiles = revisionFiles(infos, dir);
    List<RevisionFile> files = sourceFiles.get(SOURCE);
    SegmentsRevisionFile segmentsRevisionFile = (SegmentsRevisionFile) files.get(files.size() - 1);
    this.infosBytes = segmentsRevisionFile.infosBytes;
    this.segmentsFileName = segmentsRevisionFile.fileName;
  }
  
  @Override
  public int compareTo(String version) {
    long gen = Long.parseLong(version, RADIX);
    long commitGen = infos.getGeneration();
    return commitGen < gen ? -1 : (commitGen > gen ? 1 : 0);
  }
  
  @Override
  public int compareTo(Revision o) {
    NRTIndexRevision other = (NRTIndexRevision) o;
    long gen = infos.getGeneration();
    long comgen = other.infos.getGeneration();
    return Long.compare(gen, comgen);
  }
  
  @Override
  public String getVersion() {
    return version;
  }
  
  @Override
  public Map<String,List<RevisionFile>> getSourceFiles() {
    return sourceFiles;
  }
  
  @Override
  public InputStream open(String source, String fileName) throws IOException {
    assert source.equals(SOURCE) : "invalid source; expected=" + SOURCE + " got=" + source;
    if (fileName.equals(segmentsFileName)) {
    	return new ByteArrayInputStream(infosBytes);
    } else {
    	return new IndexInputInputStream(dir.openInput(fileName, IOContext.READONCE));
    }
  }
  
  @Override
  public void release() throws IOException {
  	writer.decRefDeleter(infos);
    writer.deleteUnusedFiles();
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + " version=" + version + " files=" + sourceFiles;
  }
  
}
