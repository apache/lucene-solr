package org.apache.lucene.index;

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

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFileNameFilter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * A utility class (used by both IndexReader and
 * IndexWriter) to keep track of files that need to be
 * deleted because they are no longer referenced by the
 * index.
 */
final class IndexFileDeleter {
  private Vector deletable;
  private HashSet pending;
  private Directory directory;
  private SegmentInfos segmentInfos;
  private PrintStream infoStream;

  IndexFileDeleter(SegmentInfos segmentInfos, Directory directory)
    throws IOException {
    this.segmentInfos = segmentInfos;
    this.directory = directory;
  }
  void setSegmentInfos(SegmentInfos segmentInfos) {
    this.segmentInfos = segmentInfos;
  }
  SegmentInfos getSegmentInfos() {
    return segmentInfos;
  }

  void setInfoStream(PrintStream infoStream) {
    this.infoStream = infoStream;
  }

  /** Determine index files that are no longer referenced
   * and therefore should be deleted.  This is called once
   * (by the writer), and then subsequently we add onto
   * deletable any files that are no longer needed at the
   * point that we create the unused file (eg when merging
   * segments), and we only remove from deletable when a
   * file is successfully deleted.
   */

  void findDeletableFiles() throws IOException {

    // Gather all "current" segments:
    HashMap current = new HashMap();
    for(int j=0;j<segmentInfos.size();j++) {
      SegmentInfo segmentInfo = (SegmentInfo) segmentInfos.elementAt(j);
      current.put(segmentInfo.name, segmentInfo);
    }

    // Then go through all files in the Directory that are
    // Lucene index files, and add to deletable if they are
    // not referenced by the current segments info:

    String segmentsInfosFileName = segmentInfos.getCurrentSegmentFileName();
    IndexFileNameFilter filter = IndexFileNameFilter.getFilter();

    String[] files = directory.list();

    for (int i = 0; i < files.length; i++) {

      if (filter.accept(null, files[i]) && !files[i].equals(segmentsInfosFileName) && !files[i].equals(IndexFileNames.SEGMENTS_GEN)) {

        String segmentName;
        String extension;

        // First remove any extension:
        int loc = files[i].indexOf('.');
        if (loc != -1) {
          extension = files[i].substring(1+loc);
          segmentName = files[i].substring(0, loc);
        } else {
          extension = null;
          segmentName = files[i];
        }

        // Then, remove any generation count:
        loc = segmentName.indexOf('_', 1);
        if (loc != -1) {
          segmentName = segmentName.substring(0, loc);
        }

        // Delete this file if it's not a "current" segment,
        // or, it is a single index file but there is now a
        // corresponding compound file:
        boolean doDelete = false;

        if (!current.containsKey(segmentName)) {
          // Delete if segment is not referenced:
          doDelete = true;
        } else {
          // OK, segment is referenced, but file may still
          // be orphan'd:
          SegmentInfo info = (SegmentInfo) current.get(segmentName);

          if (filter.isCFSFile(files[i]) && info.getUseCompoundFile()) {
            // This file is in fact stored in a CFS file for
            // this segment:
            doDelete = true;
          } else {
            
            if ("del".equals(extension)) {
              // This is a _segmentName_N.del file:
              if (!files[i].equals(info.getDelFileName())) {
                // If this is a seperate .del file, but it
                // doesn't match the current del filename for
                // this segment, then delete it:
                doDelete = true;
              }
            } else if (extension != null && extension.startsWith("s") && extension.matches("s\\d+")) {
              int field = Integer.parseInt(extension.substring(1));
              // This is a _segmentName_N.sX file:
              if (!files[i].equals(info.getNormFileName(field))) {
                // This is an orphan'd separate norms file:
                doDelete = true;
              }
            } else if ("cfs".equals(extension) && !info.getUseCompoundFile()) {
              // This is a partially written
              // _segmentName.cfs:
              doDelete = true;
            }
          }
        }

        if (doDelete) {
          addDeletableFile(files[i]);
          if (infoStream != null) {
            infoStream.println("IndexFileDeleter: file \"" + files[i] + "\" is unreferenced in index and will be deleted on next commit");
          }
        }
      }
    }
  }

  /*
   * Some operating systems (e.g. Windows) don't permit a file to be deleted
   * while it is opened for read (e.g. by another process or thread). So we
   * assume that when a delete fails it is because the file is open in another
   * process, and queue the file for subsequent deletion.
   */

  void deleteSegments(Vector segments) throws IOException {

    deleteFiles();                                // try to delete files that we couldn't before

    for (int i = 0; i < segments.size(); i++) {
      SegmentReader reader = (SegmentReader)segments.elementAt(i);
      if (reader.directory() == this.directory)
        deleteFiles(reader.files()); // try to delete our files
      else
        deleteFiles(reader.files(), reader.directory()); // delete other files
    }
  }

  /**
   * Delete these segments, as long as they are not listed
   * in protectedSegments.  If they are, then, instead, add
   * them to the pending set.
  */
     
  void deleteSegments(Vector segments, HashSet protectedSegments) throws IOException {

    deleteFiles();                                // try to delete files that we couldn't before

    for (int i = 0; i < segments.size(); i++) {
      SegmentReader reader = (SegmentReader)segments.elementAt(i);
      if (reader.directory() == this.directory) {
        if (protectedSegments.contains(reader.getSegmentName())) {
          addPendingFiles(reader.files()); // record these for deletion on commit
        } else {
          deleteFiles(reader.files()); // try to delete our files
        }
      }  else {
        deleteFiles(reader.files(), reader.directory()); // delete other files
      }
    }
  }
  
  void deleteFiles(Vector files, Directory directory)
       throws IOException {
    for (int i = 0; i < files.size(); i++)
      directory.deleteFile((String)files.elementAt(i));
  }

  void deleteFiles(Vector files)
       throws IOException {
    deleteFiles();                                // try to delete files that we couldn't before
    for (int i = 0; i < files.size(); i++) {
      deleteFile((String) files.elementAt(i));
    }
  }

  void deleteFile(String file)
       throws IOException {
    try {
      directory.deleteFile(file);		  // try to delete each file
    } catch (IOException e) {			  // if delete fails
      if (directory.fileExists(file)) {
        if (infoStream != null)
          infoStream.println("IndexFileDeleter: unable to remove file \"" + file + "\": " + e.toString() + "; Will re-try later.");
        addDeletableFile(file);                  // add to deletable
      }
    }
  }

  void clearPendingFiles() {
    pending = null;
  }

  /*
    Record that the files for these segments should be
    deleted, once the pending deletes are committed.
   */
  void addPendingSegments(Vector segments) throws IOException {
    for (int i = 0; i < segments.size(); i++) {
      SegmentReader reader = (SegmentReader)segments.elementAt(i);
      if (reader.directory() == this.directory) {
        addPendingFiles(reader.files());
      }
    }
  }

  /*
    Record list of files for deletion, but do not delete
    them until commitPendingFiles is called.
  */
  void addPendingFiles(Vector files) {
    for(int i=0;i<files.size();i++) {
      addPendingFile((String) files.elementAt(i));
    }
  }

  /*
    Record a file for deletion, but do not delete it until
    commitPendingFiles is called.
  */
  void addPendingFile(String fileName) {
    if (pending == null) {
      pending = new HashSet();
    }
    pending.add(fileName);
  }

  void commitPendingFiles() throws IOException {
    if (pending != null) {
      if (deletable == null) {
        deletable = new Vector();
      }
      Iterator it = pending.iterator();
      while(it.hasNext()) {
        deletable.addElement(it.next());
      }
      pending = null;
      deleteFiles();
    }
  }

  void addDeletableFile(String fileName) {
    if (deletable == null) {
      deletable = new Vector();
    }
    deletable.addElement(fileName);
  }

  void deleteFiles()
    throws IOException {
    if (deletable != null) {
      Vector oldDeletable = deletable;
      deletable = null;
      deleteFiles(oldDeletable); // try to delete deletable
    }
  }
}
