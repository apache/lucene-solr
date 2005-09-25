package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Vector;
import java.io.IOException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Constants;

final class SegmentInfos extends Vector {
  
  /** The file format version, a negative number. */
  /* Works since counter, the old 1st entry, is always >= 0 */
  public static final int FORMAT = -1;
  
  public int counter = 0;    // used to name new segments
  /**
   * counts how often the index has been changed by adding or deleting docs.
   * starting with the current time in milliseconds forces to create unique version numbers.
   */
  private long version = System.currentTimeMillis();

  public final SegmentInfo info(int i) {
    return (SegmentInfo) elementAt(i);
  }

  public final void read(Directory directory) throws IOException {
    
    IndexInput input = directory.openInput(IndexFileNames.SEGMENTS);
    try {
      int format = input.readInt();
      if(format < 0){     // file contains explicit format info
        // check that it is a format we can understand
        if (format < FORMAT)
          throw new IOException("Unknown format version: " + format);
        version = input.readLong(); // read version
        counter = input.readInt(); // read counter
      }
      else{     // file is in old format without explicit format info
        counter = format;
      }
      
      for (int i = input.readInt(); i > 0; i--) { // read segmentInfos
        SegmentInfo si =
          new SegmentInfo(input.readString(), input.readInt(), directory);
        addElement(si);
      }
      
      if(format >= 0){    // in old format the version number may be at the end of the file
        if (input.getFilePointer() >= input.length())
          version = System.currentTimeMillis(); // old file format without version number
        else
          version = input.readLong(); // read version
      }
    }
    finally {
      input.close();
    }
  }

  public final void write(Directory directory) throws IOException {
    IndexOutput output = directory.createOutput("segments.new");
    try {
      output.writeInt(FORMAT); // write FORMAT
      output.writeLong(++version); // every write changes the index
      output.writeInt(counter); // write counter
      output.writeInt(size()); // write infos
      for (int i = 0; i < size(); i++) {
        SegmentInfo si = info(i);
        output.writeString(si.name);
        output.writeInt(si.docCount);
      }         
    }
    finally {
      output.close();
    }

    // install new segment info
    directory.renameFile("segments.new", IndexFileNames.SEGMENTS);
  }

  /**
   * version number when this SegmentInfos was generated.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Current version number from segments file.
   */
  public static long readCurrentVersion(Directory directory)
    throws IOException {
      
    IndexInput input = directory.openInput(IndexFileNames.SEGMENTS);
    int format = 0;
    long version = 0;
    try {
      format = input.readInt();
      if(format < 0){
        if (format < FORMAT)
          throw new IOException("Unknown format version: " + format);
        version = input.readLong(); // read version
      }
    }
    finally {
      input.close();
    }
     
    if(format < 0)
      return version;

    // We cannot be sure about the format of the file.
    // Therefore we have to read the whole file and cannot simply seek to the version entry.

    SegmentInfos sis = new SegmentInfos();
    sis.read(directory);
    return sis.getVersion();
  }
}
