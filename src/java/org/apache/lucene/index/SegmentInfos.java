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
import org.apache.lucene.store.InputStream;
import org.apache.lucene.store.OutputStream;

final class SegmentInfos extends Vector {
  public int counter = 0;    // used to name new segments
  private long version = 0; //counts how often the index has been changed by adding or deleting docs

  public final SegmentInfo info(int i) {
    return (SegmentInfo) elementAt(i);
  }

  public final void read(Directory directory) throws IOException {
    InputStream input = directory.openFile("segments");
    try {
      counter = input.readInt(); // read counter
      for (int i = input.readInt(); i > 0; i--) { // read segmentInfos
        SegmentInfo si =
          new SegmentInfo(input.readString(), input.readInt(), directory);
        addElement(si);
      }
      if (input.getFilePointer() >= input.length())
        version = 0; // old file format without version number
      else
        version = input.readLong(); // read version
    }
    finally {
      input.close();
    }
  }

  public final void write(Directory directory) throws IOException {
    OutputStream output = directory.createFile("segments.new");
    try {
      output.writeInt(counter); // write counter
      output.writeInt(size()); // write infos
      for (int i = 0; i < size(); i++) {
        SegmentInfo si = info(i);
        output.writeString(si.name);
        output.writeInt(si.docCount);
      }
      output.writeLong(++version); // every write changes the index         
    }
    finally {
      output.close();
    }

    // install new segment info
    directory.renameFile("segments.new", "segments");
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

    // We cannot be sure whether the segments file is in the old format or the new one.
    // Therefore we have to read the whole file and cannot simple seek to the version entry.

    SegmentInfos sis = new SegmentInfos();
    sis.read(directory);
    return sis.getVersion();
  }
}
