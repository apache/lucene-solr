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
package org.apache.lucene.replicator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Token for a replication session, for guaranteeing that source replicated
 * files will be kept safe until the replication completes.
 * 
 * @see Replicator#checkForUpdate(String)
 * @see Replicator#release(String)
 * @see LocalReplicator#DEFAULT_SESSION_EXPIRATION_THRESHOLD
 * 
 * @lucene.experimental
 */
public final class SessionToken {
  
  /**
   * ID of this session.
   * Should be passed when releasing the session, thereby acknowledging the 
   * {@link Replicator Replicator} that this session is no longer in use.
   * @see Replicator#release(String)
   */
  public final String id;
  
  /**
   * @see Revision#getVersion()
   */
  public final String version;
  
  /**
   * @see Revision#getSourceFiles()
   */
  public final Map<String,List<RevisionFile>> sourceFiles;
  
  /** Constructor which deserializes from the given {@link DataInput}. */
  public SessionToken(DataInput in) throws IOException {
    this.id = in.readUTF();
    this.version = in.readUTF();
    this.sourceFiles = new HashMap<>();
    int numSources = in.readInt();
    while (numSources > 0) {
      String source = in.readUTF();
      int numFiles = in.readInt();
      List<RevisionFile> files = new ArrayList<>(numFiles);
      for (int i = 0; i < numFiles; i++) {
        String fileName = in.readUTF();
        RevisionFile file = new RevisionFile(fileName);
        file.size = in.readLong();
        files.add(file);
      }
      this.sourceFiles.put(source, files);
      --numSources;
    }
  }
  
  /** Constructor with the given id and revision. */
  public SessionToken(String id, Revision revision) {
    this.id = id;
    this.version = revision.getVersion();
    this.sourceFiles = revision.getSourceFiles();
  }
  
  /** Serialize the token data for communication between server and client. */
  public void serialize(DataOutput out) throws IOException {
    out.writeUTF(id);
    out.writeUTF(version);
    out.writeInt(sourceFiles.size());
    for (Entry<String,List<RevisionFile>> e : sourceFiles.entrySet()) {
      out.writeUTF(e.getKey());
      List<RevisionFile> files = e.getValue();
      out.writeInt(files.size());
      for (RevisionFile file : files) {
        out.writeUTF(file.fileName);
        out.writeLong(file.size);
      }
    }
  }
  
  @Override
  public String toString() {
    return "id=" + id + " version=" + version + " files=" + sourceFiles;
  }
  
}