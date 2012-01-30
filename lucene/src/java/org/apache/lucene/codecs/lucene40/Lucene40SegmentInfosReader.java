package org.apache.lucene.codecs.lucene40;

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
import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.SegmentInfosReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Default implementation of {@link SegmentInfosReader}.
 * @lucene.experimental
 */
public class Lucene40SegmentInfosReader extends SegmentInfosReader {

  @Override
  public void read(Directory directory, String segmentsFileName, ChecksumIndexInput input, SegmentInfos infos, IOContext context) throws IOException { 
    infos.version = input.readLong(); // read version
    infos.counter = input.readInt(); // read counter
    final int format = infos.getFormat();
    assert format <= SegmentInfos.FORMAT_4_0;
    for (int i = input.readInt(); i > 0; i--) { // read segmentInfos
      SegmentInfo si = readSegmentInfo(directory, format, input);
      assert si.getVersion() != null;
      infos.add(si);
    }
      
    infos.userData = input.readStringStringMap();
  }
  
  public SegmentInfo readSegmentInfo(Directory dir, int format, ChecksumIndexInput input) throws IOException {
    final String version = input.readString();
    final String name = input.readString();
    final int docCount = input.readInt();
    final long delGen = input.readLong();
    // this is still written in 4.0 if we open a 3.x and upgrade the SI
    final int docStoreOffset = input.readInt();
    final String docStoreSegment;
    final boolean docStoreIsCompoundFile;
    if (docStoreOffset != -1) { 
      docStoreSegment = input.readString();
      docStoreIsCompoundFile = input.readByte() == SegmentInfo.YES;
    } else {
      docStoreSegment = name;
      docStoreIsCompoundFile = false;
    }
    final int numNormGen = input.readInt();
    final Map<Integer,Long> normGen;
    if (numNormGen == SegmentInfo.NO) {
      normGen = null;
    } else {
      normGen = new HashMap<Integer, Long>();
      for(int j=0;j<numNormGen;j++) {
        normGen.put(input.readInt(), input.readLong());
      }
    }
    final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;

    final int delCount = input.readInt();
    assert delCount <= docCount;
    final int hasProx = input.readByte();
    final Codec codec = Codec.forName(input.readString());
    final Map<String,String> diagnostics = input.readStringStringMap();
    final int hasVectors = input.readByte();
    
    return new SegmentInfo(dir, version, name, docCount, delGen, docStoreOffset,
      docStoreSegment, docStoreIsCompoundFile, normGen, isCompoundFile,
      delCount, hasProx, codec, diagnostics, hasVectors);
  }
}
