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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.SegmentInfosReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.codecs.simpletext.SimpleTextSegmentInfosWriter.*;

/**
 * reads plaintext segments files
 * <p>
 * <b><font color="red">FOR RECREATIONAL USE ONLY</font></B>
 * @lucene.experimental
 */
public class SimpleTextSegmentInfosReader extends SegmentInfosReader {

  @Override
  public void read(Directory directory, String segmentsFileName, ChecksumIndexInput input, SegmentInfos infos, IOContext context) throws IOException {
    final BytesRef scratch = new BytesRef();
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, VERSION);
    infos.version = Long.parseLong(readString(VERSION.length, scratch));
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, COUNTER);
    infos.counter = Integer.parseInt(readString(COUNTER.length, scratch));
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, NUM_USERDATA);
    int numUserData = Integer.parseInt(readString(NUM_USERDATA.length, scratch));
    infos.userData = new HashMap<String,String>();

    for (int i = 0; i < numUserData; i++) {
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, USERDATA_KEY);
      String key = readString(USERDATA_KEY.length, scratch);
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, USERDATA_VALUE);
      String value = readString(USERDATA_VALUE.length, scratch);
      infos.userData.put(key, value);
    }
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, NUM_SEGMENTS);
    int numSegments = Integer.parseInt(readString(NUM_SEGMENTS.length, scratch));
    
    for (int i = 0; i < numSegments; i++) {
      infos.add(readSegmentInfo(directory, input, scratch));
    }
  }
  
  public SegmentInfo readSegmentInfo(Directory directory, DataInput input, BytesRef scratch) throws IOException {
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_NAME);
    final String name = readString(SI_NAME.length, scratch);
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_CODEC);
    final Codec codec = Codec.forName(readString(SI_CODEC.length, scratch));
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_VERSION);
    final String version = readString(SI_VERSION.length, scratch);
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_DOCCOUNT);
    final int docCount = Integer.parseInt(readString(SI_DOCCOUNT.length, scratch));
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_DELCOUNT);
    final int delCount = Integer.parseInt(readString(SI_DELCOUNT.length, scratch));
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_HASPROX);
    final int hasProx = readTernary(SI_HASPROX.length, scratch);
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_HASVECTORS);
    final int hasVectors = readTernary(SI_HASVECTORS.length, scratch);
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_USECOMPOUND);
    final boolean isCompoundFile = Boolean.parseBoolean(readString(SI_USECOMPOUND.length, scratch));
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_DSOFFSET);
    final int dsOffset = Integer.parseInt(readString(SI_DSOFFSET.length, scratch));
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_DSSEGMENT);
    final String dsSegment = readString(SI_DSSEGMENT.length, scratch);
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_DSCOMPOUND);
    final boolean dsCompoundFile = Boolean.parseBoolean(readString(SI_DSCOMPOUND.length, scratch));
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_DELGEN);
    final long delGen = Long.parseLong(readString(SI_DELGEN.length, scratch));
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_NUM_NORMGEN);
    final int numNormGen = Integer.parseInt(readString(SI_NUM_NORMGEN.length, scratch));
    final Map<Integer,Long> normGen;
    if (numNormGen == 0) {
      normGen = null;
    } else {
      normGen = new HashMap<Integer,Long>();
      for (int i = 0; i < numNormGen; i++) {
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, SI_NORMGEN_KEY);
        int key = Integer.parseInt(readString(SI_NORMGEN_KEY.length, scratch));
        
        SimpleTextUtil.readLine(input, scratch);
        assert StringHelper.startsWith(scratch, SI_NORMGEN_VALUE);
        long value = Long.parseLong(readString(SI_NORMGEN_VALUE.length, scratch));
        normGen.put(key, value);
      }
    }
    
    SimpleTextUtil.readLine(input, scratch);
    assert StringHelper.startsWith(scratch, SI_NUM_DIAG);
    int numDiag = Integer.parseInt(readString(SI_NUM_DIAG.length, scratch));
    Map<String,String> diagnostics = new HashMap<String,String>();

    for (int i = 0; i < numDiag; i++) {
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, SI_DIAG_KEY);
      String key = readString(SI_DIAG_KEY.length, scratch);
      
      SimpleTextUtil.readLine(input, scratch);
      assert StringHelper.startsWith(scratch, SI_DIAG_VALUE);
      String value = readString(SI_DIAG_VALUE.length, scratch);
      diagnostics.put(key, value);
    }
    
    return new SegmentInfo(directory, version, name, docCount, delGen, dsOffset,
        dsSegment, dsCompoundFile, normGen, isCompoundFile,
        delCount, hasProx, codec, diagnostics, hasVectors);
  }
  
  private String readString(int offset, BytesRef scratch) {
    return new String(scratch.bytes, scratch.offset+offset, scratch.length-offset, IOUtils.CHARSET_UTF_8);
  }
  
  private int readTernary(int offset, BytesRef scratch) throws IOException {
    String s = readString(offset, scratch);
    if ("check fieldinfo".equals(s)) {
      return SegmentInfo.CHECK_FIELDINFO;
    } else if ("true".equals(s)) {
      return SegmentInfo.YES;
    } else if ("false".equals(s)) {
      return 0;
    } else {
      throw new CorruptIndexException("invalid ternary value: " + s);
    }
  }
}
