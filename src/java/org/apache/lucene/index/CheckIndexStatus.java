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

import org.apache.lucene.store.Directory;

import java.util.List;
import java.util.ArrayList;


/**
 *
 *
 **/
public class CheckIndexStatus {

  public boolean clean;


  public boolean missingSegments;
  public boolean cantOpenSegments;
  public boolean missingSegmentVersion;


  public String segmentsFileName;
  public int numSegments;
  public String segmentFormat;
  public List/*<String>*/ segmentsChecked = new ArrayList();

  public boolean toolOutOfDate;

  public List/*<SegmentInfoStatus*/ segmentInfos = new ArrayList();
  public Directory dir;
  public SegmentInfos newSegments;
  public int totLoseDocCount;
  public int numBadSegments;

  public static class SegmentInfoStatus{
    public String name;
    public int docCount;
    public boolean compound;
    public int numFiles;
    public double sizeMB;
    public int docStoreOffset = -1;
    public String docStoreSegment;
    public boolean docStoreCompoundFile;

    public boolean hasDeletions;
    public String deletionsFileName;
    public int numDeleted;

    public boolean openReaderPassed;

    int numFields;

    public boolean hasProx;
  }

}