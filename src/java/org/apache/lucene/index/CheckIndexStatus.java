package org.apache.lucene.index;

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