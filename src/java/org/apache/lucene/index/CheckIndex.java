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

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.document.Document;

import java.text.NumberFormat;
import java.io.PrintStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import org.apache.lucene.document.Fieldable;          // for javadoc

/**
 * Basic tool and API to check the health of an index and
 * write a new segments file that removes reference to
 * problematic segments.
 * 
 * <p>As this tool checks every byte in the index, on a large
 * index it can take quite a long time to run.
 *
 * <p><b>WARNING</b>: this tool and API is new and
 * experimental and is subject to suddenly change in the
 * next release.  Please make a complete backup of your
 * index before using this to fix your index!
 */
public class CheckIndex {

  /** Default PrintStream for all CheckIndex instances.
   *  @deprecated Use {@link #setInfoStream} per instance,
   *  instead. */
  public static PrintStream out = null;

  private PrintStream infoStream;
  private Directory dir;

  /**
   * Returned from {@link #checkIndex()} detailing the health and status of the index.
   *
   * <p><b>WARNING</b>: this API is new and experimental and is
   * subject to suddenly change in the next release.
   **/

  public static class Status {

    /** True if no problems were found with the index. */
    public boolean clean;

    /** True if we were unable to locate and load the segments_N file. */
    public boolean missingSegments;

    /** True if we were unable to open the segments_N file. */
    public boolean cantOpenSegments;

    /** True if we were unable to read the version number from segments_N file. */
    public boolean missingSegmentVersion;

    /** Name of latest segments_N file in the index. */
    public String segmentsFileName;

    /** Number of segments in the index. */
    public int numSegments;

    /** String description of the version of the index. */
    public String segmentFormat;

    /** Empty unless you passed specific segments list to check as optional 3rd argument.
     *  @see CheckIndex#checkIndex(List) */
    public List/*<String>*/ segmentsChecked = new ArrayList();
  
    /** True if the index was created with a newer version of Lucene than the CheckIndex tool. */
    public boolean toolOutOfDate;

    /** List of {@link SegmentInfoStatus} instances, detailing status of each segment. */
    public List/*<SegmentInfoStatus*/ segmentInfos = new ArrayList();
  
    /** Directory index is in. */
    public Directory dir;

    /** SegmentInfos instance containing only segments that
     *  had no problems (this is used with the {@link
     *  CheckIndex#fix} method to repair the index. */
    SegmentInfos newSegments;

    /** How many documents will be lost to bad segments. */
    public int totLoseDocCount;

    /** How many bad segments were found. */
    public int numBadSegments;

    /** True if we checked only specific segments ({@link
     * #checkIndex(List)}) was called with non-null
     * argument). */
    public boolean partial;

    /** Holds the status of each segment in the index.
     *  See {@link #segmentInfos}.
     *
     * <p><b>WARNING</b>: this API is new and experimental and is
     * subject to suddenly change in the next release.
     */
    public static class SegmentInfoStatus {
      /** Name of the segment. */
      public String name;

      /** Document count (does not take deletions into account). */
      public int docCount;

      /** True if segment is compound file format. */
      public boolean compound;

      /** Number of files referenced by this segment. */
      public int numFiles;

      /** Net size (MB) of the files referenced by this
       *  segment. */
      public double sizeMB;

      /** Doc store offset, if this segment shares the doc
       *  store files (stored fields and term vectors) with
       *  other segments.  This is -1 if it does not share. */
      public int docStoreOffset = -1;
    
      /** String of the shared doc store segment, or null if
       *  this segment does not share the doc store files. */
      public String docStoreSegment;

      /** True if the shared doc store files are compound file
       *  format. */
      public boolean docStoreCompoundFile;

      /** True if this segment has pending deletions. */
      public boolean hasDeletions;

      /** Name of the current deletions file name. */
      public String deletionsFileName;
    
      /** Number of deleted documents. */
      public int numDeleted;

      /** True if we were able to open a SegmentReader on this
       *  segment. */
      public boolean openReaderPassed;

      /** Number of fields in this segment. */
      int numFields;

      /** True if at least one of the fields in this segment
       *  does not omitTf.
       *  @see Fieldable#setOmitTf */
      public boolean hasProx;
    }
  }

  /** Create a new CheckIndex on the directory. */
  public CheckIndex(Directory dir) {
    this.dir = dir;
    infoStream = out;
  }

  /** Set infoStream where messages should go.  If null, no
   *  messages are printed */
  public void setInfoStream(PrintStream out) {
    infoStream = out;
  }

  private void msg(String msg) {
    if (infoStream != null)
      infoStream.println(msg);
  }

  private static class MySegmentTermDocs extends SegmentTermDocs {

    int delCount;

    MySegmentTermDocs(SegmentReader p) {    
      super(p);
    }

    public void seek(Term term) throws IOException {
      super.seek(term);
      delCount = 0;
    }

    protected void skippingDoc() throws IOException {
      delCount++;
    }
  }

  /** Returns true if index is clean, else false. 
   *  @deprecated Please instantiate a CheckIndex and then use {@link #checkIndex()} instead */
  public static boolean check(Directory dir, boolean doFix) throws IOException {
    return check(dir, doFix, null);
  }

  /** Returns true if index is clean, else false.
   *  @deprecated Please instantiate a CheckIndex and then use {@link #checkIndex(List)} instead */
  public static boolean check(Directory dir, boolean doFix, List onlySegments) throws IOException {
    CheckIndex checker = new CheckIndex(dir);
    Status status = checker.checkIndex(onlySegments);
    if (doFix && !status.clean)
      checker.fixIndex(status);

    return status.clean;
  }

  /** Returns a {@link Status} instance detailing
   *  the state of the index.
   *
   *  <p>As this method checks every byte in the index, on a large
   *  index it can take quite a long time to run.
   *
   *  <p><b>WARNING</b>: make sure
   *  you only call this when the index is not opened by any
   *  writer. */
  public Status checkIndex() throws IOException {
    return checkIndex(null);
  }

  /** Returns a {@link Status} instance detailing
   *  the state of the index.
   * 
   *  @param onlySegments list of specific segment names to check
   *
   *  <p>As this method checks every byte in the specified
   *  segments, on a large index it can take quite a long
   *  time to run.
   *
   *  <p><b>WARNING</b>: make sure
   *  you only call this when the index is not opened by any
   *  writer. */
  public Status checkIndex(List onlySegments) throws IOException {
    NumberFormat nf = NumberFormat.getInstance();
    SegmentInfos sis = new SegmentInfos();
    Status result = new Status();
    result.dir = dir;
    try {
      sis.read(dir);
    } catch (Throwable t) {
      msg("ERROR: could not read any segments file in directory");
      result.missingSegments = true;
      if (infoStream != null)
        t.printStackTrace(infoStream);
      return result;
    }

    final int numSegments = sis.size();
    final String segmentsFileName = sis.getCurrentSegmentFileName();
    IndexInput input = null;
    try {
      input = dir.openInput(segmentsFileName);
    } catch (Throwable t) {
      msg("ERROR: could not open segments file in directory");
      if (infoStream != null)
        t.printStackTrace(infoStream);
      result.cantOpenSegments = true;
      return result;
    }
    int format = 0;
    try {
      format = input.readInt();
    } catch (Throwable t) {
      msg("ERROR: could not read segment file version in directory");
      if (infoStream != null)
        t.printStackTrace(infoStream);
      result.missingSegmentVersion = true;
      return result;
    } finally {
      if (input != null)
        input.close();
    }

    String sFormat = "";
    boolean skip = false;

    if (format == SegmentInfos.FORMAT)
      sFormat = "FORMAT [Lucene Pre-2.1]";
    if (format == SegmentInfos.FORMAT_LOCKLESS)
      sFormat = "FORMAT_LOCKLESS [Lucene 2.1]";
    else if (format == SegmentInfos.FORMAT_SINGLE_NORM_FILE)
      sFormat = "FORMAT_SINGLE_NORM_FILE [Lucene 2.2]";
    else if (format == SegmentInfos.FORMAT_SHARED_DOC_STORE)
      sFormat = "FORMAT_SHARED_DOC_STORE [Lucene 2.3]";
    else {
      if (format == SegmentInfos.FORMAT_CHECKSUM)
        sFormat = "FORMAT_CHECKSUM [Lucene 2.4]";
      else if (format == SegmentInfos.FORMAT_DEL_COUNT)
        sFormat = "FORMAT_DEL_COUNT [Lucene 2.4]";
      else if (format == SegmentInfos.FORMAT_HAS_PROX)
        sFormat = "FORMAT_HAS_PROX [Lucene 2.4]";
      else if (format < SegmentInfos.CURRENT_FORMAT) {
        sFormat = "int=" + format + " [newer version of Lucene than this tool]";
        skip = true;
      } else {
        sFormat = format + " [Lucene 1.3 or prior]";
      }
    }

    msg("Segments file=" + segmentsFileName + " numSegments=" + numSegments + " version=" + sFormat);
    result.segmentsFileName = segmentsFileName;
    result.numSegments = numSegments;
    result.segmentFormat = sFormat;

    if (onlySegments != null) {
      result.partial = true;
      if (infoStream != null)
        infoStream.print("\nChecking only these segments:");
      Iterator it = onlySegments.iterator();
      while (it.hasNext()) {
        if (infoStream != null)
          infoStream.print(" " + it.next());
      }
      result.segmentsChecked.addAll(onlySegments);
      msg(":");
    }

    if (skip) {
      msg("\nERROR: this index appears to be created by a newer version of Lucene than this tool was compiled on; please re-compile this tool on the matching version of Lucene; exiting");
      result.toolOutOfDate = true;
      return result;
    }


    result.newSegments = (SegmentInfos) sis.clone();
    result.newSegments.clear();

    for(int i=0;i<numSegments;i++) {
      final SegmentInfo info = sis.info(i);
      if (onlySegments != null && !onlySegments.contains(info.name))
        continue;
      Status.SegmentInfoStatus segInfoStat = new Status.SegmentInfoStatus();
      result.segmentInfos.add(segInfoStat);
      msg("  " + (1+i) + " of " + numSegments + ": name=" + info.name + " docCount=" + info.docCount);
      segInfoStat.name = info.name;
      segInfoStat.docCount = info.docCount;

      int toLoseDocCount = info.docCount;

      SegmentReader reader = null;

      try {
        msg("    compound=" + info.getUseCompoundFile());
        segInfoStat.compound = info.getUseCompoundFile();
        msg("    hasProx=" + info.getHasProx());
        segInfoStat.hasProx = info.getHasProx();
        msg("    numFiles=" + info.files().size());
        segInfoStat.numFiles = info.files().size();
        msg("    size (MB)=" + nf.format(info.sizeInBytes()/(1024.*1024.)));
        segInfoStat.sizeMB = info.sizeInBytes()/(1024.*1024.);


        final int docStoreOffset = info.getDocStoreOffset();
        if (docStoreOffset != -1) {
          msg("    docStoreOffset=" + docStoreOffset);
          segInfoStat.docStoreOffset = docStoreOffset;
          msg("    docStoreSegment=" + info.getDocStoreSegment());
          segInfoStat.docStoreSegment = info.getDocStoreSegment();
          msg("    docStoreIsCompoundFile=" + info.getDocStoreIsCompoundFile());
          segInfoStat.docStoreCompoundFile = info.getDocStoreIsCompoundFile();
        }
        final String delFileName = info.getDelFileName();
        if (delFileName == null){
          msg("    no deletions");
          segInfoStat.hasDeletions = false;
        }
        else{
          msg("    has deletions [delFileName=" + delFileName + "]");
          segInfoStat.hasDeletions = true;
          segInfoStat.deletionsFileName = delFileName;
        }
        if (infoStream != null)
          infoStream.print("    test: open reader.........");
        reader = SegmentReader.get(info);
        final int numDocs = reader.numDocs();
        toLoseDocCount = numDocs;
        if (reader.hasDeletions()) {
          if (reader.deletedDocs.count() != info.getDelCount()) {
            throw new RuntimeException("delete count mismatch: info=" + info.getDelCount() + " vs deletedDocs.count()=" + reader.deletedDocs.count());
          }
          if (reader.deletedDocs.count() > reader.maxDoc()) {
            throw new RuntimeException("too many deleted docs: maxDoc()=" + reader.maxDoc() + " vs deletedDocs.count()=" + reader.deletedDocs.count());
          }
          if (info.docCount - numDocs != info.getDelCount()){
            throw new RuntimeException("delete count mismatch: info=" + info.getDelCount() + " vs reader=" + (info.docCount - numDocs));
          }
          segInfoStat.numDeleted = info.docCount - numDocs;
          msg("OK [" + (segInfoStat.numDeleted) + " deleted docs]");
        } else {
          if (info.getDelCount() != 0) {
            throw new RuntimeException("delete count mismatch: info=" + info.getDelCount() + " vs reader=" + (info.docCount - numDocs));
          }
          msg("OK");
        }

        if (infoStream != null)
          infoStream.print("    test: fields, norms.......");
        Collection fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL);
        Iterator it = fieldNames.iterator();
        while(it.hasNext()) {
          final String fieldName = (String) it.next();
          byte[] b = reader.norms(fieldName);
          if (b.length != info.docCount)
            throw new RuntimeException("norms for field \"" + fieldName + "\" is length " + b.length + " != maxDoc " + info.docCount);

        }
        msg("OK [" + fieldNames.size() + " fields]");
        segInfoStat.numFields = fieldNames.size();
        if (infoStream != null)
          infoStream.print("    test: terms, freq, prox...");
        final TermEnum termEnum = reader.terms();
        final TermPositions termPositions = reader.termPositions();

        // Used only to count up # deleted docs for this
        // term
        final MySegmentTermDocs myTermDocs = new MySegmentTermDocs(reader);

        long termCount = 0;
        long totFreq = 0;
        long totPos = 0;
        final int maxDoc = reader.maxDoc();

        while(termEnum.next()) {
          termCount++;
          final Term term = termEnum.term();
          final int docFreq = termEnum.docFreq();
          termPositions.seek(term);
          int lastDoc = -1;
          int freq0 = 0;
          totFreq += docFreq;
          while(termPositions.next()) {
            freq0++;
            final int doc = termPositions.doc();
            final int freq = termPositions.freq();
            if (doc <= lastDoc)
              throw new RuntimeException("term " + term + ": doc " + doc + " <= lastDoc " + lastDoc);
            if (doc >= maxDoc)
              throw new RuntimeException("term " + term + ": doc " + doc + " >= maxDoc " + maxDoc);

            lastDoc = doc;
            if (freq <= 0)
              throw new RuntimeException("term " + term + ": doc " + doc + ": freq " + freq + " is out of bounds");
            
            int lastPos = -1;
            totPos += freq;
            for(int j=0;j<freq;j++) {
              final int pos = termPositions.nextPosition();
              if (pos < -1)
                throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " is out of bounds");
              if (pos < lastPos)
                throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " < lastPos " + lastPos);
            }
          }

          // Now count how many deleted docs occurred in
          // this term:
          final int delCount;
          if (reader.hasDeletions()) {
            myTermDocs.seek(term);
            while(myTermDocs.next()) {
            }
            delCount = myTermDocs.delCount;
          } else
            delCount = 0;

          if (freq0 + delCount != docFreq)
            throw new RuntimeException("term " + term + " docFreq=" + docFreq + " != num docs seen " + freq0 + " + num docs deleted " + delCount);
        }

        msg("OK [" + termCount + " terms; " + totFreq + " terms/docs pairs; " + totPos + " tokens]");

        if (infoStream != null)
          infoStream.print("    test: stored fields.......");
        int docCount = 0;
        long totFields = 0;
        for(int j=0;j<info.docCount;j++)
          if (!reader.isDeleted(j)) {
            docCount++;
            Document doc = reader.document(j);
            totFields += doc.getFields().size();
          }

        if (docCount != reader.numDocs())
          throw new RuntimeException("docCount=" + docCount + " but saw " + docCount + " undeleted docs");

        msg("OK [" + totFields + " total field count; avg " + nf.format((((float) totFields)/docCount)) + " fields per doc]");

        if (infoStream != null)
          infoStream.print("    test: term vectors........");
        int totVectors = 0;
        for(int j=0;j<info.docCount;j++)
          if (!reader.isDeleted(j)) {
            TermFreqVector[] tfv = reader.getTermFreqVectors(j);
            if (tfv != null)
              totVectors += tfv.length;
          }

        msg("OK [" + totVectors + " total vector count; avg " + nf.format((((float) totVectors)/docCount)) + " term/freq vector fields per doc]");
        msg("");

      } catch (Throwable t) {
        msg("FAILED");
        String comment;
        comment = "fixIndex() would remove reference to this segment";
        msg("    WARNING: " + comment + "; full exception:");
        if (infoStream != null)
          t.printStackTrace(infoStream);
        msg("");
        result.totLoseDocCount += toLoseDocCount;
        result.numBadSegments++;
        continue;
      } finally {
        if (reader != null)
          reader.close();
      }

      // Keeper
      result.newSegments.add(info.clone());
    }

    if (0 == result.numBadSegments) {
      result.clean = true;
      msg("No problems were detected with this index.\n");
    } else
      msg("WARNING: " + result.numBadSegments + " broken segments (containing " + result.totLoseDocCount + " documents) detected");

    return result;
  }
  
  /** Repairs the index using previously returned result
   *  from {@link #checkIndex}.  Note that this does not
   *  remove any of the unreferenced files after it's done;
   *  you must separately open an {@link IndexWriter}, which
   *  deletes unreferenced files when it's created.
   *
   * <p><b>WARNING</b>: this writes a
   *  new segments file into the index, effectively removing
   *  all documents in broken segments from the index.
   *  BE CAREFUL.
   *
   * <p><b>WARNING</b>: Make sure you only call this when the
   *  index is not opened  by any writer. */
  public void fixIndex(Status result) throws IOException {
    if (result.partial)
      throw new IllegalArgumentException("can only fix an index that was fully checked (this status checked a subset of segments)");
    result.newSegments.commit(result.dir);
  }

  private static boolean assertsOn;

  private static boolean testAsserts() {
    assertsOn = true;
    return true;
  }

  private static boolean assertsOn() {
    assert testAsserts();
    return assertsOn;
  }

  /** Command-line interface to check and fix an index.

    <p>
    Run it like this:
    <pre>
    java -ea:org.apache.lucene... org.apache.lucene.index.CheckIndex pathToIndex [-fix] [-segment X] [-segment Y]
    </pre>
    <ul>
    <li><code>-fix</code>: actually write a new segments_N file, removing any problematic segments

    <li><code>-segment X</code>: only check the specified
    segment(s).  This can be specified multiple times,
    to check more than one segment, eg <code>-segment _2
    -segment _a</code>.  You can't use this with the -fix
    option.
    </ul>

    <p><b>WARNING</b>: <code>-fix</code> should only be used on an emergency basis as it will cause
                       documents (perhaps many) to be permanently removed from the index.  Always make
                       a backup copy of your index before running this!  Do not run this tool on an index
                       that is actively being written to.  You have been warned!

    <p>                Run without -fix, this tool will open the index, report version information
                       and report any exceptions it hits and what action it would take if -fix were
                       specified.  With -fix, this tool will remove any segments that have issues and
                       write a new segments_N file.  This means all documents contained in the affected
                       segments will be removed.

    <p>
                       This tool exits with exit code 1 if the index cannot be opened or has any
                       corruption, else 0.
   */
  public static void main(String[] args) throws IOException {

    boolean doFix = false;
    List onlySegments = new ArrayList();
    String indexPath = null;
    int i = 0;
    while(i < args.length) {
      if (args[i].equals("-fix")) {
        doFix = true;
        i++;
      } else if (args[i].equals("-segment")) {
        if (i == args.length-1) {
          System.out.println("ERROR: missing name for -segment option");
          System.exit(1);
        }
        onlySegments.add(args[i+1]);
        i += 2;
      } else {
        if (indexPath != null) {
          System.out.println("ERROR: unexpected extra argument '" + args[i] + "'");
          System.exit(1);
        }
        indexPath = args[i];
        i++;
      }
    }

    if (indexPath == null) {
      System.out.println("\nERROR: index path not specified");
      System.out.println("\nUsage: java org.apache.lucene.index.CheckIndex pathToIndex [-fix] [-segment X] [-segment Y]\n" +
                         "\n" +
                         "  -fix: actually write a new segments_N file, removing any problematic segments\n" +
                         "  -segment X: only check the specified segments.  This can be specified multiple\n" + 
                         "              times, to check more than one segment, eg '-segment _2 -segment _a'.\n" +
                         "              You can't use this with the -fix option\n" +
                         "\n" + 
                         "**WARNING**: -fix should only be used on an emergency basis as it will cause\n" +
                         "documents (perhaps many) to be permanently removed from the index.  Always make\n" +
                         "a backup copy of your index before running this!  Do not run this tool on an index\n" +
                         "that is actively being written to.  You have been warned!\n" +
                         "\n" +
                         "Run without -fix, this tool will open the index, report version information\n" +
                         "and report any exceptions it hits and what action it would take if -fix were\n" +
                         "specified.  With -fix, this tool will remove any segments that have issues and\n" + 
                         "write a new segments_N file.  This means all documents contained in the affected\n" +
                         "segments will be removed.\n" +
                         "\n" +
                         "This tool exits with exit code 1 if the index cannot be opened or has any\n" +
                         "corruption, else 0.\n");
      System.exit(1);
    }

    if (!assertsOn())
      System.out.println("\nNOTE: testing will be more thorough if you run java with '-ea:org.apache.lucene...', so assertions are enabled");

    if (onlySegments.size() == 0)
      onlySegments = null;
    else if (doFix) {
      System.out.println("ERROR: cannot specify both -fix and -segment");
      System.exit(1);
    }

    System.out.println("\nOpening index @ " + indexPath + "\n");
    Directory dir = null;
    try {
      dir = FSDirectory.getDirectory(indexPath);
    } catch (Throwable t) {
      System.out.println("ERROR: could not open directory \"" + indexPath + "\"; exiting");
      t.printStackTrace(System.out);
      System.exit(1);
    }

    CheckIndex checker = new CheckIndex(dir);
    checker.setInfoStream(System.out);

    Status result = checker.checkIndex(onlySegments);

    if (!result.clean) {
      if (!doFix) {
        System.out.println("WARNING: would write new segments file, and " + result.totLoseDocCount + " documents would be lost, if -fix were specified\n");
      } else {
        System.out.println("WARNING: " + result.totLoseDocCount + " documents will be lost\n");
        System.out.println("NOTE: will write new segments file in 5 seconds; this will remove " + result.totLoseDocCount + " docs from the index. THIS IS YOUR LAST CHANCE TO CTRL+C!");
        for(int s=0;s<5;s++) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            s--;
            continue;
          }
          System.out.println("  " + (5-s) + "...");
        }
        System.out.println("Writing...");
        checker.fixIndex(result);
        System.out.println("OK");
        System.out.println("Wrote new segments file \"" + result.newSegments.getCurrentSegmentFileName() + "\"");
      }
    }
    System.out.println("");

    final int exitCode;
    if (result != null && result.clean == true)
      exitCode = 0;
    else
      exitCode = 1;
    System.exit(exitCode);
  }
}
