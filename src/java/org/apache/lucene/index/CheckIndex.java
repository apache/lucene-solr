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

/**
 * Basic tool to check the health of an index and write a
 * new segments file that removes reference to problematic
 * segments.  There are many more checks that this tool
 * could do but does not yet, eg: reconstructing a segments
 * file by looking for all loadable segments (if no segments
 * file is found), removing specifically specified segments,
 * listing files that exist but are not referenced, etc.
 */

public class CheckIndex {

  public static PrintStream out = System.out;

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

  /** Returns true if index is clean, else false.*/
  public static boolean check(Directory dir, boolean doFix) throws IOException {
    return check(dir, doFix, null);
  }

  /** Returns true if index is clean, else false.*/
  public static boolean check(Directory dir, boolean doFix, List onlySegments) throws IOException {
    NumberFormat nf = NumberFormat.getInstance();
    SegmentInfos sis = new SegmentInfos();
    
    try {
      sis.read(dir);
    } catch (Throwable t) {
      out.println("ERROR: could not read any segments file in directory");
      t.printStackTrace(out);
      return false;
    }

    final int numSegments = sis.size();
    final String segmentsFileName = sis.getCurrentSegmentFileName();
    IndexInput input = null;
    try {
      input = dir.openInput(segmentsFileName);
    } catch (Throwable t) {
      out.println("ERROR: could not open segments file in directory");
      t.printStackTrace(out);
      return false;
    }
    int format = 0;
    try {
      format = input.readInt();
    } catch (Throwable t) {
      out.println("ERROR: could not read segment file version in directory");
      t.printStackTrace(out);
      return false;
    } finally {
      if (input != null)
        input.close();
    }

    String sFormat = "";
    boolean skip = false;
    boolean allowMinusOnePosition = true;

    if (format == SegmentInfos.FORMAT)
      sFormat = "FORMAT [Lucene Pre-2.1]";
    if (format == SegmentInfos.FORMAT_LOCKLESS)
      sFormat = "FORMAT_LOCKLESS [Lucene 2.1]";
    else if (format == SegmentInfos.FORMAT_SINGLE_NORM_FILE)
      sFormat = "FORMAT_SINGLE_NORM_FILE [Lucene 2.2]";
    else if (format == SegmentInfos.FORMAT_SHARED_DOC_STORE)
      sFormat = "FORMAT_SHARED_DOC_STORE [Lucene 2.3]";
    else {
      // LUCENE-1255: All versions before 2.3.2/2.4 were
      // able to create position=-1 when the very first
      // Token has positionIncrement 0
      allowMinusOnePosition = false;
      if (format == SegmentInfos.FORMAT_CHECKSUM)
        sFormat = "FORMAT_CHECKSUM [Lucene 2.4]";
      else if (format == SegmentInfos.FORMAT_DEL_COUNT)
          sFormat = "FORMAT_DEL_COUNT [Lucene 2.4]";
      else if (format < SegmentInfos.CURRENT_FORMAT) {
        sFormat = "int=" + format + " [newer version of Lucene than this tool]";
        skip = true;
      } else {
        sFormat = format + " [Lucene 1.3 or prior]";
      }
    }

    out.println("Segments file=" + segmentsFileName + " numSegments=" + numSegments + " version=" + sFormat);

    if (onlySegments != null) {
      out.print("\nChecking only these segments:");
      Iterator it = onlySegments.iterator();
      while (it.hasNext()) {
        out.print(" " + it.next());
      }
      out.println(":");
    }

    if (skip) {
      out.println("\nERROR: this index appears to be created by a newer version of Lucene than this tool was compiled on; please re-compile this tool on the matching version of Lucene; exiting");
      return false;
    }

    SegmentInfos newSIS = (SegmentInfos) sis.clone();
    newSIS.clear();
    boolean changed = false;
    int totLoseDocCount = 0;
    int numBadSegments = 0;
    for(int i=0;i<numSegments;i++) {
      final SegmentInfo info = sis.info(i);
      if (onlySegments != null && !onlySegments.contains(info.name))
        continue;
      out.println("  " + (1+i) + " of " + numSegments + ": name=" + info.name + " docCount=" + info.docCount);
      int toLoseDocCount = info.docCount;

      SegmentReader reader = null;

      try {
        out.println("    compound=" + info.getUseCompoundFile());
        out.println("    numFiles=" + info.files().size());
        out.println("    size (MB)=" + nf.format(info.sizeInBytes()/(1024.*1024.)));
        final int docStoreOffset = info.getDocStoreOffset();
        if (docStoreOffset != -1) {
          out.println("    docStoreOffset=" + docStoreOffset);
          out.println("    docStoreSegment=" + info.getDocStoreSegment());
          out.println("    docStoreIsCompoundFile=" + info.getDocStoreIsCompoundFile());
        }
        final String delFileName = info.getDelFileName();
        if (delFileName == null)
          out.println("    no deletions");
        else
          out.println("    has deletions [delFileName=" + delFileName + "]");
        out.print("    test: open reader.........");
        reader = SegmentReader.get(info);
        final int numDocs = reader.numDocs();
        toLoseDocCount = numDocs;
        if (reader.hasDeletions()) {
          if (info.docCount - numDocs != info.getDelCount())
            throw new RuntimeException("delete count mismatch: info=" + info.getDelCount() + " vs reader=" + (info.docCount - numDocs));
          out.println("OK [" + (info.docCount - numDocs) + " deleted docs]");
        } else {
          if (info.getDelCount() != 0)
            throw new RuntimeException("delete count mismatch: info=" + info.getDelCount() + " vs reader=" + (info.docCount - numDocs));
          out.println("OK");
        }

        out.print("    test: fields, norms.......");
        Collection fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL);
        Iterator it = fieldNames.iterator();
        while(it.hasNext()) {
          final String fieldName = (String) it.next();
          byte[] b = reader.norms(fieldName);
          if (b.length != info.docCount)
            throw new RuntimeException("norms for field \"" + fieldName + "\" is length " + b.length + " != maxDoc " + info.docCount);

        }
        out.println("OK [" + fieldNames.size() + " fields]");

        out.print("    test: terms, freq, prox...");
        final TermEnum termEnum = reader.terms();
        final TermPositions termPositions = reader.termPositions();

        // Used only to count up # deleted docs for this
        // term
        final MySegmentTermDocs myTermDocs = new MySegmentTermDocs(reader);

        long termCount = 0;
        long totFreq = 0;
        long totPos = 0;
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
              throw new RuntimeException("term " + term + ": doc " + doc + " < lastDoc " + lastDoc);
            lastDoc = doc;
            if (freq <= 0)
              throw new RuntimeException("term " + term + ": doc " + doc + ": freq " + freq + " is out of bounds");
            
            int lastPos = -1;
            totPos += freq;
            for(int j=0;j<freq;j++) {
              final int pos = termPositions.nextPosition();
              if (pos < -1 || (pos == -1 && !allowMinusOnePosition))
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

        out.println("OK [" + termCount + " terms; " + totFreq + " terms/docs pairs; " + totPos + " tokens]");

        out.print("    test: stored fields.......");
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

        out.println("OK [" + totFields + " total field count; avg " + nf.format((((float) totFields)/docCount)) + " fields per doc]");

        out.print("    test: term vectors........");
        int totVectors = 0;
        for(int j=0;j<info.docCount;j++)
          if (!reader.isDeleted(j)) {
            TermFreqVector[] tfv = reader.getTermFreqVectors(j);
            if (tfv != null)
              totVectors += tfv.length;
          }

        out.println("OK [" + totVectors + " total vector count; avg " + nf.format((((float) totVectors)/docCount)) + " term/freq vector fields per doc]");
        out.println("");

      } catch (Throwable t) {
        out.println("FAILED");
        String comment;
        if (doFix)
          comment = "will remove reference to this segment (-fix is specified)";
        else
          comment = "would remove reference to this segment (-fix was not specified)";
        out.println("    WARNING: " + comment + "; full exception:");
        t.printStackTrace(out);
        out.println("");
        totLoseDocCount += toLoseDocCount;
        numBadSegments++;
        changed = true;
        continue;
      } finally {
        if (reader != null)
          reader.close();
      }

      // Keeper
      newSIS.add(info.clone());
    }

    if (!changed) {
      out.println("No problems were detected with this index.\n");
      return true;
    } else {
      out.println("WARNING: " + numBadSegments + " broken segments detected");
      if (doFix)
        out.println("WARNING: " + totLoseDocCount + " documents will be lost");
      else
        out.println("WARNING: " + totLoseDocCount + " documents would be lost if -fix were specified");
      out.println();
    }

    if (doFix) {
      out.println("NOTE: will write new segments file in 5 seconds; this will remove " + totLoseDocCount + " docs from the index. THIS IS YOUR LAST CHANCE TO CTRL+C!");
      for(int i=0;i<5;i++) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          i--;
          continue;
        }
          
        out.println("  " + (5-i) + "...");
      }
      out.print("Writing...");
      try {
        newSIS.commit(dir);
      } catch (Throwable t) {
        out.println("FAILED; exiting");
        t.printStackTrace(out);
        return false;
      }
      out.println("OK");
      out.println("Wrote new segments file \"" + newSIS.getCurrentSegmentFileName() + "\"");
    } else {
      out.println("NOTE: would write new segments file [-fix was not specified]");
    }
    out.println("");

    return false;
  }

  static boolean assertsOn;

  private static boolean testAsserts() {
    assertsOn = true;
    return true;
  }

  public static void main(String[] args) throws Throwable {

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
          out.println("ERROR: missing name for -segment option");
          System.exit(1);
        }
        onlySegments.add(args[i+1]);
        i += 2;
      } else {
        if (indexPath != null) {
          out.println("ERROR: unexpected extra argument '" + args[i] + "'");
          System.exit(1);
        }
        indexPath = args[i];
        i++;
      }
    }

    if (indexPath == null) {
      out.println("\nERROR: index path not specified");
      out.println("\nUsage: java org.apache.lucene.index.CheckIndex pathToIndex [-fix] [-segment X] [-segment Y]\n" +
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
                         "This tool exits with exit code 1 if the index cannot be opened or has has any\n" +
                         "corruption, else 0.\n");
      System.exit(1);
    }

    if (onlySegments.size() == 0)
      onlySegments = null;
    else if (doFix) {
      out.println("ERROR: cannot specify both -fix and -segment");
      System.exit(1);
    }

    assert testAsserts();
    if (!assertsOn)
      out.println("\nNOTE: testing will be more thorough if you run java with '-ea:org.apache.lucene', so assertions are enabled");

    out.println("\nOpening index @ " + indexPath + "\n");
    Directory dir = null;
    try {
      dir = FSDirectory.getDirectory(indexPath);
    } catch (Throwable t) {
      out.println("ERROR: could not open directory \"" + indexPath + "\"; exiting");
      t.printStackTrace(out);
      System.exit(1);
    }

    boolean isClean = check(dir, doFix, onlySegments);

    final int exitCode;
    if (isClean)
      exitCode = 0;
    else
      exitCode = 1;
    System.exit(exitCode);
  }    
}
