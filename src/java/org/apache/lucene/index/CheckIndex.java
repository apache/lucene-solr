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
import java.util.Collection;
import java.util.Iterator;

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

  public static void main(String[] args) throws Throwable {

    boolean doFix = false;
    for(int i=0;i<args.length;i++)
      if (args[i].equals("-fix")) {
        doFix = true;
        break;
      }

    if (args.length != (doFix ? 2:1)) {
      System.out.println("\nUsage: java org.apache.lucene.index.CheckIndex pathToIndex [-fix]\n" +
                         "\n" +
                         "  -fix: actually write a new segments_N file, removing any problematic segments\n" +
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
                         "segments will be removed.\n");
      System.exit(1);
    }

    NumberFormat nf = NumberFormat.getInstance();
    
    SegmentInfos sis = new SegmentInfos();
    final String dirName = args[0];
    System.out.println("\nOpening index @ " + dirName + "\n");
    Directory dir = null;
    try {
      dir = FSDirectory.getDirectory(dirName);
    } catch (Throwable t) {
      System.out.println("ERROR: could not open directory \"" + dirName + "\"; exiting");
      t.printStackTrace(System.out);
      System.exit(1);
    } 
    
    try {
      sis.read(dir);
    } catch (Throwable t) {
      System.out.println("ERROR: could not read any segments file in directory \"" + dirName + "\"; exiting");
      t.printStackTrace(System.out);
      System.exit(1);
    }

    final int numSegments = sis.size();
    final String segmentsFileName = sis.getCurrentSegmentFileName();
    IndexInput input = null;
    try {
      input = dir.openInput(segmentsFileName);
    } catch (Throwable t) {
      System.out.println("ERROR: could not open segments file in directory \"" + dirName + "\"; exiting");
      t.printStackTrace(System.out);
      System.exit(1);
    }
    int format = 0;
    try {
      format = input.readInt();
    } catch (Throwable t) {
      System.out.println("ERROR: could not read segment file version in directory \"" + dirName + "\"; exiting");
      t.printStackTrace(System.out);
      System.exit(1);
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
    else if (format < SegmentInfos.FORMAT_SHARED_DOC_STORE) {
      sFormat = "int=" + format + " [newer version of Lucene than this tool]";
      skip = true;
    } else {
      sFormat = format + " [Lucene 1.3 or prior]";
    }

    System.out.println("Segments file=" + segmentsFileName + " numSegments=" + numSegments + " version=" + sFormat);

    if (skip) {
      System.out.println("\nERROR: this index appears to be created by a newer version of Lucene than this tool was compiled on; please re-compile this tool on the matching version of Lucene; exiting");
      System.exit(1);
    }

    SegmentInfos newSIS = (SegmentInfos) sis.clone();
    newSIS.clear();
    boolean changed = false;
    int totLoseDocCount = 0;
    int numBadSegments = 0;
    for(int i=0;i<numSegments;i++) {
      final SegmentInfo info = sis.info(i);
      System.out.println("  " + (1+i) + " of " + numSegments + ": name=" + info.name + " docCount=" + info.docCount);
      int toLoseDocCount = info.docCount;

      SegmentReader reader = null;

      try {
        System.out.println("    compound=" + info.getUseCompoundFile());
        System.out.println("    numFiles=" + info.files().size());
        System.out.println("    size (MB)=" + nf.format(info.sizeInBytes()/(1024.*1024.)));
        final int docStoreOffset = info.getDocStoreOffset();
        if (docStoreOffset != -1) {
          System.out.println("    docStoreOffset=" + docStoreOffset);
          System.out.println("    docStoreSegment=" + info.getDocStoreSegment());
          System.out.println("    docStoreIsCompoundFile=" + info.getDocStoreIsCompoundFile());
        }
        final String delFileName = info.getDelFileName();
        if (delFileName == null)
          System.out.println("    no deletions");
        else
          System.out.println("    has deletions [delFileName=" + delFileName + "]");
        System.out.print("    test: open reader.........");
        reader = SegmentReader.get(info);
        final int numDocs = reader.numDocs();
        toLoseDocCount = numDocs;
        if (reader.hasDeletions())
          System.out.println("OK [" + (info.docCount - numDocs) + " deleted docs]");
        else
          System.out.println("OK");

        System.out.print("    test: fields, norms.......");
        Collection fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL);
        Iterator it = fieldNames.iterator();
        while(it.hasNext()) {
          final String fieldName = (String) it.next();
          byte[] b = reader.norms(fieldName);
          if (b.length != info.docCount)
            throw new RuntimeException("norms for field \"" + fieldName + "\" is length " + b.length + " != maxDoc " + info.docCount);

        }
        System.out.println("OK [" + fieldNames.size() + " fields]");

        System.out.print("    test: terms, freq, prox...");
        final TermEnum termEnum = reader.terms();
        final TermPositions termPositions = reader.termPositions();
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
              if (pos < 0)
                throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " is out of bounds");
              if (pos <= lastPos)
                throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " < lastPos " + lastPos);
            }
          }
          if (freq0 != docFreq)
            throw new RuntimeException("term " + term + " docFreq=" + docFreq + " != num docs seen " + freq0);
        }

        System.out.println("OK [" + termCount + " terms; " + totFreq + " terms/docs pairs; " + totPos + " tokens]");

        System.out.print("    test: stored fields.......");
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

        System.out.println("OK [" + totFields + " total field count; avg " + nf.format((((float) totFields)/docCount)) + " fields per doc]");

        System.out.print("    test: term vectors........");
        int totVectors = 0;
        for(int j=0;j<info.docCount;j++)
          if (!reader.isDeleted(j)) {
            TermFreqVector[] tfv = reader.getTermFreqVectors(j);
            if (tfv != null)
              totVectors += tfv.length;
          }

        System.out.println("OK [" + totVectors + " total vector count; avg " + nf.format((((float) totVectors)/docCount)) + " term/freq vector fields per doc]");
        System.out.println("");

      } catch (Throwable t) {
        System.out.println("FAILED");
        String comment;
        if (doFix)
          comment = "will remove reference to this segment (-fix is specified)";
        else
          comment = "would remove reference to this segment (-fix was not specified)";
        System.out.println("    WARNING: " + comment + "; full exception:");
        t.printStackTrace(System.out);
        System.out.println("");
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
      System.out.println("No problems were detected with this index.\n");
      System.exit(0);
    } else {
      System.out.println("WARNING: " + numBadSegments + " broken segments detected");
      if (doFix)
        System.out.println("WARNING: " + totLoseDocCount + " documents will be lost");
      else
        System.out.println("WARNING: " + totLoseDocCount + " documents would be lost if -fix were specified");
      System.out.println("");
    }

    if (doFix) {
      System.out.println("NOTE: will write new segments file in 5 seconds; this will remove " + totLoseDocCount + " docs from the index. THIS IS YOUR LAST CHANCE TO CTRL+C!");
      for(int i=0;i<5;i++) {
        Thread.sleep(1000);
        System.out.println("  " + (5-i) + "...");
      }
      System.out.print("Writing...");
      try {
        newSIS.write(dir);
      } catch (Throwable t) {
        System.out.println("FAILED; exiting");
        t.printStackTrace(System.out);
        System.exit(1);
      }
      System.out.println("OK");
      System.out.println("Wrote new segments file \"" + newSIS.getCurrentSegmentFileName() + "\"");
    } else {
      System.out.println("NOTE: would write new segments file [-fix was not specified]");
    }
    System.out.println("");

    System.exit(0);
  }
}
