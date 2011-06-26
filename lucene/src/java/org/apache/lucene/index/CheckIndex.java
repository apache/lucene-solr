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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.document.AbstractField;  // for javadocs
import org.apache.lucene.document.Document;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.DefaultSegmentInfosWriter;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.index.values.ValuesEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import java.text.NumberFormat;
import java.io.PrintStream;
import java.io.IOException;
import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * Basic tool and API to check the health of an index and
 * write a new segments file that removes reference to
 * problematic segments.
 * 
 * <p>As this tool checks every byte in the index, on a large
 * index it can take quite a long time to run.
 *
 * @lucene.experimental Please make a complete backup of your
 * index before using this to fix your index!
 */
public class CheckIndex {

  private PrintStream infoStream;
  private Directory dir;

  /**
   * Returned from {@link #checkIndex()} detailing the health and status of the index.
   *
   * @lucene.experimental
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
    public List<String> segmentsChecked = new ArrayList<String>();
  
    /** True if the index was created with a newer version of Lucene than the CheckIndex tool. */
    public boolean toolOutOfDate;

    /** List of {@link SegmentInfoStatus} instances, detailing status of each segment. */
    public List<SegmentInfoStatus> segmentInfos = new ArrayList<SegmentInfoStatus>();
  
    /** Directory index is in. */
    public Directory dir;

    /** 
     * SegmentInfos instance containing only segments that
     * had no problems (this is used with the {@link CheckIndex#fixIndex} 
     * method to repair the index. 
     */
    SegmentInfos newSegments;

    /** How many documents will be lost to bad segments. */
    public int totLoseDocCount;

    /** How many bad segments were found. */
    public int numBadSegments;

    /** True if we checked only specific segments ({@link
     * #checkIndex(List)}) was called with non-null
     * argument). */
    public boolean partial;

    /** The greatest segment name. */
    public int maxSegmentName;

    /** Whether the SegmentInfos.counter is greater than any of the segments' names. */
    public boolean validCounter; 

    /** Holds the userData of the last commit in the index */
    public Map<String, String> userData;

    /** Holds the status of each segment in the index.
     *  See {@link #segmentInfos}.
     *
     * <p><b>WARNING</b>: this API is new and experimental and is
     * subject to suddenly change in the next release.
     */
    public static class SegmentInfoStatus {
      /** Name of the segment. */
      public String name;

      /** CodecInfo used to read this segment. */
      public SegmentCodecs codec;

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
       *  does not omitTermFreqAndPositions.
       *  @see AbstractField#setOmitTermFreqAndPositions */
      public boolean hasProx;

      /** Map that includes certain
       *  debugging details that IndexWriter records into
       *  each segment it creates */
      public Map<String,String> diagnostics;

      /** Status for testing of field norms (null if field norms could not be tested). */
      public FieldNormStatus fieldNormStatus;

      /** Status for testing of indexed terms (null if indexed terms could not be tested). */
      public TermIndexStatus termIndexStatus;

      /** Status for testing of stored fields (null if stored fields could not be tested). */
      public StoredFieldStatus storedFieldStatus;

      /** Status for testing of term vectors (null if term vectors could not be tested). */
      public TermVectorStatus termVectorStatus;
      
      /** Status for testing of DocValues (null if DocValues could not be tested). */
      public DocValuesStatus docValuesStatus;
    }

    /**
     * Status from testing field norms.
     */
    public static final class FieldNormStatus {
      /** Number of fields successfully tested */
      public long totFields = 0L;

      /** Exception thrown during term index test (null on success) */
      public Throwable error = null;
    }

    /**
     * Status from testing term index.
     */
    public static final class TermIndexStatus {
      /** Total term count */
      public long termCount = 0L;

      /** Total frequency across all terms. */
      public long totFreq = 0L;
      
      /** Total number of positions. */
      public long totPos = 0L;

      /** Exception thrown during term index test (null on success) */
      public Throwable error = null;
    }

    /**
     * Status from testing stored fields.
     */
    public static final class StoredFieldStatus {
      
      /** Number of documents tested. */
      public int docCount = 0;
      
      /** Total number of stored fields tested. */
      public long totFields = 0;
      
      /** Exception thrown during stored fields test (null on success) */
      public Throwable error = null;
    }

    /**
     * Status from testing stored fields.
     */
    public static final class TermVectorStatus {
      
      /** Number of documents tested. */
      public int docCount = 0;
      
      /** Total number of term vectors tested. */
      public long totVectors = 0;
      
      /** Exception thrown during term vector test (null on success) */
      public Throwable error = null;
    }
    
    public static final class DocValuesStatus {
      /** Number of documents tested. */
      public int docCount;
      /** Total number of docValues tested. */
      public long totalValueFields;
      /** Exception thrown during doc values test (null on success) */
      public Throwable error = null;
    }
  }

  /** Create a new CheckIndex on the directory. */
  public CheckIndex(Directory dir) {
    this.dir = dir;
    infoStream = null;
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

  public Status checkIndex(List<String> onlySegments) throws IOException {
    return checkIndex(onlySegments, CodecProvider.getDefault());
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
  public Status checkIndex(List<String> onlySegments, CodecProvider codecs) throws IOException {
    NumberFormat nf = NumberFormat.getInstance();
    SegmentInfos sis = new SegmentInfos(codecs);
    Status result = new Status();
    result.dir = dir;
    try {
      sis.read(dir, codecs);
    } catch (Throwable t) {
      msg("ERROR: could not read any segments file in directory");
      result.missingSegments = true;
      if (infoStream != null)
        t.printStackTrace(infoStream);
      return result;
    }

    // find the oldest and newest segment versions
    String oldest = Integer.toString(Integer.MAX_VALUE), newest = Integer.toString(Integer.MIN_VALUE);
    String oldSegs = null;
    boolean foundNonNullVersion = false;
    Comparator<String> versionComparator = StringHelper.getVersionComparator();
    for (SegmentInfo si : sis) {
      String version = si.getVersion();
      if (version == null) {
        // pre-3.1 segment
        oldSegs = "pre-3.1";
      } else {
        foundNonNullVersion = true;
        if (versionComparator.compare(version, oldest) < 0) {
          oldest = version;
        }
        if (versionComparator.compare(version, newest) > 0) {
          newest = version;
        }
      }
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

    if (format == DefaultSegmentInfosWriter.FORMAT_DIAGNOSTICS) {
      sFormat = "FORMAT_DIAGNOSTICS [Lucene 2.9]";
    } else if (format == DefaultSegmentInfosWriter.FORMAT_HAS_VECTORS) {
      sFormat = "FORMAT_HAS_VECTORS [Lucene 3.1]";
    } else if (format == DefaultSegmentInfosWriter.FORMAT_3_1) {
      sFormat = "FORMAT_3_1 [Lucene 3.1+]";
    } else if (format == DefaultSegmentInfosWriter.FORMAT_4_0) {
      sFormat = "FORMAT_4_0 [Lucene 4.0]";
    } else if (format == DefaultSegmentInfosWriter.FORMAT_CURRENT) {
      throw new RuntimeException("BUG: You should update this tool!");
    } else if (format < DefaultSegmentInfosWriter.FORMAT_CURRENT) {
      sFormat = "int=" + format + " [newer version of Lucene than this tool supports]";
      skip = true;
    } else if (format > DefaultSegmentInfosWriter.FORMAT_MINIMUM) {
      sFormat = "int=" + format + " [older version of Lucene than this tool supports]";
      skip = true;
    }

    result.segmentsFileName = segmentsFileName;
    result.numSegments = numSegments;
    result.segmentFormat = sFormat;
    result.userData = sis.getUserData();
    String userDataString;
    if (sis.getUserData().size() > 0) {
      userDataString = " userData=" + sis.getUserData();
    } else {
      userDataString = "";
    }

    String versionString = null;
    if (oldSegs != null) {
      if (foundNonNullVersion) {
        versionString = "versions=[" + oldSegs + " .. " + newest + "]";
      } else {
        versionString = "version=" + oldSegs;
      }
    } else {
      versionString = oldest.equals(newest) ? ( "version=" + oldest ) : ("versions=[" + oldest + " .. " + newest + "]");
    }

    msg("Segments file=" + segmentsFileName + " numSegments=" + numSegments
        + " " + versionString + " format=" + sFormat + userDataString);

    if (onlySegments != null) {
      result.partial = true;
      if (infoStream != null)
        infoStream.print("\nChecking only these segments:");
      for (String s : onlySegments) {
        if (infoStream != null)
          infoStream.print(" " + s);
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
    result.maxSegmentName = -1;

    for(int i=0;i<numSegments;i++) {
      final SegmentInfo info = sis.info(i);
      int segmentName = Integer.parseInt(info.name.substring(1), Character.MAX_RADIX);
      if (segmentName > result.maxSegmentName) {
        result.maxSegmentName = segmentName;
      }
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
        final SegmentCodecs codec = info.getSegmentCodecs();
        msg("    codec=" + codec);
        segInfoStat.codec = codec;
        msg("    compound=" + info.getUseCompoundFile());
        segInfoStat.compound = info.getUseCompoundFile();
        msg("    hasProx=" + info.getHasProx());
        segInfoStat.hasProx = info.getHasProx();
        msg("    numFiles=" + info.files().size());
        segInfoStat.numFiles = info.files().size();
        segInfoStat.sizeMB = info.sizeInBytes(true)/(1024.*1024.);
        msg("    size (MB)=" + nf.format(segInfoStat.sizeMB));
        Map<String,String> diagnostics = info.getDiagnostics();
        segInfoStat.diagnostics = diagnostics;
        if (diagnostics.size() > 0) {
          msg("    diagnostics = " + diagnostics);
        }

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
        reader = SegmentReader.get(true, info, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR);

        segInfoStat.openReaderPassed = true;

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
        if (reader.maxDoc() != info.docCount)
          throw new RuntimeException("SegmentReader.maxDoc() " + reader.maxDoc() + " != SegmentInfos.docCount " + info.docCount);

        // Test getFieldNames()
        if (infoStream != null) {
          infoStream.print("    test: fields..............");
        }         
        Collection<String> fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL);
        msg("OK [" + fieldNames.size() + " fields]");
        segInfoStat.numFields = fieldNames.size();
        
        // Test Field Norms
        segInfoStat.fieldNormStatus = testFieldNorms(fieldNames, reader);

        // Test the Term Index
        segInfoStat.termIndexStatus = testTermIndex(reader);

        // Test Stored Fields
        segInfoStat.storedFieldStatus = testStoredFields(info, reader, nf);

        // Test Term Vectors
        segInfoStat.termVectorStatus = testTermVectors(info, reader, nf);
        
        segInfoStat.docValuesStatus = testDocValues(info, reader);

        // Rethrow the first exception we encountered
        //  This will cause stats for failed segments to be incremented properly
        if (segInfoStat.fieldNormStatus.error != null) {
          throw new RuntimeException("Field Norm test failed");
        } else if (segInfoStat.termIndexStatus.error != null) {
          throw new RuntimeException("Term Index test failed");
        } else if (segInfoStat.storedFieldStatus.error != null) {
          throw new RuntimeException("Stored Field test failed");
        } else if (segInfoStat.termVectorStatus.error != null) {
          throw new RuntimeException("Term Vector test failed");
        }  else if (segInfoStat.docValuesStatus.error != null) {
          throw new RuntimeException("DocValues test failed");
        }

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
      result.newSegments.add((SegmentInfo) info.clone());
    }

    if (0 == result.numBadSegments) {
      result.clean = true;
    } else
      msg("WARNING: " + result.numBadSegments + " broken segments (containing " + result.totLoseDocCount + " documents) detected");

    if ( ! (result.validCounter = (result.maxSegmentName < sis.counter))) {
      result.clean = false;
      result.newSegments.counter = result.maxSegmentName + 1; 
      msg("ERROR: Next segment name counter " + sis.counter + " is not greater than max segment name " + result.maxSegmentName);
    }
    
    if (result.clean) {
      msg("No problems were detected with this index.\n");
    }

    return result;
  }

  /**
   * Test field norms.
   */
  private Status.FieldNormStatus testFieldNorms(Collection<String> fieldNames, SegmentReader reader) {
    final Status.FieldNormStatus status = new Status.FieldNormStatus();

    try {
      // Test Field Norms
      if (infoStream != null) {
        infoStream.print("    test: field norms.........");
      }
      byte[] b;
      for (final String fieldName : fieldNames) {
        if (reader.hasNorms(fieldName)) {
          b = reader.norms(fieldName);
          ++status.totFields;
        }
      }

      msg("OK [" + status.totFields + " fields]");
    } catch (Throwable e) {
      msg("ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }

    return status;
  }

  /**
   * Test the term index.
   */
  private Status.TermIndexStatus testTermIndex(SegmentReader reader) {
    final Status.TermIndexStatus status = new Status.TermIndexStatus();

    final int maxDoc = reader.maxDoc();
    final Bits delDocs = reader.getDeletedDocs();

    final IndexSearcher is = new IndexSearcher(reader);

    try {

      if (infoStream != null) {
        infoStream.print("    test: terms, freq, prox...");
      }

      final Fields fields = reader.fields();
      if (fields == null) {
        msg("OK [no fields/terms]");
        return status;
      }
     
      DocsEnum docs = null;
      DocsAndPositionsEnum postings = null;

      final FieldsEnum fieldsEnum = fields.iterator();
      while(true) {
        final String field = fieldsEnum.next();
        if (field == null) {
          break;
        }
        
        final TermsEnum terms = fieldsEnum.terms();
        assert terms != null;
        boolean hasOrd = true;
        final long termCountStart = status.termCount;

        BytesRef lastTerm = null;

        Comparator<BytesRef> termComp = terms.getComparator();

        long sumTotalTermFreq = 0;

        while(true) {

          final BytesRef term = terms.next();
          if (term == null) {
            break;
          }

          // make sure terms arrive in order according to
          // the comp
          if (lastTerm == null) {
            lastTerm = new BytesRef(term);
          } else {
            if (termComp.compare(lastTerm, term) >= 0) {
              throw new RuntimeException("terms out of order: lastTerm=" + lastTerm + " term=" + term);
            }
            lastTerm.copy(term);
          }

          final int docFreq = terms.docFreq();
          status.totFreq += docFreq;

          docs = terms.docs(delDocs, docs);
          postings = terms.docsAndPositions(delDocs, postings);

          if (hasOrd) {
            long ord = -1;
            try {
              ord = terms.ord();
            } catch (UnsupportedOperationException uoe) {
              hasOrd = false;
            }

            if (hasOrd) {
              final long ordExpected = status.termCount - termCountStart;
              if (ord != ordExpected) {
                throw new RuntimeException("ord mismatch: TermsEnum has ord=" + ord + " vs actual=" + ordExpected);
              }
            }
          }

          status.termCount++;

          final DocsEnum docs2;
          final boolean hasPositions;
          if (postings != null) {
            docs2 = postings;
            hasPositions = true;
          } else {
            docs2 = docs;
            hasPositions = false;
          }

          int lastDoc = -1;
          int docCount = 0;
          long totalTermFreq = 0;
          while(true) {
            final int doc = docs2.nextDoc();
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            final int freq = docs2.freq();
            status.totPos += freq;
            totalTermFreq += freq;
            docCount++;

            if (doc <= lastDoc) {
              throw new RuntimeException("term " + term + ": doc " + doc + " <= lastDoc " + lastDoc);
            }
            if (doc >= maxDoc) {
              throw new RuntimeException("term " + term + ": doc " + doc + " >= maxDoc " + maxDoc);
            }

            lastDoc = doc;
            if (freq <= 0) {
              throw new RuntimeException("term " + term + ": doc " + doc + ": freq " + freq + " is out of bounds");
            }
            
            int lastPos = -1;
            if (postings != null) {
              for(int j=0;j<freq;j++) {
                final int pos = postings.nextPosition();
                if (pos < -1) {
                  throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " is out of bounds");
                }
                if (pos < lastPos) {
                  throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " < lastPos " + lastPos);
                }
                lastPos = pos;
                if (postings.hasPayload()) {
                  postings.getPayload();
                }
              }
            }
          }
          
          final long totalTermFreq2 = terms.totalTermFreq();
          final boolean hasTotalTermFreq = postings != null && totalTermFreq2 != -1;

          // Re-count if there are deleted docs:
          if (reader.hasDeletions()) {
            final DocsEnum docsNoDel = terms.docs(null, docs);
            docCount = 0;
            totalTermFreq = 0;
            while(docsNoDel.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              docCount++;
              totalTermFreq += docsNoDel.freq();
            }
          }

          if (docCount != docFreq) {
            throw new RuntimeException("term " + term + " docFreq=" + docFreq + " != tot docs w/o deletions " + docCount);
          }
          if (hasTotalTermFreq) {
            sumTotalTermFreq += totalTermFreq;
            if (totalTermFreq != totalTermFreq2) {
              throw new RuntimeException("term " + term + " totalTermFreq=" + totalTermFreq2 + " != recomputed totalTermFreq=" + totalTermFreq);
            }
          }

          // Test skipping
          if (docFreq >= 16) {
            if (hasPositions) {
              for(int idx=0;idx<7;idx++) {
                final int skipDocID = (int) (((idx+1)*(long) maxDoc)/8);
                postings = terms.docsAndPositions(delDocs, postings);
                final int docID = postings.advance(skipDocID);
                if (docID == DocsEnum.NO_MORE_DOCS) {
                  break;
                } else {
                  if (docID < skipDocID) {
                    throw new RuntimeException("term " + term + ": advance(docID=" + skipDocID + ") returned docID=" + docID);
                  }
                  final int freq = postings.freq();
                  if (freq <= 0) {
                    throw new RuntimeException("termFreq " + freq + " is out of bounds");
                  }
                  int lastPosition = -1;
                  for(int posUpto=0;posUpto<freq;posUpto++) {
                    final int pos = postings.nextPosition();
                    if (pos < 0) {
                      throw new RuntimeException("position " + pos + " is out of bounds");
                    }
                    if (pos <= lastPosition) {
                      throw new RuntimeException("position " + pos + " is <= lastPosition " + lastPosition);
                    }
                    lastPosition = pos;
                  } 

                  final int nextDocID = postings.nextDoc();
                  if (nextDocID == DocsEnum.NO_MORE_DOCS) {
                    break;
                  }
                  if (nextDocID <= docID) {
                    throw new RuntimeException("term " + term + ": advance(docID=" + skipDocID + "), then .next() returned docID=" + nextDocID + " vs prev docID=" + docID);
                  }
                }
              }
            } else {
              for(int idx=0;idx<7;idx++) {
                final int skipDocID = (int) (((idx+1)*(long) maxDoc)/8);
                docs = terms.docs(delDocs, docs);
                final int docID = docs.advance(skipDocID);
                if (docID == DocsEnum.NO_MORE_DOCS) {
                  break;
                } else {
                  if (docID < skipDocID) {
                    throw new RuntimeException("term " + term + ": advance(docID=" + skipDocID + ") returned docID=" + docID);
                  }
                  final int nextDocID = docs.nextDoc();
                  if (nextDocID == DocsEnum.NO_MORE_DOCS) {
                    break;
                  }
                  if (nextDocID <= docID) {
                    throw new RuntimeException("term " + term + ": advance(docID=" + skipDocID + "), then .next() returned docID=" + nextDocID + " vs prev docID=" + docID);
                  }
                }
              }
            }
          }
        }

        if (sumTotalTermFreq != 0) {
          final long v = fields.terms(field).getSumTotalTermFreq();
          if (v != -1 && sumTotalTermFreq != v) {
            throw new RuntimeException("sumTotalTermFreq for field " + field + "=" + v + " != recomputed sumTotalTermFreq=" + sumTotalTermFreq);
          }
        }

        // Test seek to last term:
        if (lastTerm != null) {
          if (terms.seekCeil(lastTerm) != TermsEnum.SeekStatus.FOUND) {
            throw new RuntimeException("seek to last term " + lastTerm + " failed");
          }

          is.search(new TermQuery(new Term(field, lastTerm)), 1);
        }

        // Test seeking by ord
        if (hasOrd && status.termCount-termCountStart > 0) {
          long termCount;
          try {
            termCount = fields.terms(field).getUniqueTermCount();
          } catch (UnsupportedOperationException uoe) {
            termCount = -1;
          }

          if (termCount != -1 && termCount != status.termCount - termCountStart) {
            throw new RuntimeException("termCount mismatch " + termCount + " vs " + (status.termCount - termCountStart));
          }

          int seekCount = (int) Math.min(10000L, termCount);
          if (seekCount > 0) {
            BytesRef[] seekTerms = new BytesRef[seekCount];
            
            // Seek by ord
            for(int i=seekCount-1;i>=0;i--) {
              long ord = i*(termCount/seekCount);
              terms.seekExact(ord);
              seekTerms[i] = new BytesRef(terms.term());
            }

            // Seek by term
            long totDocCount = 0;
            for(int i=seekCount-1;i>=0;i--) {
              if (terms.seekCeil(seekTerms[i]) != TermsEnum.SeekStatus.FOUND) {
                throw new RuntimeException("seek to existing term " + seekTerms[i] + " failed");
              }
              
              docs = terms.docs(delDocs, docs);
              if (docs == null) {
                throw new RuntimeException("null DocsEnum from to existing term " + seekTerms[i]);
              }

              while(docs.nextDoc() != DocsEnum.NO_MORE_DOCS) {
                totDocCount++;
              }
            }

            // TermQuery
            long totDocCount2 = 0;
            for(int i=0;i<seekCount;i++) {
              totDocCount2 += is.search(new TermQuery(new Term(field, seekTerms[i])), 1).totalHits;
            }

            if (totDocCount != totDocCount2) {
              throw new RuntimeException("search to seek terms produced wrong number of hits: " + totDocCount + " vs " + totDocCount2);
            }
          }
        }
      }

      msg("OK [" + status.termCount + " terms; " + status.totFreq + " terms/docs pairs; " + status.totPos + " tokens]");

    } catch (Throwable e) {
      msg("ERROR: " + e);
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }

    return status;
  }
  
  /**
   * Test stored fields for a segment.
   */
  private Status.StoredFieldStatus testStoredFields(SegmentInfo info, SegmentReader reader, NumberFormat format) {
    final Status.StoredFieldStatus status = new Status.StoredFieldStatus();

    try {
      if (infoStream != null) {
        infoStream.print("    test: stored fields.......");
      }

      // Scan stored fields for all documents
      final Bits delDocs = reader.getDeletedDocs();
      for (int j = 0; j < info.docCount; ++j) {
        if (delDocs == null || !delDocs.get(j)) {
          status.docCount++;
          Document doc = reader.document(j);
          status.totFields += doc.getFields().size();
        }
      }      

      // Validate docCount
      if (status.docCount != reader.numDocs()) {
        throw new RuntimeException("docCount=" + status.docCount + " but saw " + status.docCount + " undeleted docs");
      }

      msg("OK [" + status.totFields + " total field count; avg " + 
          format.format((((float) status.totFields)/status.docCount)) + " fields per doc]");      
    } catch (Throwable e) {
      msg("ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }

    return status;
  }
  
  private Status.DocValuesStatus testDocValues(SegmentInfo info,
      SegmentReader reader) {
    final Status.DocValuesStatus status = new Status.DocValuesStatus();
    try {
      if (infoStream != null) {
        infoStream.print("    test: DocValues........");
      }
      final FieldInfos fieldInfos = info.getFieldInfos();
      for (FieldInfo fieldInfo : fieldInfos) {
        if (fieldInfo.hasDocValues()) {
          status.totalValueFields++;
          final PerDocValues perDocValues = reader.perDocValues();
          final IndexDocValues docValues = perDocValues.docValues(fieldInfo.name);
          if (docValues == null) {
            continue;
          }
          final ValuesEnum values = docValues.getEnum();
          while (values.nextDoc() != ValuesEnum.NO_MORE_DOCS) {
            switch (fieldInfo.docValues) {
            case BYTES_FIXED_DEREF:
            case BYTES_FIXED_SORTED:
            case BYTES_FIXED_STRAIGHT:
            case BYTES_VAR_DEREF:
            case BYTES_VAR_SORTED:
            case BYTES_VAR_STRAIGHT:
              values.bytes();
              break;
            case FLOAT_32:
            case FLOAT_64:
              values.getFloat();
              break;
            case INTS:
              values.getInt();
              break;
            default:
              throw new IllegalArgumentException("Field: " + fieldInfo.name
                  + " - no such DocValues type: " + fieldInfo.docValues);
            }
          }
        }
      }

      msg("OK [" + status.docCount + " total doc Count; Num DocValues Fields "
          + status.totalValueFields);
    } catch (Throwable e) {
      msg("ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }
    return status;
  }

  /**
   * Test term vectors for a segment.
   */
  private Status.TermVectorStatus testTermVectors(SegmentInfo info, SegmentReader reader, NumberFormat format) {
    final Status.TermVectorStatus status = new Status.TermVectorStatus();
    
    try {
      if (infoStream != null) {
        infoStream.print("    test: term vectors........");
      }

      final Bits delDocs = reader.getDeletedDocs();
      for (int j = 0; j < info.docCount; ++j) {
        if (delDocs == null || !delDocs.get(j)) {
          status.docCount++;
          TermFreqVector[] tfv = reader.getTermFreqVectors(j);
          if (tfv != null) {
            status.totVectors += tfv.length;
          }
        }
      }
      
      msg("OK [" + status.totVectors + " total vector count; avg " + 
          format.format((((float) status.totVectors) / status.docCount)) + " term/freq vector fields per doc]");
    } catch (Throwable e) {
      msg("ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }
    
    return status;
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
    result.newSegments.changed();
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
  public static void main(String[] args) throws IOException, InterruptedException {

    boolean doFix = false;
    List<String> onlySegments = new ArrayList<String>();
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
      dir = FSDirectory.open(new File(indexPath));
    } catch (Throwable t) {
      System.out.println("ERROR: could not open directory \"" + indexPath + "\"; exiting");
      t.printStackTrace(System.out);
      System.exit(1);
    }

    CheckIndex checker = new CheckIndex(dir);
    checker.setInfoStream(System.out);

    Status result = checker.checkIndex(onlySegments);
    if (result.missingSegments) {
      System.exit(1);
    }

    if (!result.clean) {
      if (!doFix) {
        System.out.println("WARNING: would write new segments file, and " + result.totLoseDocCount + " documents would be lost, if -fix were specified\n");
      } else {
        System.out.println("WARNING: " + result.totLoseDocCount + " documents will be lost\n");
        System.out.println("NOTE: will write new segments file in 5 seconds; this will remove " + result.totLoseDocCount + " docs from the index. THIS IS YOUR LAST CHANCE TO CTRL+C!");
        for(int s=0;s<5;s++) {
          Thread.sleep(1000);
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
    if (result.clean == true)
      exitCode = 0;
    else
      exitCode = 1;
    System.exit(exitCode);
  }
}
