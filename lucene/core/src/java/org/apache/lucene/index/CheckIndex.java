package org.apache.lucene.index;

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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat; // javadocs
import org.apache.lucene.codecs.lucene3x.Lucene3xSegmentInfoFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType; // for javadocs
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CommandLineUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.StringHelper;

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

    Status() {
    }

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
     * @lucene.experimental
     */
    public static class SegmentInfoStatus {

      SegmentInfoStatus() {
      }

      /** Name of the segment. */
      public String name;

      /** Codec used to read this segment. */
      public Codec codec;

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

      /** Current deletions generation. */
      public long deletionsGen;
    
      /** Number of deleted documents. */
      public int numDeleted;

      /** True if we were able to open an AtomicReader on this
       *  segment. */
      public boolean openReaderPassed;

      /** Number of fields in this segment. */
      int numFields;

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
      private FieldNormStatus() {
      }

      /** Number of fields successfully tested */
      public long totFields = 0L;

      /** Exception thrown during term index test (null on success) */
      public Throwable error = null;
    }

    /**
     * Status from testing term index.
     */
    public static final class TermIndexStatus {

      TermIndexStatus() {
      }

      /** Number of terms with at least one live doc. */
      public long termCount = 0L;

      /** Number of terms with zero live docs docs. */
      public long delTermCount = 0L;

      /** Total frequency across all terms. */
      public long totFreq = 0L;
      
      /** Total number of positions. */
      public long totPos = 0L;

      /** Exception thrown during term index test (null on success) */
      public Throwable error = null;

      /** Holds details of block allocations in the block
       *  tree terms dictionary (this is only set if the
       *  {@link PostingsFormat} for this segment uses block
       *  tree. */
      public Map<String,BlockTreeTermsReader.Stats> blockTreeStats = null;
    }

    /**
     * Status from testing stored fields.
     */
    public static final class StoredFieldStatus {

      StoredFieldStatus() {
      }
      
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
      
      TermVectorStatus() {
      }

      /** Number of documents tested. */
      public int docCount = 0;
      
      /** Total number of term vectors tested. */
      public long totVectors = 0;
      
      /** Exception thrown during term vector test (null on success) */
      public Throwable error = null;
    }
    
    /**
     * Status from testing DocValues
     */
    public static final class DocValuesStatus {

      DocValuesStatus() {
      }

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

  private boolean crossCheckTermVectors;

  /** If true, term vectors are compared against postings to
   *  make sure they are the same.  This will likely
   *  drastically increase time it takes to run CheckIndex! */
  public void setCrossCheckTermVectors(boolean v) {
    crossCheckTermVectors = v;
  }

  /** See {@link #setCrossCheckTermVectors}. */
  public boolean getCrossCheckTermVectors() {
    return crossCheckTermVectors;
  }

  private boolean verbose;

  /** Set infoStream where messages should go.  If null, no
   *  messages are printed.  If verbose is true then more
   *  details are printed. */
  public void setInfoStream(PrintStream out, boolean verbose) {
    infoStream = out;
    this.verbose = verbose;
  }

  /** Set infoStream where messages should go. See {@link #setInfoStream(PrintStream,boolean)}. */
  public void setInfoStream(PrintStream out) {
    setInfoStream(out, false);
  }

  private static void msg(PrintStream out, String msg) {
    if (out != null)
      out.println(msg);
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
  public Status checkIndex(List<String> onlySegments) throws IOException {
    NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);
    SegmentInfos sis = new SegmentInfos();
    Status result = new Status();
    result.dir = dir;
    try {
      sis.read(dir);
    } catch (Throwable t) {
      msg(infoStream, "ERROR: could not read any segments file in directory");
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
    for (SegmentInfoPerCommit si : sis) {
      String version = si.info.getVersion();
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
    final String segmentsFileName = sis.getSegmentsFileName();
    // note: we only read the format byte (required preamble) here!
    IndexInput input = null;
    try {
      input = dir.openInput(segmentsFileName, IOContext.DEFAULT);
    } catch (Throwable t) {
      msg(infoStream, "ERROR: could not open segments file in directory");
      if (infoStream != null)
        t.printStackTrace(infoStream);
      result.cantOpenSegments = true;
      return result;
    }
    int format = 0;
    try {
      format = input.readInt();
    } catch (Throwable t) {
      msg(infoStream, "ERROR: could not read segment file version in directory");
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

    result.segmentsFileName = segmentsFileName;
    result.numSegments = numSegments;
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

    msg(infoStream, "Segments file=" + segmentsFileName + " numSegments=" + numSegments
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
      msg(infoStream, ":");
    }

    if (skip) {
      msg(infoStream, "\nERROR: this index appears to be created by a newer version of Lucene than this tool was compiled on; please re-compile this tool on the matching version of Lucene; exiting");
      result.toolOutOfDate = true;
      return result;
    }


    result.newSegments = sis.clone();
    result.newSegments.clear();
    result.maxSegmentName = -1;

    for(int i=0;i<numSegments;i++) {
      final SegmentInfoPerCommit info = sis.info(i);
      int segmentName = Integer.parseInt(info.info.name.substring(1), Character.MAX_RADIX);
      if (segmentName > result.maxSegmentName) {
        result.maxSegmentName = segmentName;
      }
      if (onlySegments != null && !onlySegments.contains(info.info.name)) {
        continue;
      }
      Status.SegmentInfoStatus segInfoStat = new Status.SegmentInfoStatus();
      result.segmentInfos.add(segInfoStat);
      msg(infoStream, "  " + (1+i) + " of " + numSegments + ": name=" + info.info.name + " docCount=" + info.info.getDocCount());
      segInfoStat.name = info.info.name;
      segInfoStat.docCount = info.info.getDocCount();

      int toLoseDocCount = info.info.getDocCount();

      AtomicReader reader = null;

      try {
        final Codec codec = info.info.getCodec();
        msg(infoStream, "    codec=" + codec);
        segInfoStat.codec = codec;
        msg(infoStream, "    compound=" + info.info.getUseCompoundFile());
        segInfoStat.compound = info.info.getUseCompoundFile();
        msg(infoStream, "    numFiles=" + info.files().size());
        segInfoStat.numFiles = info.files().size();
        segInfoStat.sizeMB = info.sizeInBytes()/(1024.*1024.);
        if (info.info.getAttribute(Lucene3xSegmentInfoFormat.DS_OFFSET_KEY) == null) {
          // don't print size in bytes if its a 3.0 segment with shared docstores
          msg(infoStream, "    size (MB)=" + nf.format(segInfoStat.sizeMB));
        }
        Map<String,String> diagnostics = info.info.getDiagnostics();
        segInfoStat.diagnostics = diagnostics;
        if (diagnostics.size() > 0) {
          msg(infoStream, "    diagnostics = " + diagnostics);
        }

        Map<String,String> atts = info.info.attributes();
        if (atts != null && !atts.isEmpty()) {
          msg(infoStream, "    attributes = " + atts);
        }

        if (!info.hasDeletions()) {
          msg(infoStream, "    no deletions");
          segInfoStat.hasDeletions = false;
        }
        else{
          msg(infoStream, "    has deletions [delGen=" + info.getDelGen() + "]");
          segInfoStat.hasDeletions = true;
          segInfoStat.deletionsGen = info.getDelGen();
        }
        if (infoStream != null)
          infoStream.print("    test: open reader.........");
        reader = new SegmentReader(info, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, IOContext.DEFAULT);

        segInfoStat.openReaderPassed = true;

        final int numDocs = reader.numDocs();
        toLoseDocCount = numDocs;
        if (reader.hasDeletions()) {
          if (reader.numDocs() != info.info.getDocCount() - info.getDelCount()) {
            throw new RuntimeException("delete count mismatch: info=" + (info.info.getDocCount() - info.getDelCount()) + " vs reader=" + reader.numDocs());
          }
          if ((info.info.getDocCount()-reader.numDocs()) > reader.maxDoc()) {
            throw new RuntimeException("too many deleted docs: maxDoc()=" + reader.maxDoc() + " vs del count=" + (info.info.getDocCount()-reader.numDocs()));
          }
          if (info.info.getDocCount() - numDocs != info.getDelCount()) {
            throw new RuntimeException("delete count mismatch: info=" + info.getDelCount() + " vs reader=" + (info.info.getDocCount() - numDocs));
          }
          Bits liveDocs = reader.getLiveDocs();
          if (liveDocs == null) {
            throw new RuntimeException("segment should have deletions, but liveDocs is null");
          } else {
            int numLive = 0;
            for (int j = 0; j < liveDocs.length(); j++) {
              if (liveDocs.get(j)) {
                numLive++;
              }
            }
            if (numLive != numDocs) {
              throw new RuntimeException("liveDocs count mismatch: info=" + numDocs + ", vs bits=" + numLive);
            }
          }
          
          segInfoStat.numDeleted = info.info.getDocCount() - numDocs;
          msg(infoStream, "OK [" + (segInfoStat.numDeleted) + " deleted docs]");
        } else {
          if (info.getDelCount() != 0) {
            throw new RuntimeException("delete count mismatch: info=" + info.getDelCount() + " vs reader=" + (info.info.getDocCount() - numDocs));
          }
          Bits liveDocs = reader.getLiveDocs();
          if (liveDocs != null) {
            // its ok for it to be non-null here, as long as none are set right?
            for (int j = 0; j < liveDocs.length(); j++) {
              if (!liveDocs.get(j)) {
                throw new RuntimeException("liveDocs mismatch: info says no deletions but doc " + j + " is deleted.");
              }
            }
          }
          msg(infoStream, "OK");
        }
        if (reader.maxDoc() != info.info.getDocCount()) {
          throw new RuntimeException("SegmentReader.maxDoc() " + reader.maxDoc() + " != SegmentInfos.docCount " + info.info.getDocCount());
        }

        // Test getFieldInfos()
        if (infoStream != null) {
          infoStream.print("    test: fields..............");
        }         
        FieldInfos fieldInfos = reader.getFieldInfos();
        msg(infoStream, "OK [" + fieldInfos.size() + " fields]");
        segInfoStat.numFields = fieldInfos.size();
        
        // Test Field Norms
        segInfoStat.fieldNormStatus = testFieldNorms(reader, infoStream);

        // Test the Term Index
        segInfoStat.termIndexStatus = testPostings(reader, infoStream, verbose);

        // Test Stored Fields
        segInfoStat.storedFieldStatus = testStoredFields(reader, infoStream);

        // Test Term Vectors
        segInfoStat.termVectorStatus = testTermVectors(reader, infoStream, verbose, crossCheckTermVectors);

        segInfoStat.docValuesStatus = testDocValues(reader, infoStream);

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

        msg(infoStream, "");

      } catch (Throwable t) {
        msg(infoStream, "FAILED");
        String comment;
        comment = "fixIndex() would remove reference to this segment";
        msg(infoStream, "    WARNING: " + comment + "; full exception:");
        if (infoStream != null)
          t.printStackTrace(infoStream);
        msg(infoStream, "");
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
    } else
      msg(infoStream, "WARNING: " + result.numBadSegments + " broken segments (containing " + result.totLoseDocCount + " documents) detected");

    if ( ! (result.validCounter = (result.maxSegmentName < sis.counter))) {
      result.clean = false;
      result.newSegments.counter = result.maxSegmentName + 1; 
      msg(infoStream, "ERROR: Next segment name counter " + sis.counter + " is not greater than max segment name " + result.maxSegmentName);
    }
    
    if (result.clean) {
      msg(infoStream, "No problems were detected with this index.\n");
    }

    return result;
  }

  /**
   * Test field norms.
   * @lucene.experimental
   */
  public static Status.FieldNormStatus testFieldNorms(AtomicReader reader, PrintStream infoStream) {
    final Status.FieldNormStatus status = new Status.FieldNormStatus();

    try {
      // Test Field Norms
      if (infoStream != null) {
        infoStream.print("    test: field norms.........");
      }
      for (FieldInfo info : reader.getFieldInfos()) {
        if (info.hasNorms()) {
          assert reader.hasNorms(info.name); // deprecated path
          checkNorms(info, reader, infoStream);
          ++status.totFields;
        } else {
          assert !reader.hasNorms(info.name); // deprecated path
          if (reader.getNormValues(info.name) != null) {
            throw new RuntimeException("field: " + info.name + " should omit norms but has them!");
          }
        }
      }

      msg(infoStream, "OK [" + status.totFields + " fields]");
    } catch (Throwable e) {
      msg(infoStream, "ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }

    return status;
  }

  /**
   * checks Fields api is consistent with itself.
   * searcher is optional, to verify with queries. Can be null.
   */
  private static Status.TermIndexStatus checkFields(Fields fields, Bits liveDocs, int maxDoc, FieldInfos fieldInfos, boolean doPrint, boolean isVectors, PrintStream infoStream, boolean verbose) throws IOException {
    // TODO: we should probably return our own stats thing...?!
    
    final Status.TermIndexStatus status = new Status.TermIndexStatus();
    int computedFieldCount = 0;
    
    if (fields == null) {
      msg(infoStream, "OK [no fields/terms]");
      return status;
    }
    
    DocsEnum docs = null;
    DocsEnum docsAndFreqs = null;
    DocsAndPositionsEnum postings = null;
    
    String lastField = null;
    for (String field : fields) {
      // MultiFieldsEnum relies upon this order...
      if (lastField != null && field.compareTo(lastField) <= 0) {
        throw new RuntimeException("fields out of order: lastField=" + lastField + " field=" + field);
      }
      lastField = field;
      
      // check that the field is in fieldinfos, and is indexed.
      // TODO: add a separate test to check this for different reader impls
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      if (fieldInfo == null) {
        throw new RuntimeException("fieldsEnum inconsistent with fieldInfos, no fieldInfos for: " + field);
      }
      if (!fieldInfo.isIndexed()) {
        throw new RuntimeException("fieldsEnum inconsistent with fieldInfos, isIndexed == false for: " + field);
      }
      
      // TODO: really the codec should not return a field
      // from FieldsEnum if it has no Terms... but we do
      // this today:
      // assert fields.terms(field) != null;
      computedFieldCount++;
      
      final Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      }
      
      final boolean hasPositions = terms.hasPositions();
      final boolean hasOffsets = terms.hasOffsets();
      // term vectors cannot omit TF
      final boolean hasFreqs = isVectors || fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;

      final TermsEnum termsEnum = terms.iterator(null);
      
      boolean hasOrd = true;
      final long termCountStart = status.delTermCount + status.termCount;
      
      BytesRef lastTerm = null;
      
      Comparator<BytesRef> termComp = terms.getComparator();
      
      long sumTotalTermFreq = 0;
      long sumDocFreq = 0;
      FixedBitSet visitedDocs = new FixedBitSet(maxDoc);
      while(true) {
        
        final BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }
        
        assert term.isValid();
        
        // make sure terms arrive in order according to
        // the comp
        if (lastTerm == null) {
          lastTerm = BytesRef.deepCopyOf(term);
        } else {
          if (termComp.compare(lastTerm, term) >= 0) {
            throw new RuntimeException("terms out of order: lastTerm=" + lastTerm + " term=" + term);
          }
          lastTerm.copyBytes(term);
        }
        
        final int docFreq = termsEnum.docFreq();
        if (docFreq <= 0) {
          throw new RuntimeException("docfreq: " + docFreq + " is out of bounds");
        }
        sumDocFreq += docFreq;
        
        docs = termsEnum.docs(liveDocs, docs);
        postings = termsEnum.docsAndPositions(liveDocs, postings);
        
        if (hasOrd) {
          long ord = -1;
          try {
            ord = termsEnum.ord();
          } catch (UnsupportedOperationException uoe) {
            hasOrd = false;
          }
          
          if (hasOrd) {
            final long ordExpected = status.delTermCount + status.termCount - termCountStart;
            if (ord != ordExpected) {
              throw new RuntimeException("ord mismatch: TermsEnum has ord=" + ord + " vs actual=" + ordExpected);
            }
          }
        }
        
        final DocsEnum docs2;
        if (postings != null) {
          docs2 = postings;
        } else {
          docs2 = docs;
        }
        
        int lastDoc = -1;
        int docCount = 0;
        long totalTermFreq = 0;
        while(true) {
          final int doc = docs2.nextDoc();
          if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          status.totFreq++;
          visitedDocs.set(doc);
          int freq = -1;
          if (hasFreqs) {
            freq = docs2.freq();
            if (freq <= 0) {
              throw new RuntimeException("term " + term + ": doc " + doc + ": freq " + freq + " is out of bounds");
            }
            status.totPos += freq;
            totalTermFreq += freq;
          }
          docCount++;
          
          if (doc <= lastDoc) {
            throw new RuntimeException("term " + term + ": doc " + doc + " <= lastDoc " + lastDoc);
          }
          if (doc >= maxDoc) {
            throw new RuntimeException("term " + term + ": doc " + doc + " >= maxDoc " + maxDoc);
          }
          
          lastDoc = doc;
          
          int lastPos = -1;
          int lastOffset = 0;
          if (hasPositions) {
            for(int j=0;j<freq;j++) {
              final int pos = postings.nextPosition();

              if (pos < 0) {
                throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " is out of bounds");
              }
              if (pos < lastPos) {
                throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " < lastPos " + lastPos);
              }
              lastPos = pos;
              BytesRef payload = postings.getPayload();
              if (payload != null) {
                assert payload.isValid();
              }
              if (payload != null && payload.length < 1) {
                throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + " payload length is out of bounds " + payload.length);
              }
              if (hasOffsets) {
                int startOffset = postings.startOffset();
                int endOffset = postings.endOffset();
                // NOTE: we cannot enforce any bounds whatsoever on vectors... they were a free-for-all before?
                // but for offsets in the postings lists these checks are fine: they were always enforced by IndexWriter
                if (!isVectors) {
                  if (startOffset < 0) {
                    throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + ": startOffset " + startOffset + " is out of bounds");
                  }
                  if (startOffset < lastOffset) {
                    throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + ": startOffset " + startOffset + " < lastStartOffset " + lastOffset);
                  }
                  if (endOffset < 0) {
                    throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + ": endOffset " + endOffset + " is out of bounds");
                  }
                  if (endOffset < startOffset) {
                    throw new RuntimeException("term " + term + ": doc " + doc + ": pos " + pos + ": endOffset " + endOffset + " < startOffset " + startOffset);
                  }
                }
                lastOffset = startOffset;
              }
            }
          }
        }
        
        if (docCount != 0) {
          status.termCount++;
        } else {
          status.delTermCount++;
        }
        
        final long totalTermFreq2 = termsEnum.totalTermFreq();
        final boolean hasTotalTermFreq = hasFreqs && totalTermFreq2 != -1;
        
        // Re-count if there are deleted docs:
        if (liveDocs != null) {
          if (hasFreqs) {
            final DocsEnum docsNoDel = termsEnum.docs(null, docsAndFreqs);
            docCount = 0;
            totalTermFreq = 0;
            while(docsNoDel.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              visitedDocs.set(docsNoDel.docID());
              docCount++;
              totalTermFreq += docsNoDel.freq();
            }
          } else {
            final DocsEnum docsNoDel = termsEnum.docs(null, docs, DocsEnum.FLAG_NONE);
            docCount = 0;
            totalTermFreq = -1;
            while(docsNoDel.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              visitedDocs.set(docsNoDel.docID());
              docCount++;
            }
          }
        }
        
        if (docCount != docFreq) {
          throw new RuntimeException("term " + term + " docFreq=" + docFreq + " != tot docs w/o deletions " + docCount);
        }
        if (hasTotalTermFreq) {
          if (totalTermFreq2 <= 0) {
            throw new RuntimeException("totalTermFreq: " + totalTermFreq2 + " is out of bounds");
          }
          sumTotalTermFreq += totalTermFreq;
          if (totalTermFreq != totalTermFreq2) {
            throw new RuntimeException("term " + term + " totalTermFreq=" + totalTermFreq2 + " != recomputed totalTermFreq=" + totalTermFreq);
          }
        }
        
        // Test skipping
        if (hasPositions) {
          for(int idx=0;idx<7;idx++) {
            final int skipDocID = (int) (((idx+1)*(long) maxDoc)/8);
            postings = termsEnum.docsAndPositions(liveDocs, postings);
            final int docID = postings.advance(skipDocID);
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
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
              int lastOffset = 0;
              for(int posUpto=0;posUpto<freq;posUpto++) {
                final int pos = postings.nextPosition();

                if (pos < 0) {
                  throw new RuntimeException("position " + pos + " is out of bounds");
                }
                if (pos < lastPosition) {
                  throw new RuntimeException("position " + pos + " is < lastPosition " + lastPosition);
                }
                lastPosition = pos;
                if (hasOffsets) {
                  int startOffset = postings.startOffset();
                  int endOffset = postings.endOffset();
                  // NOTE: we cannot enforce any bounds whatsoever on vectors... they were a free-for-all before?
                  // but for offsets in the postings lists these checks are fine: they were always enforced by IndexWriter
                  if (!isVectors) {
                    if (startOffset < 0) {
                      throw new RuntimeException("term " + term + ": doc " + docID + ": pos " + pos + ": startOffset " + startOffset + " is out of bounds");
                    }
                    if (startOffset < lastOffset) {
                      throw new RuntimeException("term " + term + ": doc " + docID + ": pos " + pos + ": startOffset " + startOffset + " < lastStartOffset " + lastOffset);
                    }
                    if (endOffset < 0) {
                      throw new RuntimeException("term " + term + ": doc " + docID + ": pos " + pos + ": endOffset " + endOffset + " is out of bounds");
                    }
                    if (endOffset < startOffset) {
                      throw new RuntimeException("term " + term + ": doc " + docID + ": pos " + pos + ": endOffset " + endOffset + " < startOffset " + startOffset);
                    }
                  }
                  lastOffset = startOffset;
                }
              } 
              
              final int nextDocID = postings.nextDoc();
              if (nextDocID == DocIdSetIterator.NO_MORE_DOCS) {
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
            docs = termsEnum.docs(liveDocs, docs, DocsEnum.FLAG_NONE);
            final int docID = docs.advance(skipDocID);
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            } else {
              if (docID < skipDocID) {
                throw new RuntimeException("term " + term + ": advance(docID=" + skipDocID + ") returned docID=" + docID);
              }
              final int nextDocID = docs.nextDoc();
              if (nextDocID == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              if (nextDocID <= docID) {
                throw new RuntimeException("term " + term + ": advance(docID=" + skipDocID + "), then .next() returned docID=" + nextDocID + " vs prev docID=" + docID);
              }
            }
          }
        }
      }
      
      final Terms fieldTerms = fields.terms(field);
      if (fieldTerms == null) {
        // Unusual: the FieldsEnum returned a field but
        // the Terms for that field is null; this should
        // only happen if it's a ghost field (field with
        // no terms, eg there used to be terms but all
        // docs got deleted and then merged away):
        
      } else {
        if (fieldTerms instanceof BlockTreeTermsReader.FieldReader) {
          final BlockTreeTermsReader.Stats stats = ((BlockTreeTermsReader.FieldReader) fieldTerms).computeStats();
          assert stats != null;
          if (status.blockTreeStats == null) {
            status.blockTreeStats = new HashMap<String,BlockTreeTermsReader.Stats>();
          }
          status.blockTreeStats.put(field, stats);
        }
        
        if (sumTotalTermFreq != 0) {
          final long v = fields.terms(field).getSumTotalTermFreq();
          if (v != -1 && sumTotalTermFreq != v) {
            throw new RuntimeException("sumTotalTermFreq for field " + field + "=" + v + " != recomputed sumTotalTermFreq=" + sumTotalTermFreq);
          }
        }
        
        if (sumDocFreq != 0) {
          final long v = fields.terms(field).getSumDocFreq();
          if (v != -1 && sumDocFreq != v) {
            throw new RuntimeException("sumDocFreq for field " + field + "=" + v + " != recomputed sumDocFreq=" + sumDocFreq);
          }
        }
        
        if (fieldTerms != null) {
          final int v = fieldTerms.getDocCount();
          if (v != -1 && visitedDocs.cardinality() != v) {
            throw new RuntimeException("docCount for field " + field + "=" + v + " != recomputed docCount=" + visitedDocs.cardinality());
          }
        }
        
        // Test seek to last term:
        if (lastTerm != null) {
          if (termsEnum.seekCeil(lastTerm) != TermsEnum.SeekStatus.FOUND) { 
            throw new RuntimeException("seek to last term " + lastTerm + " failed");
          }
          
          int expectedDocFreq = termsEnum.docFreq();
          DocsEnum d = termsEnum.docs(null, null, DocsEnum.FLAG_NONE);
          int docFreq = 0;
          while (d.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            docFreq++;
          }
          if (docFreq != expectedDocFreq) {
            throw new RuntimeException("docFreq for last term " + lastTerm + "=" + expectedDocFreq + " != recomputed docFreq=" + docFreq);
          }
        }
        
        // check unique term count
        long termCount = -1;
        
        if ((status.delTermCount+status.termCount)-termCountStart > 0) {
          termCount = fields.terms(field).size();
          
          if (termCount != -1 && termCount != status.delTermCount + status.termCount - termCountStart) {
            throw new RuntimeException("termCount mismatch " + (status.delTermCount + termCount) + " vs " + (status.termCount - termCountStart));
          }
        }
        
        // Test seeking by ord
        if (hasOrd && status.termCount-termCountStart > 0) {
          int seekCount = (int) Math.min(10000L, termCount);
          if (seekCount > 0) {
            BytesRef[] seekTerms = new BytesRef[seekCount];
            
            // Seek by ord
            for(int i=seekCount-1;i>=0;i--) {
              long ord = i*(termCount/seekCount);
              termsEnum.seekExact(ord);
              seekTerms[i] = BytesRef.deepCopyOf(termsEnum.term());
            }
            
            // Seek by term
            long totDocCount = 0;
            for(int i=seekCount-1;i>=0;i--) {
              if (termsEnum.seekCeil(seekTerms[i]) != TermsEnum.SeekStatus.FOUND) {
                throw new RuntimeException("seek to existing term " + seekTerms[i] + " failed");
              }
              
              docs = termsEnum.docs(liveDocs, docs, DocsEnum.FLAG_NONE);
              if (docs == null) {
                throw new RuntimeException("null DocsEnum from to existing term " + seekTerms[i]);
              }
              
              while(docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                totDocCount++;
              }
            }
            
            long totDocCountNoDeletes = 0;
            long totDocFreq = 0;
            for(int i=0;i<seekCount;i++) {
              if (!termsEnum.seekExact(seekTerms[i], true)) {
                throw new RuntimeException("seek to existing term " + seekTerms[i] + " failed");
              }
              
              totDocFreq += termsEnum.docFreq();
              docs = termsEnum.docs(null, docs, DocsEnum.FLAG_NONE);
              if (docs == null) {
                throw new RuntimeException("null DocsEnum from to existing term " + seekTerms[i]);
              }
              
              while(docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                totDocCountNoDeletes++;
              }
            }
            
            if (totDocCount > totDocCountNoDeletes) {
              throw new RuntimeException("more postings with deletes=" + totDocCount + " than without=" + totDocCountNoDeletes);
            }
            
            if (totDocCountNoDeletes != totDocFreq) {
              throw new RuntimeException("docfreqs=" + totDocFreq + " != recomputed docfreqs=" + totDocCountNoDeletes);
            }
          }
        }
      }
    }
    
    int fieldCount = fields.size();
    
    if (fieldCount != -1) {
      if (fieldCount < 0) {
        throw new RuntimeException("invalid fieldCount: " + fieldCount);
      }
      if (fieldCount != computedFieldCount) {
        throw new RuntimeException("fieldCount mismatch " + fieldCount + " vs recomputed field count " + computedFieldCount);
      }
    }
    
    // for most implementations, this is boring (just the sum across all fields)
    // but codecs that don't work per-field like preflex actually implement this,
    // but don't implement it on Terms, so the check isn't redundant.
    long uniqueTermCountAllFields = fields.getUniqueTermCount();
    
    if (uniqueTermCountAllFields != -1 && status.termCount + status.delTermCount != uniqueTermCountAllFields) {
      throw new RuntimeException("termCount mismatch " + uniqueTermCountAllFields + " vs " + (status.termCount + status.delTermCount));
    }

    if (doPrint) {
      msg(infoStream, "OK [" + status.termCount + " terms; " + status.totFreq + " terms/docs pairs; " + status.totPos + " tokens]");
    }
    
    if (verbose && status.blockTreeStats != null && infoStream != null && status.termCount > 0) {
      for(Map.Entry<String,BlockTreeTermsReader.Stats> ent : status.blockTreeStats.entrySet()) {
        infoStream.println("      field \"" + ent.getKey() + "\":");
        infoStream.println("      " + ent.getValue().toString().replace("\n", "\n      "));
      }
    }
    
    return status;
  }

  /**
   * Test the term index.
   * @lucene.experimental
   */
  public static Status.TermIndexStatus testPostings(AtomicReader reader, PrintStream infoStream) {
    return testPostings(reader, infoStream, false);
  }
  
  /**
   * Test the term index.
   * @lucene.experimental
   */
  public static Status.TermIndexStatus testPostings(AtomicReader reader, PrintStream infoStream, boolean verbose) {

    // TODO: we should go and verify term vectors match, if
    // crossCheckTermVectors is on...

    Status.TermIndexStatus status;
    final int maxDoc = reader.maxDoc();
    final Bits liveDocs = reader.getLiveDocs();

    try {
      if (infoStream != null) {
        infoStream.print("    test: terms, freq, prox...");
      }

      final Fields fields = reader.fields();
      final FieldInfos fieldInfos = reader.getFieldInfos();
      status = checkFields(fields, liveDocs, maxDoc, fieldInfos, true, false, infoStream, verbose);
      if (liveDocs != null) {
        if (infoStream != null) {
          infoStream.print("    test (ignoring deletes): terms, freq, prox...");
        }
        checkFields(fields, null, maxDoc, fieldInfos, true, false, infoStream, verbose);
      }
    } catch (Throwable e) {
      msg(infoStream, "ERROR: " + e);
      status = new Status.TermIndexStatus();
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }

    return status;
  }
  
  /**
   * Test stored fields.
   * @lucene.experimental
   */
  public static Status.StoredFieldStatus testStoredFields(AtomicReader reader, PrintStream infoStream) {
    final Status.StoredFieldStatus status = new Status.StoredFieldStatus();

    try {
      if (infoStream != null) {
        infoStream.print("    test: stored fields.......");
      }

      // Scan stored fields for all documents
      final Bits liveDocs = reader.getLiveDocs();
      for (int j = 0; j < reader.maxDoc(); ++j) {
        // Intentionally pull even deleted documents to
        // make sure they too are not corrupt:
        Document doc = reader.document(j);
        if (liveDocs == null || liveDocs.get(j)) {
          status.docCount++;
          status.totFields += doc.getFields().size();
        }
      }      

      // Validate docCount
      if (status.docCount != reader.numDocs()) {
        throw new RuntimeException("docCount=" + status.docCount + " but saw " + status.docCount + " undeleted docs");
      }

      msg(infoStream, "OK [" + status.totFields + " total field count; avg " + 
          NumberFormat.getInstance(Locale.ROOT).format((((float) status.totFields)/status.docCount)) + " fields per doc]");      
    } catch (Throwable e) {
      msg(infoStream, "ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }

    return status;
  }
  
  /**
   * Test docvalues.
   * @lucene.experimental
   */
  public static Status.DocValuesStatus testDocValues(AtomicReader reader,
                                                     PrintStream infoStream) {
    final Status.DocValuesStatus status = new Status.DocValuesStatus();
    try {
      if (infoStream != null) {
        infoStream.print("    test: docvalues...........");
      }
      for (FieldInfo fieldInfo : reader.getFieldInfos()) {
        if (fieldInfo.hasDocValues()) {
          status.totalValueFields++;
          checkDocValues(fieldInfo, reader, infoStream);
        } else {
          if (reader.getBinaryDocValues(fieldInfo.name) != null ||
              reader.getNumericDocValues(fieldInfo.name) != null ||
              reader.getSortedDocValues(fieldInfo.name) != null || 
              reader.getSortedSetDocValues(fieldInfo.name) != null) {
            throw new RuntimeException("field: " + fieldInfo.name + " has docvalues but should omit them!");
          }
        }
      }

      msg(infoStream, "OK [" + status.docCount + " total doc count; " + status.totalValueFields + " docvalues fields]");
    } catch (Throwable e) {
      msg(infoStream, "ERROR [" + String.valueOf(e.getMessage()) + "]");
      status.error = e;
      if (infoStream != null) {
        e.printStackTrace(infoStream);
      }
    }
    return status;
  }
  
  private static void checkBinaryDocValues(String fieldName, AtomicReader reader, BinaryDocValues dv) {
    BytesRef scratch = new BytesRef();
    for (int i = 0; i < reader.maxDoc(); i++) {
      dv.get(i, scratch);
      assert scratch.isValid();
    }
  }
  
  private static void checkSortedDocValues(String fieldName, AtomicReader reader, SortedDocValues dv) {
    checkBinaryDocValues(fieldName, reader, dv);
    final int maxOrd = dv.getValueCount()-1;
    FixedBitSet seenOrds = new FixedBitSet(dv.getValueCount());
    int maxOrd2 = -1;
    for (int i = 0; i < reader.maxDoc(); i++) {
      int ord = dv.getOrd(i);
      if (ord < 0 || ord > maxOrd) {
        throw new RuntimeException("ord out of bounds: " + ord);
      }
      maxOrd2 = Math.max(maxOrd2, ord);
      seenOrds.set(ord);
    }
    if (maxOrd != maxOrd2) {
      throw new RuntimeException("dv for field: " + fieldName + " reports wrong maxOrd=" + maxOrd + " but this is not the case: " + maxOrd2);
    }
    if (seenOrds.cardinality() != dv.getValueCount()) {
      throw new RuntimeException("dv for field: " + fieldName + " has holes in its ords, valueCount=" + dv.getValueCount() + " but only used: " + seenOrds.cardinality());
    }
    BytesRef lastValue = null;
    BytesRef scratch = new BytesRef();
    for (int i = 0; i <= maxOrd; i++) {
      dv.lookupOrd(i, scratch);
      assert scratch.isValid();
      if (lastValue != null) {
        if (scratch.compareTo(lastValue) <= 0) {
          throw new RuntimeException("dv for field: " + fieldName + " has ords out of order: " + lastValue + " >=" + scratch);
        }
      }
      lastValue = BytesRef.deepCopyOf(scratch);
    }
  }
  
  private static void checkSortedSetDocValues(String fieldName, AtomicReader reader, SortedSetDocValues dv) {
    final long maxOrd = dv.getValueCount()-1;
    OpenBitSet seenOrds = new OpenBitSet(dv.getValueCount());
    long maxOrd2 = -1;
    for (int i = 0; i < reader.maxDoc(); i++) {
      dv.setDocument(i);
      long lastOrd = -1;
      long ord;
      while ((ord = dv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        if (ord <= lastOrd) {
          throw new RuntimeException("ords out of order: " + ord + " <= " + lastOrd + " for doc: " + i);
        }
        if (ord < 0 || ord > maxOrd) {
          throw new RuntimeException("ord out of bounds: " + ord);
        }
        lastOrd = ord;
        maxOrd2 = Math.max(maxOrd2, ord);
        seenOrds.set(ord);
      }
    }
    if (maxOrd != maxOrd2) {
      throw new RuntimeException("dv for field: " + fieldName + " reports wrong maxOrd=" + maxOrd + " but this is not the case: " + maxOrd2);
    }
    if (seenOrds.cardinality() != dv.getValueCount()) {
      throw new RuntimeException("dv for field: " + fieldName + " has holes in its ords, valueCount=" + dv.getValueCount() + " but only used: " + seenOrds.cardinality());
    }
    
    BytesRef lastValue = null;
    BytesRef scratch = new BytesRef();
    for (long i = 0; i <= maxOrd; i++) {
      dv.lookupOrd(i, scratch);
      assert scratch.isValid();
      if (lastValue != null) {
        if (scratch.compareTo(lastValue) <= 0) {
          throw new RuntimeException("dv for field: " + fieldName + " has ords out of order: " + lastValue + " >=" + scratch);
        }
      }
      lastValue = BytesRef.deepCopyOf(scratch);
    }
  }

  private static void checkNumericDocValues(String fieldName, AtomicReader reader, NumericDocValues ndv) {
    for (int i = 0; i < reader.maxDoc(); i++) {
      ndv.get(i);
    }
  }
  
  private static void checkDocValues(FieldInfo fi, AtomicReader reader, PrintStream infoStream) throws Exception {
    switch(fi.getDocValuesType()) {
      case SORTED:
        checkSortedDocValues(fi.name, reader, reader.getSortedDocValues(fi.name));
        if (reader.getBinaryDocValues(fi.name) != null ||
            reader.getNumericDocValues(fi.name) != null ||
            reader.getSortedSetDocValues(fi.name) != null) {
          throw new RuntimeException(fi.name + " returns multiple docvalues types!");
        }
        break;
      case SORTED_SET:
        checkSortedSetDocValues(fi.name, reader, reader.getSortedSetDocValues(fi.name));
        if (reader.getBinaryDocValues(fi.name) != null ||
            reader.getNumericDocValues(fi.name) != null ||
            reader.getSortedDocValues(fi.name) != null) {
          throw new RuntimeException(fi.name + " returns multiple docvalues types!");
        }
        break;
      case BINARY:
        checkBinaryDocValues(fi.name, reader, reader.getBinaryDocValues(fi.name));
        if (reader.getNumericDocValues(fi.name) != null ||
            reader.getSortedDocValues(fi.name) != null ||
            reader.getSortedSetDocValues(fi.name) != null) {
          throw new RuntimeException(fi.name + " returns multiple docvalues types!");
        }
        break;
      case NUMERIC:
        checkNumericDocValues(fi.name, reader, reader.getNumericDocValues(fi.name));
        if (reader.getBinaryDocValues(fi.name) != null ||
            reader.getSortedDocValues(fi.name) != null ||
            reader.getSortedSetDocValues(fi.name) != null) {
          throw new RuntimeException(fi.name + " returns multiple docvalues types!");
        }
        break;
      default:
        throw new AssertionError();
    }
  }
  
  private static void checkNorms(FieldInfo fi, AtomicReader reader, PrintStream infoStream) throws IOException {
    switch(fi.getNormType()) {
      case NUMERIC:
        checkNumericDocValues(fi.name, reader, reader.getNormValues(fi.name));
        break;
      default:
        throw new AssertionError("wtf: " + fi.getNormType());
    }
  }

  /**
   * Test term vectors.
   * @lucene.experimental
   */
  public static Status.TermVectorStatus testTermVectors(AtomicReader reader, PrintStream infoStream) {
    return testTermVectors(reader, infoStream, false, false);
  }

  /**
   * Test term vectors.
   * @lucene.experimental
   */
  public static Status.TermVectorStatus testTermVectors(AtomicReader reader, PrintStream infoStream, boolean verbose, boolean crossCheckTermVectors) {
    final Status.TermVectorStatus status = new Status.TermVectorStatus();
    final FieldInfos fieldInfos = reader.getFieldInfos();
    final Bits onlyDocIsDeleted = new FixedBitSet(1);
    
    try {
      if (infoStream != null) {
        infoStream.print("    test: term vectors........");
      }

      DocsEnum docs = null;
      DocsAndPositionsEnum postings = null;

      // Only used if crossCheckTermVectors is true:
      DocsEnum postingsDocs = null;
      DocsAndPositionsEnum postingsPostings = null;

      final Bits liveDocs = reader.getLiveDocs();

      final Fields postingsFields;
      // TODO: testTermsIndex
      if (crossCheckTermVectors) {
        postingsFields = reader.fields();
      } else {
        postingsFields = null;
      }

      TermsEnum termsEnum = null;
      TermsEnum postingsTermsEnum = null;

      for (int j = 0; j < reader.maxDoc(); ++j) {
        // Intentionally pull/visit (but don't count in
        // stats) deleted documents to make sure they too
        // are not corrupt:
        Fields tfv = reader.getTermVectors(j);

        // TODO: can we make a IS(FIR) that searches just
        // this term vector... to pass for searcher?

        if (tfv != null) {
          // First run with no deletions:
          checkFields(tfv, null, 1, fieldInfos, false, true, infoStream, verbose);

          // Again, with the one doc deleted:
          checkFields(tfv, onlyDocIsDeleted, 1, fieldInfos, false, true, infoStream, verbose);

          // Only agg stats if the doc is live:
          final boolean doStats = liveDocs == null || liveDocs.get(j);
          if (doStats) {
            status.docCount++;
          }

          for(String field : tfv) {
            if (doStats) {
              status.totVectors++;
            }

            // Make sure FieldInfo thinks this field is vector'd:
            final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
            if (!fieldInfo.hasVectors()) {
              throw new RuntimeException("docID=" + j + " has term vectors for field=" + field + " but FieldInfo has storeTermVector=false");
            }

            if (crossCheckTermVectors) {
              Terms terms = tfv.terms(field);
              termsEnum = terms.iterator(termsEnum);
              final boolean postingsHasFreq = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
              final boolean postingsHasPayload = fieldInfo.hasPayloads();
              final boolean vectorsHasPayload = terms.hasPayloads();

              Terms postingsTerms = postingsFields.terms(field);
              if (postingsTerms == null) {
                throw new RuntimeException("vector field=" + field + " does not exist in postings; doc=" + j);
              }
              postingsTermsEnum = postingsTerms.iterator(postingsTermsEnum);
              
              final boolean hasProx = terms.hasOffsets() || terms.hasPositions();
              BytesRef term = null;
              while ((term = termsEnum.next()) != null) {

                if (hasProx) {
                  postings = termsEnum.docsAndPositions(null, postings);
                  assert postings != null;
                  docs = null;
                } else {
                  docs = termsEnum.docs(null, docs);
                  assert docs != null;
                  postings = null;
                }

                final DocsEnum docs2;
                if (hasProx) {
                  assert postings != null;
                  docs2 = postings;
                } else {
                  assert docs != null;
                  docs2 = docs;
                }

                final DocsEnum postingsDocs2;
                if (!postingsTermsEnum.seekExact(term, true)) {
                  throw new RuntimeException("vector term=" + term + " field=" + field + " does not exist in postings; doc=" + j);
                }
                postingsPostings = postingsTermsEnum.docsAndPositions(null, postingsPostings);
                if (postingsPostings == null) {
                  // Term vectors were indexed w/ pos but postings were not
                  postingsDocs = postingsTermsEnum.docs(null, postingsDocs);
                  if (postingsDocs == null) {
                    throw new RuntimeException("vector term=" + term + " field=" + field + " does not exist in postings; doc=" + j);
                  }
                }

                if (postingsPostings != null) {
                  postingsDocs2 = postingsPostings;
                } else {
                  postingsDocs2 = postingsDocs;
                }
                  
                final int advanceDoc = postingsDocs2.advance(j);
                if (advanceDoc != j) {
                  throw new RuntimeException("vector term=" + term + " field=" + field + ": doc=" + j + " was not found in postings (got: " + advanceDoc + ")");
                }

                final int doc = docs2.nextDoc();
                  
                if (doc != 0) {
                  throw new RuntimeException("vector for doc " + j + " didn't return docID=0: got docID=" + doc);
                }

                if (postingsHasFreq) {
                  final int tf = docs2.freq();
                  if (postingsHasFreq && postingsDocs2.freq() != tf) {
                    throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + ": freq=" + tf + " differs from postings freq=" + postingsDocs2.freq());
                  }
                
                  if (hasProx) {
                    for (int i = 0; i < tf; i++) {
                      int pos = postings.nextPosition();
                      if (postingsPostings != null) {
                        int postingsPos = postingsPostings.nextPosition();
                        if (terms.hasPositions() && pos != postingsPos) {
                          throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + ": pos=" + pos + " differs from postings pos=" + postingsPos);
                        }
                      }

                      // Call the methods to at least make
                      // sure they don't throw exc:
                      final int startOffset = postings.startOffset();
                      final int endOffset = postings.endOffset();
                      // TODO: these are too anal...?
                      /*
                        if (endOffset < startOffset) {
                        throw new RuntimeException("vector startOffset=" + startOffset + " is > endOffset=" + endOffset);
                        }
                        if (startOffset < lastStartOffset) {
                        throw new RuntimeException("vector startOffset=" + startOffset + " is < prior startOffset=" + lastStartOffset);
                        }
                        lastStartOffset = startOffset;
                      */

                      if (postingsPostings != null) {
                        final int postingsStartOffset = postingsPostings.startOffset();

                        final int postingsEndOffset = postingsPostings.endOffset();
                        if (startOffset != -1 && postingsStartOffset != -1 && startOffset != postingsStartOffset) {
                          throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + ": startOffset=" + startOffset + " differs from postings startOffset=" + postingsStartOffset);
                        }
                        if (endOffset != -1 && postingsEndOffset != -1 && endOffset != postingsEndOffset) {
                          throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + ": endOffset=" + endOffset + " differs from postings endOffset=" + postingsEndOffset);
                        }
                      }
                      
                      BytesRef payload = postings.getPayload();
           
                      if (payload != null) {
                        assert vectorsHasPayload;
                      }
                      
                      if (postingsHasPayload && vectorsHasPayload) {
                        assert postingsPostings != null;
                        
                        if (payload == null) {
                          // we have payloads, but not at this position. 
                          // postings has payloads too, it should not have one at this position
                          if (postingsPostings.getPayload() != null) {
                            throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + " has no payload but postings does: " + postingsPostings.getPayload());
                          }
                        } else {
                          // we have payloads, and one at this position
                          // postings should also have one at this position, with the same bytes.
                          if (postingsPostings.getPayload() == null) {
                            throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + " has payload=" + payload + " but postings does not.");
                          }
                          BytesRef postingsPayload = postingsPostings.getPayload();
                          if (!payload.equals(postingsPayload)) {
                            throw new RuntimeException("vector term=" + term + " field=" + field + " doc=" + j + " has payload=" + payload + " but differs from postings payload=" + postingsPayload);
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      float vectorAvg = status.docCount == 0 ? 0 : status.totVectors / (float)status.docCount;
      msg(infoStream, "OK [" + status.totVectors + " total vector count; avg " + 
          NumberFormat.getInstance(Locale.ROOT).format(vectorAvg) + " term/freq vector fields per doc]");
    } catch (Throwable e) {
      msg(infoStream, "ERROR [" + String.valueOf(e.getMessage()) + "]");
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
  public void fixIndex(Status result, Codec codec) throws IOException {
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
    java -ea:org.apache.lucene... org.apache.lucene.index.CheckIndex pathToIndex [-fix] [-verbose] [-segment X] [-segment Y]
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
    boolean doCrossCheckTermVectors = false;
    Codec codec = Codec.getDefault(); // only used when fixing
    boolean verbose = false;
    List<String> onlySegments = new ArrayList<String>();
    String indexPath = null;
    String dirImpl = null;
    int i = 0;
    while(i < args.length) {
      String arg = args[i];
      if ("-fix".equals(arg)) {
        doFix = true;
      } else if ("-crossCheckTermVectors".equals(arg)) {
        doCrossCheckTermVectors = true;
      } else if ("-codec".equals(arg)) {
        if (i == args.length-1) {
          System.out.println("ERROR: missing name for -codec option");
          System.exit(1);
        }
        i++;
        codec = Codec.forName(args[i]);
      } else if (arg.equals("-verbose")) {
        verbose = true;
      } else if (arg.equals("-segment")) {
        if (i == args.length-1) {
          System.out.println("ERROR: missing name for -segment option");
          System.exit(1);
        }
        i++;
        onlySegments.add(args[i]);
      } else if ("-dir-impl".equals(arg)) {
        if (i == args.length - 1) {
          System.out.println("ERROR: missing value for -dir-impl option");
          System.exit(1);
        }
        i++;
        dirImpl = args[i];
      } else {
        if (indexPath != null) {
          System.out.println("ERROR: unexpected extra argument '" + args[i] + "'");
          System.exit(1);
        }
        indexPath = args[i];
      }
      i++;
    }

    if (indexPath == null) {
      System.out.println("\nERROR: index path not specified");
      System.out.println("\nUsage: java org.apache.lucene.index.CheckIndex pathToIndex [-fix] [-crossCheckTermVectors] [-segment X] [-segment Y] [-dir-impl X]\n" +
                         "\n" +
                         "  -fix: actually write a new segments_N file, removing any problematic segments\n" +
                         "  -crossCheckTermVectors: verifies that term vectors match postings; THIS IS VERY SLOW!\n" +
                         "  -codec X: when fixing, codec to write the new segments_N file with\n" +
                         "  -verbose: print additional details\n" +
                         "  -segment X: only check the specified segments.  This can be specified multiple\n" + 
                         "              times, to check more than one segment, eg '-segment _2 -segment _a'.\n" +
                         "              You can't use this with the -fix option\n" +
                         "  -dir-impl X: use a specific " + FSDirectory.class.getSimpleName() + " implementation. " +
                         "If no package is specified the " + FSDirectory.class.getPackage().getName() + " package will be used.\n" +
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
      if (dirImpl == null) {
        dir = FSDirectory.open(new File(indexPath));
      } else {
        dir = CommandLineUtil.newFSDirectory(dirImpl, new File(indexPath));
      }
    } catch (Throwable t) {
      System.out.println("ERROR: could not open directory \"" + indexPath + "\"; exiting");
      t.printStackTrace(System.out);
      System.exit(1);
    }

    CheckIndex checker = new CheckIndex(dir);
    checker.setCrossCheckTermVectors(doCrossCheckTermVectors);
    checker.setInfoStream(System.out, verbose);

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
        checker.fixIndex(result, codec);
        System.out.println("OK");
        System.out.println("Wrote new segments file \"" + result.newSegments.getSegmentsFileName() + "\"");
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
