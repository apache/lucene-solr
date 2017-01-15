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
package org.apache.lucene.index;


import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/**
 * A collection of segmentInfo objects with methods for operating on those
 * segments in relation to the file system.
 * <p>
 * The active segments in the index are stored in the segment info file,
 * <tt>segments_N</tt>. There may be one or more <tt>segments_N</tt> files in
 * the index; however, the one with the largest generation is the active one
 * (when older segments_N files are present it's because they temporarily cannot
 * be deleted, or a custom {@link IndexDeletionPolicy} is in
 * use). This file lists each segment by name and has details about the codec
 * and generation of deletes.
 * </p>
 * <p>
 * Files:
 * <ul>
 * <li><tt>segments_N</tt>: Header, LuceneVersion, Version, NameCounter, SegCount, MinSegmentLuceneVersion, &lt;SegName,
 * HasSegID, SegID, SegCodec, DelGen, DeletionCount, FieldInfosGen, DocValuesGen,
 * UpdatesFiles&gt;<sup>SegCount</sup>, CommitUserData, Footer
 * </ul>
 * Data types:
 * <ul>
 * <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 * <li>LuceneVersion --&gt; Which Lucene code {@link Version} was used for this commit, written as three {@link DataOutput#writeVInt vInt}: major, minor, bugfix
 * <li>MinSegmentLuceneVersion --&gt; Lucene code {@link Version} of the oldest segment, written as three {@link DataOutput#writeVInt vInt}: major, minor, bugfix; this is only
 *   written only if there's at least one segment
 * <li>NameCounter, SegCount, DeletionCount --&gt;
 * {@link DataOutput#writeInt Int32}</li>
 * <li>Generation, Version, DelGen, Checksum, FieldInfosGen, DocValuesGen --&gt;
 * {@link DataOutput#writeLong Int64}</li>
 * <li>HasSegID --&gt; {@link DataOutput#writeByte Int8}</li>
 * <li>SegID --&gt; {@link DataOutput#writeByte Int8<sup>ID_LENGTH</sup>}</li>
 * <li>SegName, SegCodec --&gt; {@link DataOutput#writeString String}</li>
 * <li>CommitUserData --&gt; {@link DataOutput#writeMapOfStrings
 * Map&lt;String,String&gt;}</li>
 * <li>UpdatesFiles --&gt; Map&lt;{@link DataOutput#writeInt Int32},
 * {@link DataOutput#writeSetOfStrings(Set) Set&lt;String&gt;}&gt;</li>
 * <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * Field Descriptions:
 * <ul>
 * <li>Version counts how often the index has been changed by adding or deleting
 * documents.</li>
 * <li>NameCounter is used to generate names for new segment files.</li>
 * <li>SegName is the name of the segment, and is used as the file name prefix
 * for all of the files that compose the segment's index.</li>
 * <li>DelGen is the generation count of the deletes file. If this is -1, there
 * are no deletes. Anything above zero means there are deletes stored by
 * {@link LiveDocsFormat}.</li>
 * <li>DeletionCount records the number of deleted documents in this segment.</li>
 * <li>SegCodec is the {@link Codec#getName() name} of the Codec that encoded
 * this segment.</li>
 * <li>HasSegID is nonzero if the segment has an identifier. Otherwise, when it is 0
 * the identifier is {@code null} and no SegID is written. Null only happens for Lucene
 * 4.x segments referenced in commits.</li>
 * <li>SegID is the identifier of the Codec that encoded this segment. </li>
 * <li>CommitUserData stores an optional user-supplied opaque
 * Map&lt;String,String&gt; that was passed to
 * {@link IndexWriter#setLiveCommitData(Iterable)}.</li>
 * <li>FieldInfosGen is the generation count of the fieldInfos file. If this is
 * -1, there are no updates to the fieldInfos in that segment. Anything above
 * zero means there are updates to fieldInfos stored by {@link FieldInfosFormat}
 * .</li>
 * <li>DocValuesGen is the generation count of the updatable DocValues. If this
 * is -1, there are no updates to DocValues in that segment. Anything above zero
 * means there are updates to DocValues stored by {@link DocValuesFormat}.</li>
 * <li>UpdatesFiles stores the set of files that were updated in that segment
 * per field.</li>
 * </ul>
 * 
 * @lucene.experimental
 */
public final class SegmentInfos implements Cloneable, Iterable<SegmentCommitInfo> {

  /** Adds the {@link Version} that committed this segments_N file, as well as the {@link Version} of the oldest segment, since 5.3+ */
  public static final int VERSION_53 = 6;

  static final int VERSION_CURRENT = VERSION_53;

  /** Used to name new segments. */
  // TODO: should this be a long ...?
  public int counter;
  
  /** Counts how often the index has been changed.  */
  public long version;

  private long generation;     // generation of the "segments_N" for the next commit
  private long lastGeneration; // generation of the "segments_N" file we last successfully read
                               // or wrote; this is normally the same as generation except if
                               // there was an IOException that had interrupted a commit

  /** Opaque Map&lt;String, String&gt; that user can specify during IndexWriter.commit */
  public Map<String,String> userData = Collections.emptyMap();
  
  private List<SegmentCommitInfo> segments = new ArrayList<>();
  
  /**
   * If non-null, information about loading segments_N files
   * will be printed here.  @see #setInfoStream.
   */
  private static PrintStream infoStream = null;

  /** Id for this commit; only written starting with Lucene 5.0 */
  private byte[] id;

  /** Which Lucene version wrote this commit, or null if this commit is pre-5.3. */
  private Version luceneVersion;

  /** Version of the oldest segment in the index, or null if there are no segments. */
  private Version minSegmentLuceneVersion;

  /** Sole constructor. Typically you call this and then
   *  use {@link #readLatestCommit(Directory) or
   *  #readCommit(Directory,String)} to populate each {@link
   *  SegmentCommitInfo}.  Alternatively, you can add/remove your
   *  own {@link SegmentCommitInfo}s. */
  public SegmentInfos() {
  }

  /** Returns {@link SegmentCommitInfo} at the provided
   *  index. */
  public SegmentCommitInfo info(int i) {
    return segments.get(i);
  }

  /**
   * Get the generation of the most recent commit to the
   * list of index files (N in the segments_N file).
   *
   * @param files -- array of file names to check
   */
  public static long getLastCommitGeneration(String[] files) {
    long max = -1;
    for (String file : files) {
      if (file.startsWith(IndexFileNames.SEGMENTS) && !file.equals(IndexFileNames.OLD_SEGMENTS_GEN)) {
        long gen = generationFromSegmentsFileName(file);
        if (gen > max) {
          max = gen;
        }
      }
    }
    return max;
  }

  /**
   * Get the generation of the most recent commit to the
   * index in this directory (N in the segments_N file).
   *
   * @param directory -- directory to search for the latest segments_N file
   */
  public static long getLastCommitGeneration(Directory directory) throws IOException {
    return getLastCommitGeneration(directory.listAll());
  }

  /**
   * Get the filename of the segments_N file for the most
   * recent commit in the list of index files.
   *
   * @param files -- array of file names to check
   */

  public static String getLastCommitSegmentsFileName(String[] files) {
    return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                 "",
                                                 getLastCommitGeneration(files));
  }

  /**
   * Get the filename of the segments_N file for the most
   * recent commit to the index in this Directory.
   *
   * @param directory -- directory to search for the latest segments_N file
   */
  public static String getLastCommitSegmentsFileName(Directory directory) throws IOException {
    return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                 "",
                                                 getLastCommitGeneration(directory));
  }

  /**
   * Get the segments_N filename in use by this segment infos.
   */
  public String getSegmentsFileName() {
    return IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                 "",
                                                 lastGeneration);
  }
  
  /**
   * Parse the generation off the segments file name and
   * return it.
   */
  public static long generationFromSegmentsFileName(String fileName) {
    if (fileName.equals(IndexFileNames.SEGMENTS)) {
      return 0;
    } else if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
      return Long.parseLong(fileName.substring(1+IndexFileNames.SEGMENTS.length()),
                            Character.MAX_RADIX);
    } else {
      throw new IllegalArgumentException("fileName \"" + fileName + "\" is not a segments file");
    }
  }
  
  /** return generation of the next pending_segments_N that will be written */
  private long getNextPendingGeneration() {
    if (generation == -1) {
      return 1;
    } else {
      return generation+1;
    }
  }

  /** Since Lucene 5.0, every commit (segments_N) writes a unique id.  This will
   *  return that id */
  public byte[] getId() {
    return id.clone();
  }

  /**
   * Read a particular segmentFileName.  Note that this may
   * throw an IOException if a commit is in process.
   *
   * @param directory -- directory containing the segments file
   * @param segmentFileName -- segment file to load
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  public static final SegmentInfos readCommit(Directory directory, String segmentFileName) throws IOException {

    long generation = generationFromSegmentsFileName(segmentFileName);
    //System.out.println(Thread.currentThread() + ": SegmentInfos.readCommit " + segmentFileName);
    try (ChecksumIndexInput input = directory.openChecksumInput(segmentFileName, IOContext.READ)) {
      try {
        return readCommit(directory, input, generation);
      } catch (EOFException e) {
        throw new CorruptIndexException("Unexpected end of file while reading index.", input, e);
      }
    }
  }

  /** Read the commit from the provided {@link ChecksumIndexInput}. */
  public static final SegmentInfos readCommit(Directory directory, ChecksumIndexInput input, long generation) throws IOException {

    // NOTE: as long as we want to throw indexformattooold (vs corruptindexexception), we need
    // to read the magic ourselves.
    int magic = input.readInt();
    if (magic != CodecUtil.CODEC_MAGIC) {
      throw new IndexFormatTooOldException(input, magic, CodecUtil.CODEC_MAGIC, CodecUtil.CODEC_MAGIC);
    }
    int format = CodecUtil.checkHeaderNoMagic(input, "segments", VERSION_53, VERSION_CURRENT);
    byte id[] = new byte[StringHelper.ID_LENGTH];
    input.readBytes(id, 0, id.length);
    CodecUtil.checkIndexHeaderSuffix(input, Long.toString(generation, Character.MAX_RADIX));

    SegmentInfos infos = new SegmentInfos();
    infos.id = id;
    infos.generation = generation;
    infos.lastGeneration = generation;
    if (format >= VERSION_53) {
      infos.luceneVersion = Version.fromBits(input.readVInt(), input.readVInt(), input.readVInt());
      if (infos.luceneVersion.onOrAfter(Version.LUCENE_6_0_0) == false) {
        throw new IndexFormatTooOldException(input, "this index is too old (version: " + infos.luceneVersion + ")");
      }
    } else {
      throw new IndexFormatTooOldException(input, "this index segments file is too old (segment infos format: " + format + ")");
    }

    infos.version = input.readLong();
    //System.out.println("READ sis version=" + infos.version);
    infos.counter = input.readInt();
    int numSegments = input.readInt();
    if (numSegments < 0) {
      throw new CorruptIndexException("invalid segment count: " + numSegments, input);
    }

    if (numSegments > 0) {
      infos.minSegmentLuceneVersion = Version.fromBits(input.readVInt(), input.readVInt(), input.readVInt());
    } else {
      // else leave as null: no segments
    }

    long totalDocs = 0;
    for (int seg = 0; seg < numSegments; seg++) {
      String segName = input.readString();
      final byte segmentID[];
      byte hasID = input.readByte();
      if (hasID == 1) {
        segmentID = new byte[StringHelper.ID_LENGTH];
        input.readBytes(segmentID, 0, segmentID.length);
      } else if (hasID == 0) {
        throw new IndexFormatTooOldException(input, "Segment is from Lucene 4.x");
      } else {
        throw new CorruptIndexException("invalid hasID byte, got: " + hasID, input);
      }
      Codec codec = readCodec(input, format < VERSION_53);
      SegmentInfo info = codec.segmentInfoFormat().read(directory, segName, segmentID, IOContext.READ);
      info.setCodec(codec);
      totalDocs += info.maxDoc();
      long delGen = input.readLong();
      int delCount = input.readInt();
      if (delCount < 0 || delCount > info.maxDoc()) {
        throw new CorruptIndexException("invalid deletion count: " + delCount + " vs maxDoc=" + info.maxDoc(), input);
      }
      long fieldInfosGen = input.readLong();
      long dvGen = input.readLong();
      SegmentCommitInfo siPerCommit = new SegmentCommitInfo(info, delCount, delGen, fieldInfosGen, dvGen);
      siPerCommit.setFieldInfosFiles(input.readSetOfStrings());
      final Map<Integer,Set<String>> dvUpdateFiles;
      final int numDVFields = input.readInt();
      if (numDVFields == 0) {
        dvUpdateFiles = Collections.emptyMap();
      } else {
        Map<Integer,Set<String>> map = new HashMap<>(numDVFields);
        for (int i = 0; i < numDVFields; i++) {
          map.put(input.readInt(), input.readSetOfStrings());
        }
        dvUpdateFiles = Collections.unmodifiableMap(map);
      }
      siPerCommit.setDocValuesUpdatesFiles(dvUpdateFiles);
      infos.add(siPerCommit);

      Version segmentVersion = info.getVersion();

      if (segmentVersion.onOrAfter(infos.minSegmentLuceneVersion) == false) {
        throw new CorruptIndexException("segments file recorded minSegmentLuceneVersion=" + infos.minSegmentLuceneVersion + " but segment=" + info + " has older version=" + segmentVersion, input);
      }
    }

    infos.userData = input.readMapOfStrings();

    CodecUtil.checkFooter(input);

    // LUCENE-6299: check we are in bounds
    if (totalDocs > IndexWriter.getActualMaxDocs()) {
      throw new CorruptIndexException("Too many documents: an index cannot exceed " + IndexWriter.getActualMaxDocs() + " but readers have total maxDoc=" + totalDocs, input);
    }

    return infos;
  }

  private static final List<String> unsupportedCodecs = Arrays.asList(
      "Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45", "Lucene46", "Lucene49", "Lucene410"
  );

  private static Codec readCodec(DataInput input, boolean unsupportedAllowed) throws IOException {
    final String name = input.readString();
    try {
      return Codec.forName(name);
    } catch (IllegalArgumentException e) {
      // give better error messages if we can, first check if this is a legacy codec
      if (unsupportedCodecs.contains(name)) {
        // We should only get here on pre-5.3 indices, but we can't test this until 7.0 when 5.x indices become too old:
        assert unsupportedAllowed;
        IOException newExc = new IndexFormatTooOldException(input, "Codec '" + name + "' is too old");
        newExc.initCause(e);
        throw newExc;
      }
      // or maybe it's an old default codec that moved
      if (name.startsWith("Lucene")) {
        throw new IllegalArgumentException("Could not load codec '" + name + "'.  Did you forget to add lucene-backward-codecs.jar?", e);
      }
      throw e;
    }
  }

  /** Find the latest commit ({@code segments_N file}) and
   *  load all {@link SegmentCommitInfo}s. */
  public static final SegmentInfos readLatestCommit(Directory directory) throws IOException {
    return new FindSegmentsFile<SegmentInfos>(directory) {
      @Override
      protected SegmentInfos doBody(String segmentFileName) throws IOException {
        return readCommit(directory, segmentFileName);
      }
    }.run();
  }

  // Only true after prepareCommit has been called and
  // before finishCommit is called
  boolean pendingCommit;

  private void write(Directory directory) throws IOException {

    long nextGeneration = getNextPendingGeneration();
    String segmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.PENDING_SEGMENTS,
                                                                   "",
                                                                   nextGeneration);

    // Always advance the generation on write:
    generation = nextGeneration;
    
    IndexOutput segnOutput = null;
    boolean success = false;

    try {
      segnOutput = directory.createOutput(segmentFileName, IOContext.DEFAULT);
      write(directory, segnOutput);
      segnOutput.close();
      directory.sync(Collections.singleton(segmentFileName));
      success = true;
    } finally {
      if (success) {
        pendingCommit = true;
      } else {
        // We hit an exception above; try to close the file
        // but suppress any exception:
        IOUtils.closeWhileHandlingException(segnOutput);
        // Try not to leave a truncated segments_N file in
        // the index:
        IOUtils.deleteFilesIgnoringExceptions(directory, segmentFileName);
      }
    }
  }

  /** Write ourselves to the provided {@link IndexOutput} */
  public void write(Directory directory, IndexOutput out) throws IOException {
    CodecUtil.writeIndexHeader(out, "segments", VERSION_CURRENT, 
                               StringHelper.randomId(), Long.toString(generation, Character.MAX_RADIX));
    out.writeVInt(Version.LATEST.major);
    out.writeVInt(Version.LATEST.minor);
    out.writeVInt(Version.LATEST.bugfix);
    //System.out.println(Thread.currentThread().getName() + ": now write " + out.getName() + " with version=" + version);

    out.writeLong(version); 
    out.writeInt(counter); // write counter
    out.writeInt(size());

    if (size() > 0) {

      Version minSegmentVersion = null;

      // We do a separate loop up front so we can write the minSegmentVersion before
      // any SegmentInfo; this makes it cleaner to throw IndexFormatTooOldExc at read time:
      for (SegmentCommitInfo siPerCommit : this) {
        Version segmentVersion = siPerCommit.info.getVersion();
        if (minSegmentVersion == null || segmentVersion.onOrAfter(minSegmentVersion) == false) {
          minSegmentVersion = segmentVersion;
        }
      }

      out.writeVInt(minSegmentVersion.major);
      out.writeVInt(minSegmentVersion.minor);
      out.writeVInt(minSegmentVersion.bugfix);
    }

    // write infos
    for (SegmentCommitInfo siPerCommit : this) {
      SegmentInfo si = siPerCommit.info;
      out.writeString(si.name);
      byte segmentID[] = si.getId();
      // TODO: remove this in lucene 6, we don't need to include 4.x segments in commits anymore
      if (segmentID == null) {
        out.writeByte((byte)0);
      } else {
        if (segmentID.length != StringHelper.ID_LENGTH) {
          throw new IllegalStateException("cannot write segment: invalid id segment=" + si.name + "id=" + StringHelper.idToString(segmentID));
        }
        out.writeByte((byte)1);
        out.writeBytes(segmentID, segmentID.length);
      }
      out.writeString(si.getCodec().getName());
      out.writeLong(siPerCommit.getDelGen());
      int delCount = siPerCommit.getDelCount();
      if (delCount < 0 || delCount > si.maxDoc()) {
        throw new IllegalStateException("cannot write segment: invalid maxDoc segment=" + si.name + " maxDoc=" + si.maxDoc() + " delCount=" + delCount);
      }
      out.writeInt(delCount);
      out.writeLong(siPerCommit.getFieldInfosGen());
      out.writeLong(siPerCommit.getDocValuesGen());
      out.writeSetOfStrings(siPerCommit.getFieldInfosFiles());
      final Map<Integer,Set<String>> dvUpdatesFiles = siPerCommit.getDocValuesUpdatesFiles();
      out.writeInt(dvUpdatesFiles.size());
      for (Entry<Integer,Set<String>> e : dvUpdatesFiles.entrySet()) {
        out.writeInt(e.getKey());
        out.writeSetOfStrings(e.getValue());
      }
    }
    out.writeMapOfStrings(userData);
    CodecUtil.writeFooter(out);
  }

  /**
   * Returns a copy of this instance, also copying each
   * SegmentInfo.
   */
  
  @Override
  public SegmentInfos clone() {
    try {
      final SegmentInfos sis = (SegmentInfos) super.clone();
      // deep clone, first recreate all collections:
      sis.segments = new ArrayList<>(size());
      for(final SegmentCommitInfo info : this) {
        assert info.info.getCodec() != null;
        // dont directly access segments, use add method!!!
        sis.add(info.clone());
      }
      sis.userData = new HashMap<>(userData);
      return sis;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("should not happen", e);
    }
  }

  /**
   * version number when this SegmentInfos was generated.
   */
  public long getVersion() {
    return version;
  }

  /** Returns current generation. */
  public long getGeneration() {
    return generation;
  }

  /** Returns last succesfully read or written generation. */
  public long getLastGeneration() {
    return lastGeneration;
  }

  /** If non-null, information about retries when loading
   * the segments file will be printed to this.
   */
  public static void setInfoStream(PrintStream infoStream) {
    SegmentInfos.infoStream = infoStream;
  }

  /**
   * Returns {@code infoStream}.
   *
   * @see #setInfoStream
   */
  public static PrintStream getInfoStream() {
    return infoStream;
  }

  /**
   * Prints the given message to the infoStream. Note, this method does not
   * check for null infoStream. It assumes this check has been performed by the
   * caller, which is recommended to avoid the (usually) expensive message
   * creation.
   */
  private static void message(String message) {
    infoStream.println("SIS [" + Thread.currentThread().getName() + "]: " + message);
  }

  /**
   * Utility class for executing code that needs to do
   * something with the current segments file.  This is
   * necessary with lock-less commits because from the time
   * you locate the current segments file name, until you
   * actually open it, read its contents, or check modified
   * time, etc., it could have been deleted due to a writer
   * commit finishing.
   */
  public abstract static class FindSegmentsFile<T> {

    final Directory directory;

    /** Sole constructor. */ 
    public FindSegmentsFile(Directory directory) {
      this.directory = directory;
    }

    /** Locate the most recent {@code segments} file and
     *  run {@link #doBody} on it. */
    public T run() throws IOException {
      return run(null);
    }
    
    /** Run {@link #doBody} on the provided commit. */
    public T run(IndexCommit commit) throws IOException {
      if (commit != null) {
        if (directory != commit.getDirectory())
          throw new IOException("the specified commit does not match the specified Directory");
        return doBody(commit.getSegmentsFileName());
      }

      long lastGen = -1;
      long gen = -1;
      IOException exc = null;

      // Loop until we succeed in calling doBody() without
      // hitting an IOException.  An IOException most likely
      // means an IW deleted our commit while opening
      // the time it took us to load the now-old infos files
      // (and segments files).  It's also possible it's a
      // true error (corrupt index).  To distinguish these,
      // on each retry we must see "forward progress" on
      // which generation we are trying to load.  If we
      // don't, then the original error is real and we throw
      // it.
      
      for (;;) {
        lastGen = gen;
        String files[] = directory.listAll();
        String files2[] = directory.listAll();
        Arrays.sort(files);
        Arrays.sort(files2);
        if (!Arrays.equals(files, files2)) {
          // listAll() is weakly consistent, this means we hit "concurrent modification exception"
          continue;
        }
        gen = getLastCommitGeneration(files);
        
        if (infoStream != null) {
          message("directory listing gen=" + gen);
        }
        
        if (gen == -1) {
          throw new IndexNotFoundException("no segments* file found in " + directory + ": files: " + Arrays.toString(files));
        } else if (gen > lastGen) {
          String segmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen);
        
          try {
            T t = doBody(segmentFileName);
            if (infoStream != null) {
              message("success on " + segmentFileName);
            }
            return t;
          } catch (IOException err) {
            // Save the original root cause:
            if (exc == null) {
              exc = err;
            }

            if (infoStream != null) {
              message("primary Exception on '" + segmentFileName + "': " + err + "'; will retry: gen = " + gen);
            }
          }
        } else {
          throw exc;
        }
      }
    }

    /**
     * Subclass must implement this.  The assumption is an
     * IOException will be thrown if something goes wrong
     * during the processing that could have been caused by
     * a writer committing.
     */
    protected abstract T doBody(String segmentFileName) throws IOException;
  }

  /** Carry over generation numbers from another SegmentInfos
   *
   * @lucene.internal */
  public void updateGeneration(SegmentInfos other) {
    lastGeneration = other.lastGeneration;
    generation = other.generation;
  }

  // Carry over generation numbers, and version/counter, from another SegmentInfos
  void updateGenerationVersionAndCounter(SegmentInfos other) {
    updateGeneration(other);
    this.version = other.version;
    this.counter = other.counter;
  }

  /** Set the generation to be used for the next commit */
  public void setNextWriteGeneration(long generation) {
    if (generation < this.generation) {
      throw new IllegalStateException("cannot decrease generation to " + generation + " from current generation " + this.generation);
    }
    this.generation = generation;
  }

  final void rollbackCommit(Directory dir) {
    if (pendingCommit) {
      pendingCommit = false;
      
      // we try to clean up our pending_segments_N

      // Must carefully compute fileName from "generation"
      // since lastGeneration isn't incremented:
      final String pending = IndexFileNames.fileNameFromGeneration(IndexFileNames.PENDING_SEGMENTS, "", generation);

      // Suppress so we keep throwing the original exception
      // in our caller
      IOUtils.deleteFilesIgnoringExceptions(dir, pending);
    }
  }

  /** Call this to start a commit.  This writes the new
   *  segments file, but writes an invalid checksum at the
   *  end, so that it is not visible to readers.  Once this
   *  is called you must call {@link #finishCommit} to complete
   *  the commit or {@link #rollbackCommit} to abort it.
   *  <p>
   *  Note: {@link #changed()} should be called prior to this
   *  method if changes have been made to this {@link SegmentInfos} instance
   *  </p>  
   **/
  final void prepareCommit(Directory dir) throws IOException {
    if (pendingCommit) {
      throw new IllegalStateException("prepareCommit was already called");
    }
    write(dir);
  }

  /** Returns all file names referenced by SegmentInfo.
   *  The returned collection is recomputed on each
   *  invocation.  */
  public Collection<String> files(boolean includeSegmentsFile) throws IOException {
    HashSet<String> files = new HashSet<>();
    if (includeSegmentsFile) {
      final String segmentFileName = getSegmentsFileName();
      if (segmentFileName != null) {
        files.add(segmentFileName);
      }
    }
    final int size = size();
    for(int i=0;i<size;i++) {
      final SegmentCommitInfo info = info(i);
      files.addAll(info.files());
    }
    
    return files;
  }

  /** Returns the committed segments_N filename. */
  final String finishCommit(Directory dir) throws IOException {
    if (pendingCommit == false) {
      throw new IllegalStateException("prepareCommit was not called");
    }
    boolean success = false;
    final String dest;
    try {
      final String src = IndexFileNames.fileNameFromGeneration(IndexFileNames.PENDING_SEGMENTS, "", generation);
      dest = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", generation);
      dir.rename(src, dest);
      dir.syncMetaData();
      success = true;
    } finally {
      if (!success) {
        // deletes pending_segments_N:
        rollbackCommit(dir);
      }
    }

    pendingCommit = false;
    lastGeneration = generation;
    return dest;
  }

  /** Writes and syncs to the Directory dir, taking care to
   *  remove the segments file on exception
   *  <p>
   *  Note: {@link #changed()} should be called prior to this
   *  method if changes have been made to this {@link SegmentInfos} instance
   *  </p>  
   **/
  public final void commit(Directory dir) throws IOException {
    prepareCommit(dir);
    finishCommit(dir);
  }

  /** Returns readable description of this segment. */
  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(getSegmentsFileName()).append(": ");
    final int count = size();
    for(int i = 0; i < count; i++) {
      if (i > 0) {
        buffer.append(' ');
      }
      final SegmentCommitInfo info = info(i);
      buffer.append(info.toString(0));
    }
    return buffer.toString();
  }

  /** Return {@code userData} saved with this commit.
   * 
   * @see IndexWriter#commit()
   */
  public Map<String,String> getUserData() {
    return userData;
  }

  /** Sets the commit data. */
  public void setUserData(Map<String,String> data, boolean doIncrementVersion) {
    if (data == null) {
      userData = Collections.<String,String>emptyMap();
    } else {
      userData = data;
    }
    if (doIncrementVersion) {
      changed();
    }
  }

  /** Replaces all segments in this instance, but keeps
   *  generation, version, counter so that future commits
   *  remain write once.
   */
  void replace(SegmentInfos other) {
    rollbackSegmentInfos(other.asList());
    lastGeneration = other.lastGeneration;
  }

  /** Returns sum of all segment's maxDocs.  Note that
   *  this does not include deletions */
  public int totalMaxDoc() {
    long count = 0;
    for(SegmentCommitInfo info : this) {
      count += info.info.maxDoc();
    }
    // we should never hit this, checks should happen elsewhere...
    assert count <= IndexWriter.getActualMaxDocs();
    return Math.toIntExact(count);
  }

  /** Call this before committing if changes have been made to the
   *  segments. */
  public void changed() {
    version++;
    //System.out.println(Thread.currentThread().getName() + ": SIS.change to version=" + version);
    //new Throwable().printStackTrace(System.out);
  }
  
  void setVersion(long newVersion) {
    if (newVersion < version) {
      throw new IllegalArgumentException("newVersion (=" + newVersion + ") cannot be less than current version (=" + version + ")");
    }
    //System.out.println(Thread.currentThread().getName() + ": SIS.setVersion change from " + version + " to " + newVersion);
    version = newVersion;
  }

  /** applies all changes caused by committing a merge to this SegmentInfos */
  void applyMergeChanges(MergePolicy.OneMerge merge, boolean dropSegment) {
    final Set<SegmentCommitInfo> mergedAway = new HashSet<>(merge.segments);
    boolean inserted = false;
    int newSegIdx = 0;
    for (int segIdx = 0, cnt = segments.size(); segIdx < cnt; segIdx++) {
      assert segIdx >= newSegIdx;
      final SegmentCommitInfo info = segments.get(segIdx);
      if (mergedAway.contains(info)) {
        if (!inserted && !dropSegment) {
          segments.set(segIdx, merge.info);
          inserted = true;
          newSegIdx++;
        }
      } else {
        segments.set(newSegIdx, info);
        newSegIdx++;
      }
    }

    // the rest of the segments in list are duplicates, so don't remove from map, only list!
    segments.subList(newSegIdx, segments.size()).clear();
    
    // Either we found place to insert segment, or, we did
    // not, but only because all segments we merged becamee
    // deleted while we are merging, in which case it should
    // be the case that the new segment is also all deleted,
    // we insert it at the beginning if it should not be dropped:
    if (!inserted && !dropSegment) {
      segments.add(0, merge.info);
    }
  }

  List<SegmentCommitInfo> createBackupSegmentInfos() {
    final List<SegmentCommitInfo> list = new ArrayList<>(size());
    for(final SegmentCommitInfo info : this) {
      assert info.info.getCodec() != null;
      list.add(info.clone());
    }
    return list;
  }
  
  void rollbackSegmentInfos(List<SegmentCommitInfo> infos) {
    this.clear();
    this.addAll(infos);
  }
  
  /** Returns an <b>unmodifiable</b> {@link Iterator} of contained segments in order. */
  // @Override (comment out until Java 6)
  @Override
  public Iterator<SegmentCommitInfo> iterator() {
    return asList().iterator();
  }
  
  /** Returns all contained segments as an <b>unmodifiable</b> {@link List} view. */
  public List<SegmentCommitInfo> asList() {
    return Collections.unmodifiableList(segments);
  }

  /** Returns number of {@link SegmentCommitInfo}s. */
  public int size() {
    return segments.size();
  }

  /** Appends the provided {@link SegmentCommitInfo}. */
  public void add(SegmentCommitInfo si) {
    segments.add(si);
  }
  
  /** Appends the provided {@link SegmentCommitInfo}s. */
  public void addAll(Iterable<SegmentCommitInfo> sis) {
    for (final SegmentCommitInfo si : sis) {
      this.add(si);
    }
  }
  
  /** Clear all {@link SegmentCommitInfo}s. */
  public void clear() {
    segments.clear();
  }

  /** Remove the provided {@link SegmentCommitInfo}.
   *
   * <p><b>WARNING</b>: O(N) cost */
  public void remove(SegmentCommitInfo si) {
    segments.remove(si);
  }
  
  /** Remove the {@link SegmentCommitInfo} at the
   * provided index.
   *
   * <p><b>WARNING</b>: O(N) cost */
  void remove(int index) {
    segments.remove(index);
  }

  /** Return true if the provided {@link
   *  SegmentCommitInfo} is contained.
   *
   * <p><b>WARNING</b>: O(N) cost */
  boolean contains(SegmentCommitInfo si) {
    return segments.contains(si);
  }

  /** Returns index of the provided {@link
   *  SegmentCommitInfo}.
   *
   * <p><b>WARNING</b>: O(N) cost */
  int indexOf(SegmentCommitInfo si) {
    return segments.indexOf(si);
  }

  /** Returns which Lucene {@link Version} wrote this commit, or null if the
   *  version this index was written with did not directly record the version. */
  public Version getCommitLuceneVersion() {
    return luceneVersion;
  }

  /** Returns the version of the oldest segment, or null if there are no segments. */
  public Version getMinSegmentLuceneVersion() {
    return minSegmentLuceneVersion;
  }
}
