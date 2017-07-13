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
package org.apache.lucene.util;


import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;

/**
 * On-disk sorting of byte arrays. Each byte array (entry) is a composed of the following
 * fields:
 * <ul>
 *   <li>(two bytes) length of the following byte array,
 *   <li>exactly the above count of bytes for the sequence to be sorted.
 * </ul>
 * 
 * @see #sort(String)
 * @lucene.experimental
 * @lucene.internal
 */
public class OfflineSorter {

  /** Convenience constant for megabytes */
  public final static long MB = 1024 * 1024;
  /** Convenience constant for gigabytes */
  public final static long GB = MB * 1024;
  
  /**
   * Minimum recommended buffer size for sorting.
   */
  public final static long MIN_BUFFER_SIZE_MB = 32;

  /**
   * Absolute minimum required buffer size for sorting.
   */
  public static final long ABSOLUTE_MIN_SORT_BUFFER_SIZE = MB / 2;
  private static final String MIN_BUFFER_SIZE_MSG = "At least 0.5MB RAM buffer is needed";

  /**
   * Maximum number of temporary files before doing an intermediate merge.
   */
  public final static int MAX_TEMPFILES = 10;

  private final Directory dir;
  private final int valueLength;
  private final String tempFileNamePrefix;

  private final ExecutorService exec;
  private final Semaphore partitionsInRAM;

  /** 
   * A bit more descriptive unit for constructors.
   * 
   * @see #automatic()
   * @see #megabytes(long)
   */
  public static final class BufferSize {
    final int bytes;
  
    private BufferSize(long bytes) {
      if (bytes > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Buffer too large for Java ("
            + (Integer.MAX_VALUE / MB) + "mb max): " + bytes);
      }
      
      if (bytes < ABSOLUTE_MIN_SORT_BUFFER_SIZE) {
        throw new IllegalArgumentException(MIN_BUFFER_SIZE_MSG + ": " + bytes);
      }
  
      this.bytes = (int) bytes;
    }
    
    /**
     * Creates a {@link BufferSize} in MB. The given 
     * values must be &gt; 0 and &lt; 2048.
     */
    public static BufferSize megabytes(long mb) {
      return new BufferSize(mb * MB);
    }
  
    /** 
     * Approximately half of the currently available free heap, but no less
     * than {@link #ABSOLUTE_MIN_SORT_BUFFER_SIZE}. However if current heap allocation 
     * is insufficient or if there is a large portion of unallocated heap-space available 
     * for sorting consult with max allowed heap size. 
     */
    public static BufferSize automatic() {
      Runtime rt = Runtime.getRuntime();
      
      // take sizes in "conservative" order
      final long max = rt.maxMemory(); // max allocated
      final long total = rt.totalMemory(); // currently allocated
      final long free = rt.freeMemory(); // unused portion of currently allocated
      final long totalAvailableBytes = max - total + free;
      
      // by free mem (attempting to not grow the heap for this)
      long sortBufferByteSize = free/2;
      final long minBufferSizeBytes = MIN_BUFFER_SIZE_MB*MB;
      if (sortBufferByteSize <  minBufferSizeBytes
          || totalAvailableBytes > 10 * minBufferSizeBytes) { // lets see if we need/should to grow the heap 
        if (totalAvailableBytes/2 > minBufferSizeBytes) { // there is enough mem for a reasonable buffer
          sortBufferByteSize = totalAvailableBytes/2; // grow the heap
        } else {
          //heap seems smallish lets be conservative fall back to the free/2 
          sortBufferByteSize = Math.max(ABSOLUTE_MIN_SORT_BUFFER_SIZE, sortBufferByteSize);
        }
      }
      return new BufferSize(Math.min((long)Integer.MAX_VALUE, sortBufferByteSize));
    }
  }
  
  /**
   * Sort info (debugging mostly).
   */
  public class SortInfo {
    /** number of temporary files created when merging partitions */
    public int tempMergeFiles;
    /** number of partition merges */
    public int mergeRounds;
    /** number of lines of data read */
    public int lineCount;
    /** time spent merging sorted partitions (in milliseconds) */
    public final AtomicLong mergeTimeMS = new AtomicLong();
    /** time spent sorting data (in milliseconds) */
    public final AtomicLong sortTimeMS = new AtomicLong();
    /** total time spent (in milliseconds) */
    public long totalTimeMS;
    /** time spent in i/o read (in milliseconds) */
    public long readTimeMS;
    /** read buffer size (in bytes) */
    public final long bufferSize = ramBufferSize.bytes;
    
    /** create a new SortInfo (with empty statistics) for debugging */
    public SortInfo() {}
    
    @Override
    public String toString() {
      return String.format(Locale.ROOT,
                           "time=%.2f sec. total (%.2f reading, %.2f sorting, %.2f merging), lines=%d, temp files=%d, merges=%d, soft ram limit=%.2f MB",
                           totalTimeMS / 1000.0d, readTimeMS / 1000.0d, sortTimeMS.get() / 1000.0d, mergeTimeMS.get() / 1000.0d,
                           lineCount, tempMergeFiles, mergeRounds,
                           (double) bufferSize / MB);
    }
  }

  private final BufferSize ramBufferSize;
  
  SortInfo sortInfo;
  private int maxTempFiles;
  private final Comparator<BytesRef> comparator;
  
  /** Default comparator: sorts in binary (codepoint) order */
  public static final Comparator<BytesRef> DEFAULT_COMPARATOR = Comparator.naturalOrder();

  /**
   * Defaults constructor.
   * 
   * @see BufferSize#automatic()
   */
  public OfflineSorter(Directory dir, String tempFileNamePrefix) throws IOException {
    this(dir, tempFileNamePrefix, DEFAULT_COMPARATOR, BufferSize.automatic(), MAX_TEMPFILES, -1, null, 0);
  }
  
  /**
   * Defaults constructor with a custom comparator.
   * 
   * @see BufferSize#automatic()
   */
  public OfflineSorter(Directory dir, String tempFileNamePrefix, Comparator<BytesRef> comparator) throws IOException {
    this(dir, tempFileNamePrefix, comparator, BufferSize.automatic(), MAX_TEMPFILES, -1, null, 0);
  }

  /**
   * All-details constructor.  If {@code valueLength} is -1 (the default), the length of each value differs; otherwise,
   * all values have the specified length.  If you pass a non-null {@code ExecutorService} then it will be
   * used to run sorting operations that can be run concurrently, and maxPartitionsInRAM is the maximum
   * concurrent in-memory partitions.  Thus the maximum possible RAM used by this class while sorting is
   * {@code maxPartitionsInRAM * ramBufferSize}.
   */
  public OfflineSorter(Directory dir, String tempFileNamePrefix, Comparator<BytesRef> comparator,
                       BufferSize ramBufferSize, int maxTempfiles, int valueLength, ExecutorService exec,
                       int maxPartitionsInRAM) {
    if (exec != null) {
      this.exec = exec;
      if (maxPartitionsInRAM <= 0) {
        throw new IllegalArgumentException("maxPartitionsInRAM must be > 0; got " + maxPartitionsInRAM);
      }
    } else {
      this.exec = new SameThreadExecutorService();
      maxPartitionsInRAM = 1;
    }
    this.partitionsInRAM = new Semaphore(maxPartitionsInRAM);

    if (ramBufferSize.bytes < ABSOLUTE_MIN_SORT_BUFFER_SIZE) {
      throw new IllegalArgumentException(MIN_BUFFER_SIZE_MSG + ": " + ramBufferSize.bytes);
    }
    
    if (maxTempfiles < 2) {
      throw new IllegalArgumentException("maxTempFiles must be >= 2");
    }

    if (valueLength != -1 && (valueLength == 0 || valueLength > Short.MAX_VALUE)) {
      throw new IllegalArgumentException("valueLength must be 1 .. " + Short.MAX_VALUE + "; got: " + valueLength);
    }
    
    this.valueLength = valueLength;
    this.ramBufferSize = ramBufferSize;
    this.maxTempFiles = maxTempfiles;
    this.comparator = comparator;
    this.dir = dir;
    this.tempFileNamePrefix = tempFileNamePrefix;
  }

  /** Returns the {@link Directory} we use to create temp files. */
  public Directory getDirectory() {
    return dir;
  }

  /** Returns the temp file name prefix passed to {@link Directory#createTempOutput} to generate temporary files. */
  public String getTempFileNamePrefix() {
    return tempFileNamePrefix;
  }

  /** 
   * Sort input to a new temp file, returning its name.
   */
  public String sort(String inputFileName) throws IOException {
    
    sortInfo = new SortInfo();
    long startMS = System.currentTimeMillis();

    List<Future<Partition>> segments = new ArrayList<>();
    int[] levelCounts = new int[1];

    // So we can remove any partially written temp files on exception:
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);

    boolean success = false;
    try (ByteSequencesReader is = getReader(dir.openChecksumInput(inputFileName, IOContext.READONCE), inputFileName)) {
      while (true) {
        Partition part = readPartition(is);
        if (part.count == 0) {
          if (partitionsInRAM != null) {
            partitionsInRAM.release();
          }
          assert part.exhausted;
          break;
        }

        Callable<Partition> job = new SortPartitionTask(trackingDir, part);

        segments.add(exec.submit(job));
        sortInfo.tempMergeFiles++;
        sortInfo.lineCount += part.count;
        levelCounts[0]++;

        // Handle intermediate merges; we need a while loop to "cascade" the merge when necessary:
        int mergeLevel = 0;
        while (levelCounts[mergeLevel] == maxTempFiles) {
          mergePartitions(trackingDir, segments);
          if (mergeLevel+2 > levelCounts.length) {
            levelCounts = ArrayUtil.grow(levelCounts, mergeLevel+2);
          }
          levelCounts[mergeLevel+1]++;
          levelCounts[mergeLevel] = 0;
          mergeLevel++;
        }

        if (part.exhausted) {
          break;
        }
      }
      
      // TODO: we shouldn't have to do this?  Can't we return a merged reader to
      // the caller, who often consumes the result just once, instead?

      // Merge all partitions down to 1 (basically a forceMerge(1)):
      while (segments.size() > 1) {     
        mergePartitions(trackingDir, segments);
      }

      String result;
      if (segments.isEmpty()) {
        try (IndexOutput out = trackingDir.createTempOutput(tempFileNamePrefix, "sort", IOContext.DEFAULT)) {
          // Write empty file footer
          CodecUtil.writeFooter(out);
          result = out.getName();
        }
      } else {
        result = getPartition(segments.get(0)).fileName;
      }

      // We should be explicitly removing all intermediate files ourselves unless there is an exception:
      assert trackingDir.getCreatedFiles().size() == 1 && trackingDir.getCreatedFiles().contains(result);

      sortInfo.totalTimeMS = System.currentTimeMillis() - startMS;

      CodecUtil.checkFooter(is.in);

      success = true;

      return result;

    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    } finally {
      if (success == false) {
        IOUtils.deleteFilesIgnoringExceptions(trackingDir, trackingDir.getCreatedFiles());
      }
    }
  }

  /** Called on exception, to check whether the checksum is also corrupt in this source, and add that 
   *  information (checksum matched or didn't) as a suppressed exception. */
  private void verifyChecksum(Throwable priorException, ByteSequencesReader reader) throws IOException {
    try (ChecksumIndexInput in = dir.openChecksumInput(reader.name, IOContext.READONCE)) {
      CodecUtil.checkFooter(in, priorException);
    }
  }

  /** Merge the most recent {@code maxTempFile} partitions into a new partition. */
  void mergePartitions(Directory trackingDir, List<Future<Partition>> segments) throws IOException {
    long start = System.currentTimeMillis();
    List<Future<Partition>> segmentsToMerge;
    if (segments.size() > maxTempFiles) {
      segmentsToMerge = segments.subList(segments.size() - maxTempFiles, segments.size());
    } else {
      segmentsToMerge = segments;
    }

    sortInfo.mergeRounds++;

    MergePartitionsTask task = new MergePartitionsTask(trackingDir, new ArrayList<>(segmentsToMerge));

    segmentsToMerge.clear();
    segments.add(exec.submit(task));

    sortInfo.tempMergeFiles++;
  }

  /** Holds one partition of items, either loaded into memory or based on a file. */
  private static class Partition {
    public final SortableBytesRefArray buffer;
    public final boolean exhausted;
    public final long count;
    public final String fileName;

    /** A partition loaded into memory. */
    public Partition(SortableBytesRefArray buffer, boolean exhausted) {
      this.buffer = buffer;
      this.fileName = null;
      this.count = buffer.size();
      this.exhausted = exhausted;
    }

    /** An on-disk partition. */
    public Partition(String fileName, long count) {
      this.buffer = null;
      this.fileName = fileName;
      this.count = count;
      this.exhausted = true;
    }
  }

  /** Read in a single partition of data, setting isExhausted[0] to true if there are no more items. */
  Partition readPartition(ByteSequencesReader reader) throws IOException, InterruptedException {
    if (partitionsInRAM != null) {
      partitionsInRAM.acquire();
    }
    boolean success = false;
    try {
      long start = System.currentTimeMillis();
      SortableBytesRefArray buffer;
      boolean exhausted = false;
      int count;
      if (valueLength != -1) {
        // fixed length case
        buffer = new FixedLengthBytesRefArray(valueLength);
        int limit = ramBufferSize.bytes / valueLength;
        for(int i=0;i<limit;i++) {
          BytesRef item = null;
          try {
            item = reader.next();
          } catch (Throwable t) {
            verifyChecksum(t, reader);
          }
          if (item == null) {
            exhausted = true;
            break;
          }
          buffer.append(item);
        }
      } else {
        Counter bufferBytesUsed = Counter.newCounter();
        buffer = new BytesRefArray(bufferBytesUsed);
        while (true) {
          BytesRef item = null;
          try {
            item = reader.next();
          } catch (Throwable t) {
            verifyChecksum(t, reader);
          }
          if (item == null) {
            exhausted = true;
            break;
          }
          buffer.append(item);
          // Account for the created objects.
          // (buffer slots do not account to buffer size.) 
          if (bufferBytesUsed.get() > ramBufferSize.bytes) {
            break;
          }
        }
      }
      sortInfo.readTimeMS += System.currentTimeMillis() - start;
      success = true;
      return new Partition(buffer, exhausted);
    } finally {
      if (success == false && partitionsInRAM != null) {
        partitionsInRAM.release();
      }
    }
  }

  static class FileAndTop {
    final int fd;
    BytesRef current;

    FileAndTop(int fd, BytesRef firstLine) {
      this.fd = fd;
      this.current = firstLine;
    }
  }

  /** Subclasses can override to change how byte sequences are written to disk. */
  protected ByteSequencesWriter getWriter(IndexOutput out, long itemCount) throws IOException {
    return new ByteSequencesWriter(out);
  }

  /** Subclasses can override to change how byte sequences are read from disk. */
  protected ByteSequencesReader getReader(ChecksumIndexInput in, String name) throws IOException {
    return new ByteSequencesReader(in, name);
  }

  /**
   * Utility class to emit length-prefixed byte[] entries to an output stream for sorting.
   * Complementary to {@link ByteSequencesReader}.  You must use {@link CodecUtil#writeFooter}
   * to write a footer at the end of the input file.
   */
  public static class ByteSequencesWriter implements Closeable {
    protected final IndexOutput out;

    // TODO: this should optimize the fixed width case as well

    /** Constructs a ByteSequencesWriter to the provided DataOutput */
    public ByteSequencesWriter(IndexOutput out) {
      this.out = out;
    }

    /**
     * Writes a BytesRef.
     * @see #write(byte[], int, int)
     */
    public final void write(BytesRef ref) throws IOException {
      assert ref != null;
      write(ref.bytes, ref.offset, ref.length);
    }

    /**
     * Writes a byte array.
     * @see #write(byte[], int, int)
     */
    public final void write(byte[] bytes) throws IOException {
      write(bytes, 0, bytes.length);
    }

    /**
     * Writes a byte array.
     * <p>
     * The length is written as a <code>short</code>, followed
     * by the bytes.
     */
    public void write(byte[] bytes, int off, int len) throws IOException {
      assert bytes != null;
      assert off >= 0 && off + len <= bytes.length;
      assert len >= 0;
      if (len > Short.MAX_VALUE) {
        throw new IllegalArgumentException("len must be <= " + Short.MAX_VALUE + "; got " + len);
      }
      out.writeShort((short) len);
      out.writeBytes(bytes, off, len);
    }
    
    /**
     * Closes the provided {@link IndexOutput}.
     */
    @Override
    public void close() throws IOException {
      out.close();
    }    
  }

  /**
   * Utility class to read length-prefixed byte[] entries from an input.
   * Complementary to {@link ByteSequencesWriter}.
   */
  public static class ByteSequencesReader implements BytesRefIterator, Closeable {
    protected final String name;
    protected final ChecksumIndexInput in;
    protected final long end;
    private final BytesRefBuilder ref = new BytesRefBuilder();

    /** Constructs a ByteSequencesReader from the provided IndexInput */
    public ByteSequencesReader(ChecksumIndexInput in, String name) {
      this.in = in;
      this.name = name;
      end = in.length() - CodecUtil.footerLength();
    }

    /**
     * Reads the next entry into the provided {@link BytesRef}. The internal
     * storage is resized if needed.
     * 
     * @return Returns <code>false</code> if EOF occurred when trying to read
     * the header of the next sequence. Returns <code>true</code> otherwise.
     * @throws EOFException if the file ends before the full sequence is read.
     */
    public BytesRef next() throws IOException {
      if (in.getFilePointer() >= end) {
        return null;
      }

      short length = in.readShort();
      ref.grow(length);
      ref.setLength(length);
      in.readBytes(ref.bytes(), 0, length);
      return ref.get();
    }

    /**
     * Closes the provided {@link IndexInput}.
     */
    @Override
    public void close() throws IOException {
      in.close();
    }
  }

  /** Returns the comparator in use to sort entries */
  public Comparator<BytesRef> getComparator() {
    return comparator;
  }

  /** Sorts one in-memory partition, writes it to disk, and returns the resulting file-based partition. */
  private class SortPartitionTask implements Callable<Partition> {

    private final Directory dir;
    private final Partition part;
      
    public SortPartitionTask(Directory dir, Partition part) {
      this.dir = dir;
      this.part = part;
    }
    
    @Override
    public Partition call() throws IOException {
      try (IndexOutput tempFile = dir.createTempOutput(tempFileNamePrefix, "sort", IOContext.DEFAULT);
           ByteSequencesWriter out = getWriter(tempFile, part.buffer.size());) {
      
        BytesRef spare;

        long startMS = System.currentTimeMillis();
        BytesRefIterator iter = part.buffer.iterator(comparator);
        sortInfo.sortTimeMS.addAndGet(System.currentTimeMillis() - startMS);

        int count = 0;
        while ((spare = iter.next()) != null) {
          out.write(spare);
          count++;
        }

        assert count == part.count;

        CodecUtil.writeFooter(out.out);
        part.buffer.clear();

        return new Partition(tempFile.getName(), part.count);
      } finally {
        if (partitionsInRAM != null) {
          partitionsInRAM.release();
        }
      }
    }
  }

  private Partition getPartition(Future<Partition> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    } catch (ExecutionException ee) {
      // Theoretically cause can be null; guard against that.
      Throwable cause = ee.getCause();
      throw IOUtils.rethrowAlways(cause != null ? cause : ee);
    }
  }

  /** Merges multiple file-based partitions to a single on-disk partition. */
  private class MergePartitionsTask implements Callable<Partition> {
    private final Directory dir;
    private final List<Future<Partition>> segmentsToMerge;
    
    public MergePartitionsTask(Directory dir, List<Future<Partition>> segmentsToMerge) {
      this.dir = dir;
      this.segmentsToMerge = segmentsToMerge;
    }

    @Override
    public Partition call() throws IOException {
      long totalCount = 0;
      for (Future<Partition> segment : segmentsToMerge) {
        totalCount += getPartition(segment).count;
      }

      PriorityQueue<FileAndTop> queue = new PriorityQueue<FileAndTop>(segmentsToMerge.size()) {
          @Override
          protected boolean lessThan(FileAndTop a, FileAndTop b) {
            return comparator.compare(a.current, b.current) < 0;
          }
        };

      ByteSequencesReader[] streams = new ByteSequencesReader[segmentsToMerge.size()];

      String newSegmentName = null;

      long startMS = System.currentTimeMillis();
      try (ByteSequencesWriter writer = getWriter(dir.createTempOutput(tempFileNamePrefix, "sort", IOContext.DEFAULT), totalCount)) {

        newSegmentName = writer.out.getName();
      
        // Open streams and read the top for each file
        for (int i = 0; i < segmentsToMerge.size(); i++) {
          Partition segment = getPartition(segmentsToMerge.get(i));
          streams[i] = getReader(dir.openChecksumInput(segment.fileName, IOContext.READONCE), segment.fileName);
              
          BytesRef item = null;
          try {
            item = streams[i].next();
          } catch (Throwable t) {
            verifyChecksum(t, streams[i]);
          }
          assert item != null;
          queue.insertWithOverflow(new FileAndTop(i, item));
        }
  
        // Unix utility sort() uses ordered array of files to pick the next line from, updating
        // it as it reads new lines. The PQ used here is a more elegant solution and has 
        // a nicer theoretical complexity bound :) The entire sorting process is I/O bound anyway
        // so it shouldn't make much of a difference (didn't check).
        FileAndTop top;
        while ((top = queue.top()) != null) {
          writer.write(top.current);
          try {
            top.current = streams[top.fd].next();
          } catch (Throwable t) {
            verifyChecksum(t, streams[top.fd]);
          }

          if (top.current != null) {
            queue.updateTop();
          } else {
            queue.pop();
          }
        }

        CodecUtil.writeFooter(writer.out);

        for(ByteSequencesReader reader : streams) {
          CodecUtil.checkFooter(reader.in);
        }

        sortInfo.mergeTimeMS.addAndGet(System.currentTimeMillis() - startMS);
      } finally {
        IOUtils.close(streams);
      }
      List<String> toDelete = new ArrayList<>();
      for (Future<Partition> segment : segmentsToMerge) {
        toDelete.add(getPartition(segment).fileName);
      }
      IOUtils.deleteFiles(dir, toDelete);

      return new Partition(newSegmentName, totalCount);
    }
  }
}
