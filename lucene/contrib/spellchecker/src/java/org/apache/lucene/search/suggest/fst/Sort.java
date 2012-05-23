package org.apache.lucene.search.suggest.fst;

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

import java.io.*;
import java.util.*;

import org.apache.lucene.search.suggest.BytesRefList;
import org.apache.lucene.util.*;
import org.apache.lucene.util.PriorityQueue;

/**
 * On-disk sorting of byte arrays. Each byte array (entry) is a composed of the following
 * fields:
 * <ul>
 *   <li>(two bytes) length of the following byte array,
 *   <li>exactly the above count of bytes for the sequence to be sorted.
 * </ul>
 * 
 * @see #sort(File, File)
 * @lucene.experimental
 * @lucene.internal
 */
public final class Sort {
  public final static long MB = 1024 * 1024;
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
  public final static int MAX_TEMPFILES = 128;

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
     * values must be $gt; 0 and &lt; 2048.
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
        if (totalAvailableBytes/2 > minBufferSizeBytes){ // there is enough mem for a reasonable buffer
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
    public int tempMergeFiles;
    public int mergeRounds;
    public int lines;
    public long mergeTime;
    public long sortTime;
    public long totalTime;
    public long readTime;
    public final long bufferSize = ramBufferSize.bytes;
    
    @Override
    public String toString() {
      return String.format(Locale.ENGLISH,
          "time=%.2f sec. total (%.2f reading, %.2f sorting, %.2f merging), lines=%d, temp files=%d, merges=%d, soft ram limit=%.2f MB",
          totalTime / 1000.0d, readTime / 1000.0d, sortTime / 1000.0d, mergeTime / 1000.0d,
          lines, tempMergeFiles, mergeRounds,
          (double) bufferSize / MB);
    }
  }

  private final BufferSize ramBufferSize;
  private final File tempDirectory;
  
  private final BytesRefList buffer = new BytesRefList();
  private SortInfo sortInfo;
  private int maxTempFiles;
  private final Comparator<BytesRef> comparator;
  
  public static final Comparator<BytesRef> DEFAULT_COMPARATOR = BytesRef.getUTF8SortedAsUnicodeComparator();

  /**
   * Defaults constructor.
   * 
   * @see #defaultTempDir()
   * @see BufferSize#automatic()
   */
  public Sort() throws IOException {
    this(DEFAULT_COMPARATOR, BufferSize.automatic(), defaultTempDir(), MAX_TEMPFILES);
  }
  
  public Sort(Comparator<BytesRef> comparator) throws IOException {
    this(comparator, BufferSize.automatic(), defaultTempDir(), MAX_TEMPFILES);
  }

  /**
   * All-details constructor.
   */
  public Sort(Comparator<BytesRef> comparator, BufferSize ramBufferSize, File tempDirectory, int maxTempfiles) {
    if (ramBufferSize.bytes < ABSOLUTE_MIN_SORT_BUFFER_SIZE) {
      throw new IllegalArgumentException(MIN_BUFFER_SIZE_MSG + ": " + ramBufferSize.bytes);
    }
    
    if (maxTempfiles < 2) {
      throw new IllegalArgumentException("maxTempFiles must be >= 2");
    }

    this.ramBufferSize = ramBufferSize;
    this.tempDirectory = tempDirectory;
    this.maxTempFiles = maxTempfiles;
    this.comparator = comparator;
  }

  /** 
   * Sort input to output, explicit hint for the buffer size. The amount of allocated
   * memory may deviate from the hint (may be smaller or larger).  
   */
  public SortInfo sort(File input, File output) throws IOException {
    sortInfo = new SortInfo();
    sortInfo.totalTime = System.currentTimeMillis();

    output.delete();

    ArrayList<File> merges = new ArrayList<File>();
    ByteSequencesReader is = new ByteSequencesReader(input);
    boolean success = false;
    try {
      int lines = 0;
      while ((lines = readPartition(is)) > 0) {                    
        merges.add(sortPartition(lines));
        sortInfo.tempMergeFiles++;
        sortInfo.lines += lines;

        // Handle intermediate merges.
        if (merges.size() == maxTempFiles) {
          File intermediate = File.createTempFile("sort", "intermediate", tempDirectory);
          mergePartitions(merges, intermediate);
          for (File file : merges) {
            file.delete();
          }
          merges.clear();
          merges.add(intermediate);
          sortInfo.tempMergeFiles++;
        }
      }
      success = true;
    } finally {
      if (success)
        IOUtils.close(is);
      else
        IOUtils.closeWhileHandlingException(is);
    }

    // One partition, try to rename or copy if unsuccessful.
    if (merges.size() == 1) {     
      // If simple rename doesn't work this means the output is
      // on a different volume or something. Copy the input then.
      if (!merges.get(0).renameTo(output)) {
        copy(merges.get(0), output);
      }
    } else { 
      // otherwise merge the partitions with a priority queue.                  
      mergePartitions(merges, output);                            
      for (File file : merges) {
        file.delete();
      }
    }

    sortInfo.totalTime = (System.currentTimeMillis() - sortInfo.totalTime); 
    return sortInfo;
  }

  /**
   * Returns the default temporary directory. By default, java.io.tmpdir. If not accessible
   * or not available, an IOException is thrown
   */
  public static File defaultTempDir() throws IOException {
    String tempDirPath = System.getProperty("java.io.tmpdir");
    if (tempDirPath == null) 
      throw new IOException("Java has no temporary folder property (java.io.tmpdir)?");

    File tempDirectory = new File(tempDirPath);
    if (!tempDirectory.exists() || !tempDirectory.canWrite()) {
      throw new IOException("Java's temporary folder not present or writeable?: " 
          + tempDirectory.getAbsolutePath());
    }
    return tempDirectory;
  }

  /**
   * Copies one file to another.
   */
  private static void copy(File file, File output) throws IOException {
    // 64kb copy buffer (empirical pick).
    byte [] buffer = new byte [16 * 1024];
    InputStream is = null;
    OutputStream os = null;
    try {
      is = new FileInputStream(file);
      os = new FileOutputStream(output);
      int length;
      while ((length = is.read(buffer)) > 0) {
        os.write(buffer, 0, length);
      }
    } finally {
      IOUtils.close(is, os);
    }
  }

  /** Sort a single partition in-memory. */
  protected File sortPartition(int len) throws IOException {
    BytesRefList data = this.buffer;
    File tempFile = File.createTempFile("sort", "partition", tempDirectory);

    long start = System.currentTimeMillis();
    sortInfo.sortTime += (System.currentTimeMillis() - start);
    
    final ByteSequencesWriter out = new ByteSequencesWriter(tempFile);
    BytesRef spare;
    try {
      BytesRefIterator iter = buffer.iterator(comparator);
      while((spare = iter.next()) != null) {
        assert spare.length <= Short.MAX_VALUE;
        out.write(spare);
      }
      
      out.close();

      // Clean up the buffer for the next partition.
      data.clear();
      return tempFile;
    } finally {
      IOUtils.close(out);
    }
  }

  /** Merge a list of sorted temporary files (partitions) into an output file */
  void mergePartitions(List<File> merges, File outputFile) throws IOException {
    long start = System.currentTimeMillis();

    ByteSequencesWriter out = new ByteSequencesWriter(outputFile);
    final int size = merges.size();
    PriorityQueue<FileAndTop> queue = new PriorityQueue<FileAndTop>() {
      {
        initialize(size);
      }
      protected boolean lessThan(FileAndTop a, FileAndTop b) {
        return comparator.compare(a.current, b.current) < 0;
      }
    };

    ByteSequencesReader [] streams = new ByteSequencesReader [merges.size()];
    try {
      // Open streams and read the top for each file
      for (int i = 0; i < merges.size(); i++) {
        streams[i] = new ByteSequencesReader(merges.get(i));
        byte line[] = streams[i].read();
        if (line != null) {
          queue.insertWithOverflow(new FileAndTop(i, line));
        }
      }
  
      // Unix utility sort() uses ordered array of files to pick the next line from, updating
      // it as it reads new lines. The PQ used here is a more elegant solution and has 
      // a nicer theoretical complexity bound :) The entire sorting process is I/O bound anyway
      // so it shouldn't make much of a difference (didn't check).
      FileAndTop top;
      while ((top = queue.top()) != null) {
        out.write(top.current);
        if (!streams[top.fd].read(top.current)) {
          queue.pop();
        } else {
          queue.updateTop();
        }
      }
  
      sortInfo.mergeTime += System.currentTimeMillis() - start;
      sortInfo.mergeRounds++;
    } finally {
      // The logic below is: if an exception occurs in closing out, it has a priority over exceptions
      // happening in closing streams.
      try {
        IOUtils.close(streams);
      } finally {
        IOUtils.close(out);
      }
    }
  }

  /** Read in a single partition of data */
  int readPartition(ByteSequencesReader reader) throws IOException {
    long start = System.currentTimeMillis();
    final BytesRef scratch = new BytesRef();
    while ((scratch.bytes = reader.read()) != null) {
      scratch.length = scratch.bytes.length; 
      buffer.append(scratch);
      // Account for the created objects.
      // (buffer slots do not account to buffer size.) 
      if (ramBufferSize.bytes < buffer.bytesUsed()) {
        break;
      }
    }
    sortInfo.readTime += (System.currentTimeMillis() - start);
    return buffer.size();
  }

  static class FileAndTop {
    final int fd;
    final BytesRef current;

    FileAndTop(int fd, byte [] firstLine) {
      this.fd = fd;
      this.current = new BytesRef(firstLine);
    }
  }

  /**
   * Utility class to emit length-prefixed byte[] entries to an output stream for sorting.
   * Complementary to {@link ByteSequencesReader}.
   */
  public static class ByteSequencesWriter implements Closeable {
    private final DataOutput os;

    public ByteSequencesWriter(File file) throws IOException {
      this(new DataOutputStream(
          new BufferedOutputStream(
              new FileOutputStream(file))));
    }

    public ByteSequencesWriter(DataOutput os) {
      this.os = os;
    }

    public void write(BytesRef ref) throws IOException {
      assert ref != null;
      write(ref.bytes, ref.offset, ref.length);
    }

    public void write(byte [] bytes) throws IOException {
      write(bytes, 0, bytes.length);
    }

    public void write(byte [] bytes, int off, int len) throws IOException {
      assert bytes != null;
      assert off >= 0 && off + len <= bytes.length;
      assert len >= 0;
      os.writeShort(len);
      os.write(bytes, off, len);
    }        
    
    /**
     * Closes the provided {@link DataOutput} if it is {@link Closeable}.
     */
    //@Override - not until Java 6
    public void close() throws IOException {
      if (os instanceof Closeable) {
        ((Closeable) os).close();
      }
    }    
  }

  /**
   * Utility class to read length-prefixed byte[] entries from an input.
   * Complementary to {@link ByteSequencesWriter}.
   */
  public static class ByteSequencesReader implements Closeable {
    private final DataInput is;

    public ByteSequencesReader(File file) throws IOException {
      this(new DataInputStream(
          new BufferedInputStream(
              new FileInputStream(file))));
    }

    public ByteSequencesReader(DataInput is) {
      this.is = is;
    }

    /**
     * Reads the next entry into the provided {@link BytesRef}. The internal
     * storage is resized if needed.
     * 
     * @return Returns <code>false</code> if EOF occurred when trying to read
     * the header of the next sequence. Returns <code>true</code> otherwise.
     * @throws EOFException if the file ends before the full sequence is read.
     */
    public boolean read(BytesRef ref) throws IOException {
      short length;
      try {
        length = is.readShort();
      } catch (EOFException e) {
        return false;
      }

      ref.grow(length);
      ref.offset = 0;
      ref.length = length;
      is.readFully(ref.bytes, 0, length);
      return true;
    }

    /**
     * Reads the next entry and returns it if successful.
     * 
     * @see #read(BytesRef)
     * 
     * @return Returns <code>null</code> if EOF occurred before the next entry
     * could be read.
     * @throws EOFException if the file ends before the full sequence is read.
     */
    public byte[] read() throws IOException {
      short length;
      try {
        length = is.readShort();
      } catch (EOFException e) {
        return null;
      }

      assert length >= 0 : "Sanity: sequence length < 0: " + length;
      byte [] result = new byte [length];
      is.readFully(result);
      return result;
    }

    /**
     * Closes the provided {@link DataInput} if it is {@link Closeable}.
     */
    //@Override - not until Java 6
    public void close() throws IOException {
      if (is instanceof Closeable) {
        ((Closeable) is).close();
      }
    }
  }

  public Comparator<BytesRef> getComparator() {
    return comparator;
  }  
}
