package org.apache.lucene.search.suggest.fst;

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

import java.io.*;
import java.util.*;

import org.apache.lucene.util.*;
import org.apache.lucene.util.PriorityQueue;

// TODO: the buffer is currently byte[][] which with very small arrays will terribly overallocate
// memory (alignments) and make GC very happy.
// 
// We could move it to a single byte[] + and use custom sorting, but we'd need to check if this
// yields any improvement first.

/**
 * On-disk sorting of byte arrays. Each byte array (entry) is a composed of the following
 * fields:
 * <ul>
 *   <li>(two bytes) length of the following byte array,
 *   <li>exactly the above count of bytes for the sequence to be sorted.
 * </ul>
 * 
 * @see #sort(File, File)
 */
public final class Sort {
  public final static int MB = 1024 * 1024;
  public final static int GB = MB * 1024;
  
  /**
   * Minimum recommended buffer size for sorting.
   */
  public final static int MIN_BUFFER_SIZE_MB = 32;

  /**
   * Absolute minimum required buffer size for sorting.
   */
  public static final int ABSOLUTE_MIN_SORT_BUFFER_SIZE = MB / 2;
  private static final String MIN_BUFFER_SIZE_MSG = "At least 0.5MB RAM buffer is needed";

  /**
   * Maximum number of temporary files before doing an intermediate merge.
   */
  public final static int MAX_TEMPFILES = 128;

  /**
   * Minimum slot buffer expansion.
   */
  private final static int MIN_EXPECTED_GROWTH = 1000;

  /** 
   * A bit more descriptive unit for constructors.
   * 
   * @see #automatic()
   * @see #megabytes(int)
   */
  public static final class BufferSize {
    final int bytes;
  
    private BufferSize(long bytes) {
      if (bytes > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Buffer too large for Java ("
            + (Integer.MAX_VALUE / MB) + "mb max): " + bytes);
      }
  
      this.bytes = (int) bytes;
    }
  
    public static BufferSize megabytes(int mb) {
      return new BufferSize(mb * MB);
    }
  
    /** 
     * Approximately half of the currently available free heap, but no less
     * than {@link #MIN_BUFFER_SIZE_MB}. However if current heap allocation 
     * is insufficient for sorting consult with max allowed heap size. 
     */
    public static BufferSize automatic() {
      Runtime rt = Runtime.getRuntime();
      
      // take sizes in "conservative" order
      long max = rt.maxMemory();
      long total = rt.totalMemory();
      long free = rt.freeMemory();

      // by free mem (attempting to not grow the heap for this)
      long half = free/2;
      if (half >= ABSOLUTE_MIN_SORT_BUFFER_SIZE) { 
        return new BufferSize(Math.min(MIN_BUFFER_SIZE_MB * MB, half));
      }
      
      // by max mem (heap will grow)
      half = (max - total) / 2;
      return new BufferSize(Math.min(MIN_BUFFER_SIZE_MB * MB, half));
    }
  }

  /**
   * byte[] in unsigned byte order.
   */
  static final Comparator<byte[]> unsignedByteOrderComparator = new Comparator<byte[]>() {
    public int compare(byte[] left, byte[] right) {
      final int max = Math.min(left.length, right.length);
      for (int i = 0, j = 0; i < max; i++, j++) {
        int diff = (left[i]  & 0xff) - (right[j] & 0xff); 
        if (diff != 0) 
          return diff;
      }
      return left.length - right.length;
    }
  };

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

  private final static byte [][] EMPTY = new byte [0][];

  private final BufferSize ramBufferSize;
  private final File tempDirectory;

  private byte [][] buffer = new byte [0][];
  private SortInfo sortInfo;
  private int maxTempFiles;

  /**
   * Defaults constructor.
   * 
   * @see #defaultTempDir()
   * @see BufferSize#automatic()
   */
  public Sort() throws IOException {
    this(BufferSize.automatic(), defaultTempDir(), MAX_TEMPFILES);
  }

  /**
   * All-details constructor.
   */
  public Sort(BufferSize ramBufferSize, File tempDirectory, int maxTempfiles) {
    if (ramBufferSize.bytes < ABSOLUTE_MIN_SORT_BUFFER_SIZE) {
      throw new IllegalArgumentException(MIN_BUFFER_SIZE_MSG + ": " + ramBufferSize.bytes);
    }
    
    if (maxTempfiles < 2) {
      throw new IllegalArgumentException("maxTempFiles must be >= 2");
    }

    this.ramBufferSize = ramBufferSize;
    this.tempDirectory = tempDirectory;
    this.maxTempFiles = maxTempfiles;
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
    byte [][] data = this.buffer;
    File tempFile = File.createTempFile("sort", "partition", tempDirectory);

    long start = System.currentTimeMillis();
    Arrays.sort(data, 0, len, unsignedByteOrderComparator);
    sortInfo.sortTime += (System.currentTimeMillis() - start);
    
    ByteSequencesWriter out = new ByteSequencesWriter(tempFile);
    try {
      for (int i = 0; i < len; i++) {
        assert data[i].length <= Short.MAX_VALUE;
        out.write(data[i]);
      }
      out.close();

      // Clean up the buffer for the next partition.
      this.buffer = EMPTY;
      return tempFile;
    } finally {
      IOUtils.close(out);
    }
  }

  /** Merge a list of sorted temporary files (partitions) into an output file */
  void mergePartitions(List<File> merges, File outputFile) throws IOException {
    long start = System.currentTimeMillis();

    ByteSequencesWriter out = new ByteSequencesWriter(outputFile);

    PriorityQueue<FileAndTop> queue = new PriorityQueue<FileAndTop>(merges.size()) {
      protected boolean lessThan(FileAndTop a, FileAndTop b) {
        return a.current.compareTo(b.current) < 0;
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

    // We will be reallocating from scratch.
    Arrays.fill(this.buffer, null);

    int bytesLimit = this.ramBufferSize.bytes;
    byte [][] data = this.buffer;
    byte[] line;
    int linesRead = 0;
    while ((line = reader.read()) != null) {
      if (linesRead + 1 >= data.length) {
        data = Arrays.copyOf(data,
            ArrayUtil.oversize(linesRead + MIN_EXPECTED_GROWTH, 
                RamUsageEstimator.NUM_BYTES_OBJECT_REF));
      }
      data[linesRead++] = line;

      // Account for the created objects.
      // (buffer slots do not account to buffer size.) 
      bytesLimit -= line.length + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
      if (bytesLimit < 0) {
        break;
      }
    }
    this.buffer = data;

    sortInfo.readTime += (System.currentTimeMillis() - start);
    return linesRead;
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
    @Override
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
    @Override
    public void close() throws IOException {
      if (is instanceof Closeable) {
        ((Closeable) is).close();
      }
    }
  }  
}