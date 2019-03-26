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
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.CRC32;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.util.BitUtil;

/**
 * A {@link ByteBuffer}-based {@link Directory} implementation that
 * can be used to store index files on the heap.
 *
 * <p>Important: Note that {@link MMapDirectory} is nearly always a better choice as
 * it uses OS caches more effectively (through memory-mapped buffers).
 * A heap-based directory like this one can have the advantage in case of ephemeral, small,
 * short-lived indexes when disk syncs provide an additional overhead.</p>
 *
 * @lucene.experimental
 */
public final class ByteBuffersDirectory extends BaseDirectory {
  public static final BiFunction<String, ByteBuffersDataOutput, IndexInput> OUTPUT_AS_MANY_BUFFERS = 
      (fileName, output) -> {
        ByteBuffersDataInput dataInput = output.toDataInput();
        String inputName = String.format(Locale.ROOT, "%s (file=%s, buffers=%s)",
            ByteBuffersIndexInput.class.getSimpleName(),
            fileName,
            dataInput.toString());
        return new ByteBuffersIndexInput(dataInput, inputName);
      };

  public static final BiFunction<String, ByteBuffersDataOutput, IndexInput> OUTPUT_AS_ONE_BUFFER = 
      (fileName, output) -> {
        ByteBuffersDataInput dataInput = new ByteBuffersDataInput(Arrays.asList(ByteBuffer.wrap(output.toArrayCopy())));
        String inputName = String.format(Locale.ROOT, "%s (file=%s, buffers=%s)",
            ByteBuffersIndexInput.class.getSimpleName(),
            fileName,
            dataInput.toString());
        return new ByteBuffersIndexInput(dataInput, inputName);
      };

  public static final BiFunction<String, ByteBuffersDataOutput, IndexInput> OUTPUT_AS_BYTE_ARRAY = OUTPUT_AS_ONE_BUFFER;

  public static final BiFunction<String, ByteBuffersDataOutput, IndexInput> OUTPUT_AS_MANY_BUFFERS_LUCENE = 
      (fileName, output) -> {
        List<ByteBuffer> bufferList = output.toBufferList();
        bufferList.add(ByteBuffer.allocate(0));

        int chunkSizePower;
        int blockSize = ByteBuffersDataInput.determineBlockPage(bufferList);
        if (blockSize == 0) {
          chunkSizePower = 30;
        } else {
          chunkSizePower = Integer.numberOfTrailingZeros(BitUtil.nextHighestPowerOfTwo(blockSize));
        }

        String inputName = String.format(Locale.ROOT, "%s (file=%s)",
            ByteBuffersDirectory.class.getSimpleName(),
            fileName);

        ByteBufferGuard guard = new ByteBufferGuard("none", (String resourceDescription, ByteBuffer b) -> {});
        return ByteBufferIndexInput.newInstance(inputName,
            bufferList.toArray(new ByteBuffer [bufferList.size()]),
            output.size(), chunkSizePower, guard);
      };

  private final Function<String, String> tempFileName = new Function<String, String>() {
    private final AtomicLong counter = new AtomicLong();

    @Override
    public String apply(String suffix) {
      return suffix + "_" + Long.toString(counter.getAndIncrement(), Character.MAX_RADIX);
    }
  };

  private final ConcurrentHashMap<String, FileEntry> files = new ConcurrentHashMap<>();

  /**
   * Conversion between a buffered index output and the corresponding index input
   * for a given file.   
   */
  private final BiFunction<String, ByteBuffersDataOutput, IndexInput> outputToInput;

  /**
   * A supplier of {@link ByteBuffersDataOutput} instances used to buffer up 
   * the content of written files.
   */
  private final Supplier<ByteBuffersDataOutput> bbOutputSupplier;

  public ByteBuffersDirectory() {
    this(new SingleInstanceLockFactory());
  }
  
  public ByteBuffersDirectory(LockFactory lockFactory) {
    this(lockFactory, ByteBuffersDataOutput::new, OUTPUT_AS_MANY_BUFFERS);
  }

  public ByteBuffersDirectory(LockFactory factory, 
      Supplier<ByteBuffersDataOutput> bbOutputSupplier,
      BiFunction<String, ByteBuffersDataOutput, IndexInput> outputToInput) {
    super(factory);
    this.outputToInput = Objects.requireNonNull(outputToInput);
    this.bbOutputSupplier = Objects.requireNonNull(bbOutputSupplier);
  }

  @Override
  public String[] listAll() throws IOException {
    ensureOpen();
    return files.keySet().stream().sorted().toArray(String[]::new);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    FileEntry removed = files.remove(name);
    if (removed == null) {
      throw new NoSuchFileException(name);
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    FileEntry file = files.get(name);
    if (file == null) {
      throw new NoSuchFileException(name);
    }
    return file.length();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    FileEntry e = new FileEntry(name); 
    if (files.putIfAbsent(name, e) != null) {
      throw new FileAlreadyExistsException("File already exists: " + name);
    }
    return e.createOutput(outputToInput);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    ensureOpen();
    while (true) {
      String name = IndexFileNames.segmentFileName(prefix, tempFileName.apply(suffix), "tmp");
      FileEntry e = new FileEntry(name); 
      if (files.putIfAbsent(name, e) == null) {
        return e.createOutput(outputToInput);
      }
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    ensureOpen();

    FileEntry file = files.get(source);
    if (file == null) {
      throw new NoSuchFileException(source);
    }
    if (files.putIfAbsent(dest, file) != null) {
      throw new FileAlreadyExistsException(dest);
    }
    if (!files.remove(source, file)) {
      throw new IllegalStateException("File was unexpectedly replaced: " + source);
    }
    files.remove(source);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    ensureOpen();
  }

  @Override
  public void syncMetaData() throws IOException {
    ensureOpen();
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    FileEntry e = files.get(name);
    if (e == null) {
      throw new NoSuchFileException(name);
    } else {
      return e.openInput();
    }
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
    files.clear();
  }

  @Override
  public Set<String> getPendingDeletions() {
    return Collections.emptySet();
  }

  private final class FileEntry {
    private final String fileName;

    private volatile IndexInput content;
    private volatile long cachedLength;

    public FileEntry(String name) {
      this.fileName = name;
    }

    public long length() {
      // We return 0 length until the IndexOutput is closed and flushed.
      return cachedLength;
    }

    public IndexInput openInput() throws IOException {
      IndexInput local = this.content;
      if (local == null) {
        throw new AccessDeniedException("Can't open a file still open for writing: " + fileName);
      }

      return local.clone();
    }

    final IndexOutput createOutput(BiFunction<String, ByteBuffersDataOutput, IndexInput> outputToInput) throws IOException {
      if (content != null) {
        throw new IOException("Can only write to a file once: " + fileName);
      }

      String clazzName = ByteBuffersDirectory.class.getSimpleName();
      String outputName = String.format(Locale.ROOT, "%s output (file=%s)", clazzName, fileName);

      return new ByteBuffersIndexOutput(
          bbOutputSupplier.get(), outputName, fileName,
          new CRC32(),
          (output) -> {
            content = outputToInput.apply(fileName, output);
            cachedLength = output.size();
          });
    }
  }
}
