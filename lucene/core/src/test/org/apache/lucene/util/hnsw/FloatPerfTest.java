package org.apache.lucene.util.hnsw;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

class FloatPerfTest {

  static final int COUNT = 1_000_000;
  //static final int COUNT = 1;
  static final int NUM_VECTORS = 1_000_000;
  static final int NUM_ITERS = 5;

  final long seed;

  Random r = new Random();
  Path filePath;
  int dim;
  int j0, k0;
  int curJ, curK;
  double total;
  long startNanos;

  public static void main(String[] args) throws IOException {
    FloatPerfTest tester = new FloatPerfTest(Paths.get(args[0]), Integer.parseInt(args[1]));
    tester.run();
  }

  FloatPerfTest(Path filePath, int dim) {
    this.filePath = filePath;
    this.dim = dim;
    j0 = r.nextInt(NUM_VECTORS);
    k0 = r.nextInt(NUM_VECTORS);
    seed = r.nextLong();
  }

  void reset() {
    curJ = j0;
    curK = k0;
    r = new Random(seed);
  }

  void run() throws IOException {
    reset();
    test0();

    reset();
    test1();
    //tester.test4();
    //tester.test5();

    reset();
    test2();

    //tester.test2b();

    reset();
    test3();
  }

  int nextJx() {
    curJ = (curJ + 1) % NUM_VECTORS;
    return curJ;
  }

  int nextKu() {
    curK = (curK + 1) % NUM_VECTORS;
    return curK;
  }

  int nextJ() {
    return r.nextInt(NUM_VECTORS);
  }

  int nextK() {
    return r.nextInt(NUM_VECTORS);
  }

  int nextJxx() {
    curJ = (curJ + 37) % NUM_VECTORS;
    return curJ;
  }

  int nextKxx() {
    curK = (curK + 41) % NUM_VECTORS;
    return curK;
  }

  void resetTest() {
    total = 0;
    startNanos = System.nanoTime();
  }

  void printTestOutput() {
    double elapsedUs = (System.nanoTime() - startNanos) / 1000.0;
    System.out.printf(Locale.ROOT, "%f us\t%f us\t%d\t%f\n", elapsedUs, elapsedUs / COUNT, COUNT, total);
  }

  // Baseline - this is what we are doing today for Vectors in Lucene; memory map a file (like
  // ByteBuffer(s)IndexInput), read from it into a scratch ByteBuffer wrapped by a FloatBuffer used
  // to decode into a resulting float[] array
  void test0() throws IOException {
    System.out.println("\nBASELINE\n");
    float[] u = new float[dim];
    float[] v = new float[dim];
    try (VectorReader vectors = new VectorReader(filePath, dim, false)) {
      for (int iter = 0; iter < NUM_ITERS; iter++) {
        resetTest();
        for (int i = 1; i <= COUNT; i++) {
          vectors.get(u, nextJ());
          vectors.get(v, nextK());
          total += dotProduct(u, 0, v, 0, dim);
        }
        printTestOutput();
      }
    }
  }

  // Like test0, but using a direct ByteBuffer for the temporary buffer; this made no difference.
  void test0Direct() throws IOException {
    System.out.println("\nBASELINE DIRECT\n");
    float[] u = new float[dim];
    float[] v = new float[dim];
    try (VectorReader vectors = new VectorReader(filePath, dim, true)) {
      for (int iter = 0; iter < NUM_ITERS; iter++) {
        resetTest();
        for (int i = 1; i <= COUNT; i++) {
          vectors.get(u, nextJ());
          vectors.get(v, nextK());
          total += dotProduct(u, 0, v, 0, dim);
        }
        printTestOutput();
      }
    }
  }

  // Like Baseline, but what we could do if we provided access to IndexInput's internal ByteBuffer;
  // memory map a file, call asFloatBuffer() on it and use that FloatBuffer to decode into a
  // temporary float[] array
  void test1() throws IOException {
    System.out.println("\nSKIP ONE COPY\n");
    float[] u = new float[dim];
    float[] v = new float[dim];
    try (BinaryFileVectors vectors = new BinaryFileVectors(filePath, dim)) {
      for (int iter = 0; iter < NUM_ITERS; iter++) {
        resetTest();
        for (int i = 1; i <= COUNT; i++) {
          vectors.get(u, nextJ());
          vectors.get(v, nextK());
          total += dotProduct(u, 0, v, 0, dim);
        }
        printTestOutput();
      }
    }
  }

  // Best performance - precopy all floats into a single on-heap float[]
  void test2() throws IOException {
    System.out.println("\nONE BIG ARRAY\n");
    float[] temp = new float[dim];
    Random r = new Random();
    try (BinaryFileVectors vectors = new BinaryFileVectors(filePath, dim)) {
      float[] array = new float[vectors.size * dim];
      for (int i = 0; i < NUM_VECTORS; i++) {
        vectors.get(temp, i);
        System.arraycopy(temp, 0, array, i * dim, dim);
      }
      for (int iter = 0; iter < NUM_ITERS; iter++) {
        resetTest();
        for (int i = 1; i <= COUNT; i++) {
          total += dotProduct(array, nextJ() * dim, array, nextK() * dim, dim);
        }
        printTestOutput();
      }
    }
  }

  // precopy all floats into two on-heap float[], 2x the RAM. This sometimes is like test2, and sometimes slower,
  // depending on sizes of arrays, dimensions, something.
  void test2b() throws IOException {
    System.out.println("\nTWO BIG ARRAYs\n");
    float[] temp = new float[dim];
    Random r = new Random();
    try (BinaryFileVectors vectors = new BinaryFileVectors(filePath, dim)) {
      float[] u = new float[vectors.size * dim];
      float[] v = new float[vectors.size * dim];
      for (int i = 0; i < NUM_VECTORS; i++) {
        vectors.get(temp, i);
        System.arraycopy(temp, 0, u, i * dim, dim);
        System.arraycopy(temp, 0, v, i * dim, dim);
      }

      for (int iter = 0; iter < NUM_ITERS; iter++) {
        resetTest();
        for (int i = 1; i <= COUNT; i++) {
          total += dotProduct(u, nextJ() * dim, v, nextK() * dim, dim);
        }
        printTestOutput();
      }
    }
  }

  // Precopy all floats into a multiple on-heap float[], one per vector
  void test3() throws IOException {
    System.out.println("\nMANY ARRAYS\n");
    float[] temp = new float[dim];
    Random r = new Random();
    try (BinaryFileVectors vectors = new BinaryFileVectors(filePath, dim)) {
      float[][] array = new float[vectors.size][];
      for (int i = 0; i < NUM_VECTORS; i++) {
        vectors.get(temp, i);
        array[i] = Arrays.copyOf(temp, dim);
      }
      for (int iter = 0; iter < NUM_ITERS; iter++) {
        resetTest();
        for (int i = 1; i <= COUNT; i++) {
          total += dotProduct(array[nextJ()], 0, array[nextK()], 0, dim);
        }
        printTestOutput();
      }
    }
  }

  // Precopy all bytes onto heap and use FloatBuffer to access as needed as in test1
  // Slowest of all
  void test4() throws IOException {
    System.out.println("\nBYTES ON HEAP\n");
    float[] u = new float[dim];
    float[] v = new float[dim];
    Random r = new Random();
    try (BinaryFileVectors vectors = new BinaryFileVectors(filePath, dim, true)) {
      for (int iter = 0; iter < NUM_ITERS; iter++) {
        resetTest();
        for (int i = 1; i <= COUNT; i++) {
          int j = r.nextInt(vectors.size);
          int k = r.nextInt(vectors.size);
          vectors.get(u, nextJ());
          vectors.get(v, nextK());
          total += dotProduct(u, 0, v, 0, dim);
        }
        printTestOutput();
      }
    }
  }

  // Precopy all bytes onto direct ByteBuffer on heap and use FloatBuffer to access as needed as in test1
  // This is slightly slower than baseline
  void test5() throws IOException {
    System.out.println("\nBYTES ON HEAP DIRECT\n");
    float[] u = new float[dim];
    float[] v = new float[dim];
    Random r = new Random();
    try (BinaryFileVectors vectors = new BinaryFileVectors(filePath, dim, true, true)) {
      for (int iter = 0; iter < NUM_ITERS; iter++) {
        resetTest();
        for (int i = 1; i <= COUNT; i++) {
          vectors.get(u, nextJ());
          vectors.get(v, nextK());
          total += dotProduct(u, 0, v, 0, dim);
        }
        printTestOutput();
      }
    }
  }

  static class BinaryFileVectors implements Closeable {

    private final int size;
    private final int dim;
    private final FileChannel in;
    private final FloatBuffer floatBuffer;

    BinaryFileVectors(Path filePath, int dim, boolean onHeap, boolean directBuffer) throws IOException {
      in = FileChannel.open(filePath);
      long totalBytes = NUM_VECTORS * dim * Float.BYTES;
      if (totalBytes > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("input over 2GB not supported");
      }
      this.dim = dim;
      int vectorByteSize = dim * Float.BYTES;
      size = (int) (totalBytes / vectorByteSize);
      ByteBuffer bytes;
      if (onHeap) {
        if (directBuffer) {
          bytes = ByteBuffer.allocateDirect((int) totalBytes);
        } else {
          bytes = ByteBuffer.allocate((int) totalBytes);
        }
        int n = in.read(bytes);
        if (n != totalBytes) {
          throw new IOException("Failed to read " + totalBytes + ", got " + n);
        }
        bytes.position(0);
      } else {
        bytes = in.map(FileChannel.MapMode.READ_ONLY, 0, totalBytes);
      }
      bytes.order(ByteOrder.LITTLE_ENDIAN);
      floatBuffer = bytes.asFloatBuffer();
    }

    BinaryFileVectors(Path filePath, int dim, boolean onHeap) throws IOException {
      this(filePath, dim, onHeap, false);
    }

    BinaryFileVectors(Path filePath, int dim) throws IOException {
      this(filePath, dim, false);
    }

    public void get(float[] vector, int targetOrd) {
      floatBuffer.position(targetOrd * dim);
      floatBuffer.get(vector);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }

  static class VectorReader implements Closeable {

    private final int dim;
    private final FileChannel in;
    private final ByteBuffer mmap;
    private final ByteBuffer backingBuffer;
    private final FloatBuffer floatBuffer;

    VectorReader(Path filePath, int dim, boolean directBuffer) throws IOException {
      in = FileChannel.open(filePath);
      long totalBytes = NUM_VECTORS * dim * Float.BYTES;
      if (totalBytes > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("input over 2GB not supported");
      }
      this.dim = dim;
      int vectorByteSize = dim * Float.BYTES;
      mmap = in.map(FileChannel.MapMode.READ_ONLY, 0, totalBytes);
      mmap.order(ByteOrder.LITTLE_ENDIAN);
      backingBuffer = ByteBuffer.allocate(vectorByteSize);
      backingBuffer.order(ByteOrder.LITTLE_ENDIAN);
      floatBuffer = backingBuffer.asFloatBuffer();
    }

    public void get(float[] vector, int targetOrd) {
      /*
        simulates Lucene90VectorReader.OffHeapVectorValues.vectorValue(), which does:

        dataIn.seek(ord * byteSize);
        dataIn.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize, false);
        floatBuffer.position(0);
        floatBuffer.get(value, 0, fieldEntry.dimension);
       */
      mmap.position(targetOrd * dim * Float.BYTES);
      mmap.get(backingBuffer.array(), 0, backingBuffer.capacity());
      floatBuffer.position(0);
      floatBuffer.get(vector);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }  

  static float dotProduct(float[] a, int aOffset, float[] b, int bOffset, int dim) {
    float res = 0f;
    int i, j;
    for (i = aOffset, j = bOffset; i < aOffset + dim; i++, j++) {
      res += b[i] * a[j];
    }
    return res;
  }

}
