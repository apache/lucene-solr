package org.apache.lucene.gradle;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

public class SpillWriter extends Writer {
  private final static int MAX_BUFFERED = 2 * 1024;
  private final StringWriter buffer = new StringWriter(MAX_BUFFERED);

  private final Supplier<Path> spillPathSupplier;
  private Writer spill;
  private Path spillPath;

  public SpillWriter(Supplier<Path> spillPathSupplier) {
    this.spillPathSupplier = spillPathSupplier;
  }

  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    getSink(len).write(cbuf, off, len);
  }

  private Writer getSink(int expectedWriteChars) throws IOException {
    if (spill == null) {
      if (buffer.getBuffer().length() + expectedWriteChars <= MAX_BUFFERED) {
        return buffer;
      }

      spillPath = spillPathSupplier.get();
      spill = Files.newBufferedWriter(spillPath, StandardCharsets.UTF_8);
      spill.append(buffer.getBuffer());
      buffer.getBuffer().setLength(0);
    }

    return spill;
  }

  @Override
  public void flush() throws IOException {
    getSink(0).flush();
  }

  @Override
  public void close() throws IOException {
    buffer.close();
    if (spill != null) {
      spill.close();
      Files.delete(spillPath);
    }
  }

  public void copyTo(PrintStream ps) throws IOException {
    if (spill != null) {
      flush();
      char [] buf = new char [MAX_BUFFERED];
      try (Reader reader = Files.newBufferedReader(spillPath, StandardCharsets.UTF_8)) {
        int len;
        while ((len = reader.read(buf)) >= 0) {
          ps.print(new String(buf, 0, len));
        }
      }
    } else {
      ps.append(buffer.getBuffer());
    }
  }
}
