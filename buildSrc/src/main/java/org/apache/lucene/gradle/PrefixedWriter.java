package org.apache.lucene.gradle;

import java.io.IOException;
import java.io.Writer;

/**
 * Prefixes every new line with a given string, synchronizing multiple streams to emit consistent lines.
 */
public class PrefixedWriter extends Writer {
  Writer sink;

  private final static char LF = '\n';
  private final String prefix;
  private final StringBuilder lineBuffer = new StringBuilder();
  private final int maxLineLength;

  public PrefixedWriter(String prefix, Writer sink, int maxLineLength) {
    super(sink);
    this.sink = sink;
    this.prefix = prefix;
    this.maxLineLength = maxLineLength;
  }

  @Override
  public void write(int c) throws IOException {
    if (lineBuffer.length() == maxLineLength || c == LF) {
      sink.write(prefix);
      sink.write(lineBuffer.toString());
      sink.write(LF);

      lineBuffer.setLength(0);
      if (c != LF) { 
        lineBuffer.append((char) c);
      }
    } else {
      lineBuffer.append((char) c);
    }
  }

  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    for (int i = off; i < off + len; i++) {
      write(cbuf[i]);
    }
  }

  @Override
  public void flush() throws IOException {
    // don't pass flushes.
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Complete the current line (emit LF if not at the start of the line already).
   */
  public void completeLine() throws IOException {
    if (lineBuffer.length() > 0) {
      write(LF);
    }
  }
}
