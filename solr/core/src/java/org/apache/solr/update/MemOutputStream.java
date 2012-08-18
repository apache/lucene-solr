package org.apache.solr.update;

import org.apache.solr.common.util.FastOutputStream;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/** @lucene.internal */
public class MemOutputStream extends FastOutputStream {
  public List<byte[]> buffers = new LinkedList<byte[]>();
  public MemOutputStream(byte[] tempBuffer) {
    super(null, tempBuffer, 0);
  }

  @Override
  public void flush(byte[] arr, int offset, int len) throws IOException {
    if (arr == buf && offset==0 && len==buf.length) {
      buffers.add(buf);  // steal the buffer
      buf = new byte[8192];
    } else if (len > 0) {
      byte[] newBuf = new byte[len];
      System.arraycopy(arr, offset, newBuf, 0, len);
      buffers.add(newBuf);
    }
  }

  public void writeAll(FastOutputStream fos) throws IOException {
    for (byte[] buffer : buffers) {
      fos.write(buffer);
    }
    if (pos > 0) {
      fos.write(buf, 0, pos);
    }
  }
}
