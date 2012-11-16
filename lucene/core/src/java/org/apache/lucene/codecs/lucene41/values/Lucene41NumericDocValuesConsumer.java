package org.apache.lucene.codecs.lucene41.values;

import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NumericDocValuesConsumer;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Writer;

public class Lucene41NumericDocValuesConsumer extends NumericDocValuesConsumer {
  private final IndexOutput output;
  private final Writer writer;
  private final long minValue;
  private int numDocsWritten = 0;
  static final int VERSION_START = -1;
  static final String CODEC_NAME = "Lucene41Numeric";

  public Lucene41NumericDocValuesConsumer(IndexOutput output, long minValue, long maxValue, int valueCount) throws IOException {
    this.output = output;
    final long delta = maxValue - minValue;
    final int bitsRequired = delta < 0 ? 64 : PackedInts.bitsRequired(delta);
    CodecUtil.writeHeader(output, CODEC_NAME, VERSION_START);
    output.writeLong(minValue);
    this.minValue = minValue;
    // nocommit write without header?
    this.writer = PackedInts.getWriter(output, valueCount, bitsRequired, PackedInts.FASTEST);
  }
  
  
  @Override
  public void add(long value) throws IOException {
    writer.add(value-minValue);
    numDocsWritten++;
  }
  
  @Override
  public void finish() throws IOException {
    try {
      writer.finish();
    } finally {
      output.close();
    }
  }
  
}
