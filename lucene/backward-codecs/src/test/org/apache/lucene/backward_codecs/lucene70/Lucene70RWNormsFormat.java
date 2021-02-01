package org.apache.lucene.backward_codecs.lucene70;

import java.io.IOException;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.index.SegmentWriteState;

public class Lucene70RWNormsFormat extends Lucene70NormsFormat {

  @Override
  public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene70NormsConsumer(
        state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
  }
}
