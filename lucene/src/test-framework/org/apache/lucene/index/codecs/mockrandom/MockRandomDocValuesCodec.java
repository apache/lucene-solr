package org.apache.lucene.index.codecs.mockrandom;

import java.util.Random;

/**
 * Randomly combines terms index impl w/ postings impls. and uses non-CFS format for docvalues
 */
public class MockRandomDocValuesCodec extends MockRandomCodec {

  public MockRandomDocValuesCodec(Random random) {
    super(random, "MockDocValuesCodec", false);
    // uses noCFS for docvalues for test coverage
  }

}
