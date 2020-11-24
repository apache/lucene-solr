package org.apache.lucene.search.similarities;

import java.util.Random;

public class TestIndriDirichletSimilarity extends BaseSimilarityTestCase {

  @Override
  protected Similarity getSimilarity(Random random) {
    // smoothing parameter mu, unbounded
    final float mu;
    switch (random.nextInt(4)) {
    case 0:
      // minimum value
      mu = 0;
      break;
    case 1:
      // tiny value
      mu = Float.MIN_VALUE;
      break;
    case 2:
      // maximum value
      // we just limit the test to "reasonable" mu values but don't enforce this
      // anywhere.
      mu = Integer.MAX_VALUE;
      break;
    default:
      // random value
      mu = Integer.MAX_VALUE * random.nextFloat();
      break;
    }
    return new IndriDirichletSimilarity(mu);
  }

}