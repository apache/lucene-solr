package org.apache.solr.search;

import junit.framework.TestCase;

import java.util.Random;

import org.apache.solr.util.OpenBitSet;
import org.apache.solr.util.BitSetIterator;

/**
 * @author yonik
 * @version $Id$
 */
public class TestDocSet extends TestCase {
  Random rand = new Random();

  public OpenBitSet getRandomSet(int sz, int bitsToSet) {
    OpenBitSet bs = new OpenBitSet(sz);
    if (sz==0) return bs;
    for (int i=0; i<bitsToSet; i++) {
      bs.fastSet(rand.nextInt(sz));
    }
    return bs;
  }

  public DocSet getHashDocSet(OpenBitSet bs) {
    int[] docs = new int[(int)bs.cardinality()];
    BitSetIterator iter = new BitSetIterator(bs);
    for (int i=0; i<docs.length; i++) {
      docs[i] = iter.next();
    }
    return new HashDocSet(docs,0,docs.length);
  }

  public DocSet getBitDocSet(OpenBitSet bs) {
    return new BitDocSet(bs);
  }

  public DocSet getDocSet(OpenBitSet bs) {
    return rand.nextInt(2)==0 ? getHashDocSet(bs) : getBitDocSet(bs);
  }

  public void checkEqual(OpenBitSet bs, DocSet set) {
    for (int i=0; i<bs.capacity(); i++) {
      assertEquals(bs.get(i), set.exists(i));
    }
  }

  protected void doSingle(int maxSize) {
    int sz = rand.nextInt(maxSize+1);
    int sz2 = rand.nextInt(maxSize);
    OpenBitSet a1 = getRandomSet(sz, rand.nextInt(sz+1));
    OpenBitSet a2 = getRandomSet(sz, rand.nextInt(sz2+1));

    DocSet b1 = getDocSet(a1);
    DocSet b2 = getDocSet(a2);

    assertEquals((int)a1.cardinality(), b1.size());
    assertEquals((int)a2.cardinality(), b2.size());

    checkEqual(a1,b1);
    checkEqual(a2,b2);

    OpenBitSet a_and = (OpenBitSet)a1.clone(); a_and.and(a2);
    OpenBitSet a_or = (OpenBitSet)a1.clone(); a_or.or(a2);
    // OpenBitSet a_xor = (OpenBitSet)a1.clone(); a_xor.xor(a2);
    // OpenBitSet a_andn = (OpenBitSet)a1.clone(); a_andn.andNot(a2);

    checkEqual(a_and, b1.intersection(b2));
    checkEqual(a_or, b1.union(b2));

    assertEquals(a_and.cardinality(), b1.intersectionSize(b2));
    assertEquals(a_or.cardinality(), b1.unionSize(b2));

  }


  public void doMany(int maxSz, int iter) {
    for (int i=0; i<iter; i++) {
      doSingle(maxSz);
    }
  }

  public void testRandomDocSets() {
    doMany(300, 5000);
  }

}
