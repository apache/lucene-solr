package org.apache.lucene.index;

/** Extends <code>TermFreqVector</code> to provide additional information about
 *  positions in which each of the terms is found.
 */
public interface TermPositionVector extends TermFreqVector {

    /** Returns an array of positions in which the term is found.
     *  Terms are identified by the index at which its number appears in the
     *  term number array obtained from <code>getTermNumbers</code> method.
     */
    public int[] getTermPositions(int index);
}