package org.apache.lucene.index;
import java.io.IOException;
import java.util.*;

/**
 */
class SegmentTermVector implements TermFreqVector {
  private String field;
  private String terms[];
  private int termFreqs[];
  
  SegmentTermVector(String field, String terms[], int termFreqs[]) {
    this.field = field;
    this.terms = terms;
    this.termFreqs = termFreqs;
  }

  /**
   * 
   * @return The number of the field this vector is associated with
   */
  public String getField() {
    return field;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append('{');
    sb.append(field).append(": ");
    for (int i=0; i<terms.length; i++) {
      if (i>0) sb.append(", ");
      sb.append(terms[i]).append('/').append(termFreqs[i]);
    }
    sb.append('}');
    return sb.toString();
  }


  public String toString(IndexReader ir)
    throws IOException
  {
    return toString();
    /*StringBuffer sb = new StringBuffer();
    //TODO: Reimplement

    sb.append('{');
    sb.append(field).append(": ");
    for (int i=0; i<terms.length; i++) {
    if (i>0) sb.append(", ");
    Term t = ir.getTerm(terms[i]);
    String text = t == null ? "UNKNOWN(" + i + ")" : t.text;
    sb.append(text).append('/').append(termFreqs[i]);
    if (termProx != null) appendTermProx(sb.append('/'), termProx[i]);
    }
    sb.append('}');
    return sb.toString();*/
  }


  /** Number of terms in the term vector. If there are no terms in the
   *  vector, returns 0.
   */
  public int size() {
    return terms == null ? 0 : terms.length;
  }

  /** Array of term numbers in ascending order. If there are no terms in
   *  the vector, returns null.
   */
  public String [] getTerms() {
    return terms;
  }

  /** Array of term frequencies. Locations of the array correspond one to one
   *  to the term numbers in the array obtained from <code>getTermNumbers</code>
   *  method. Each location in the array contains the number of times this
   *  term occurs in the document or the document field. If there are no terms in
   *  the vector, returns null.
   */
  public int[] getTermFrequencies() {
    return termFreqs;
  }



  /** Return an index in the term numbers array returned from <code>getTermNumbers</code>
   *  at which the term with the specified <code>termNumber</code> appears. If this
   *  term does not appear in the array, return -1.
   */
  public int indexOf(String termText) {
    int res = Arrays.binarySearch(terms, termText);
    return res >= 0 ? res : -1;
  }

  /** Just like <code>indexOf(int)</code> but searches for a number of terms
   *  at the same time. Returns an array that has the same size as the number
   *  of terms searched for, each slot containing the result of searching for
   *  that term number. Array of term numbers must be sorted in ascending order.
   *
   *  @param termNumbers array containing term numbers to look for
   *  @param start index in the array where the list of termNumbers starts
   *  @param len the number of termNumbers in the list
   */
  public int[] indexesOf(String [] termNumbers, int start, int len) {
    // TODO: there must be a more efficient way of doing this.
    //       At least, we could advance the lower bound of the terms array
    //       as we find valid indexes. Also, it might be possible to leverage
    //       this even more by starting in the middle of the termNumbers array
    //       and thus dividing the terms array maybe in half with each found index.
    int res[] = new int[len];

    for (int i=0; i < len; i++) {
      res[i] = indexOf(termNumbers[i]);
    }
    return res;
  }
}
