package org.apache.lucene.index;

/** Provides access to stored term vector of 
 *  a document field.
 */
public interface TermFreqVector {
  /**
   * 
   * @return The field this vector is associated with.
   * 
   */ 
  public String getField();
  
  /** 
   * @return The number of terms in the term vector.
   */
  public int size();

  /** 
   * @return An Array of term texts in ascending order.
   */
  public String[] getTerms();


  /** Array of term frequencies. Locations of the array correspond one to one
   *  to the terms in the array obtained from <code>getTerms</code>
   *  method. Each location in the array contains the number of times this
   *  term occurs in the document or the document field.
   */
  public int[] getTermFrequencies();
  

  /** Return an index in the term numbers array returned from
   *  <code>getTerms</code> at which the term with the specified
   *  <code>term</code> appears. If this term does not appear in the array,
   *  return -1.
   */
  public int indexOf(String term);


  /** Just like <code>indexOf(int)</code> but searches for a number of terms
   *  at the same time. Returns an array that has the same size as the number
   *  of terms searched for, each slot containing the result of searching for
   *  that term number.
   *
   *  @param terms array containing terms to look for
   *  @param start index in the array where the list of terms starts
   *  @param len the number of terms in the list
   */
  public int[] indexesOf(String[] terms, int start, int len);

}
