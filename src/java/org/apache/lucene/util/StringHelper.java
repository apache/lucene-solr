package org.apache.lucene.util;



/**
 * Methods for manipulating strings
 *
 **/
public abstract class StringHelper {
  
  /**
   * 
   * @param s1 The first string to compare
   * @param s2 The second string to compare
   * @return The first position where the two strings differ.
   */ 
  public static final int stringDifference(String s1, String s2) {
    int len1 = s1.length();
    int len2 = s2.length();
    int len = len1 < len2 ? len1 : len2;
    for (int i = 0; i < len; i++) {   
      if (s1.charAt(i) != s2.charAt(i)) {
	      return i;
      }
    }
    return len;
  }


  private StringHelper() {
  }
}
