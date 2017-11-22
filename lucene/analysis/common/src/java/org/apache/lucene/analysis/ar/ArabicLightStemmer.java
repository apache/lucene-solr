/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.analysis.ar;



import static org.apache.lucene.analysis.util.StemmerUtil.*;
import java.util.Arrays;
import org.apache.lucene.analysis.CharArraySet;

/**
 *  Light Stemmer for Arabic.
 *  <p>
 *  Stemming  is done in-place for efficiency, operating on a termbuffer.
 *  <p>
 *  Stemming is defined as:
 *  <ul>
 *  <li> Removal of attached definite article, conjunction, and prepositions.
 *  <li> Stemming of common prefixes, infixes and suffixes.
 * </ul>
 *
 */
public class ArabicLightStemmer {
  
  private char str[];
  private boolean validated = false;
  
  private CharArraySet validatedStemList;
  private boolean highAccuracyRequired;
  
  
  public static final char ALEF = '\u0627';
  public static final char BEH = '\u0628';
  public static final char TEH_MARBUTA = '\u0629';
  public static final char TEH = '\u062A';
  public static final char FEH = '\u0641';
  public static final char KAF = '\u0643';
  public static final char LAM = '\u0644';
  public static final char NOON = '\u0646';
  public static final char MEM = '\u0645';
  public static final char HEH = '\u0647';
  public static final char WAW = '\u0648';
  public static final char YEH = '\u064A';
  public static final char YEH_HAMZA = '\u0626';
  public static final char WAW_HAMZA = '\u0624';
  public static final char HAMZA = '\u0621';
  
  
  public boolean isHighAccuracyRequired() {
    return highAccuracyRequired;
  }

  public void setHighAccuracyRequired(boolean highAccuracyRequired) {
    this.highAccuracyRequired = highAccuracyRequired;
  }

  /*public CharArraySet getValidatedStemList() {
    return validatedStemList;
  }*/

  public void setValidatedStemList(CharArraySet validatedStemList) {
    this.validatedStemList = validatedStemList;
  }
  
  public static final char prefixes[][] = {
    ("" + ALEF + LAM).toCharArray(), 
    ("" + WAW + ALEF + LAM).toCharArray(), 
    ("" + BEH + ALEF + LAM).toCharArray(),
    ("" + KAF + ALEF + LAM).toCharArray(),
    ("" + FEH + ALEF + LAM).toCharArray(),
    ("" + LAM + LAM).toCharArray(),
    ("" + WAW).toCharArray(),
  };
  
  public static final char compoundPrefixes[][] = {
    ("" + WAW + KAF + ALEF + LAM).toCharArray(),
    ("" + WAW + FEH + ALEF + LAM).toCharArray(),
    ("" + WAW + BEH + ALEF + LAM).toCharArray(),
    ("" + WAW + ALEF + LAM).toCharArray(), 
    ("" + BEH + ALEF + LAM).toCharArray(),
    ("" + KAF + ALEF + LAM).toCharArray(),
    ("" + FEH + ALEF + LAM).toCharArray(),
    ("" + ALEF + LAM).toCharArray(), 
    ("" + WAW + LAM + LAM).toCharArray(),
    ("" + LAM + LAM).toCharArray(),      
  };
  
  public static final char singlePrefixes[][] = {
    ("" + LAM + ALEF).toCharArray(), 
    ("" + WAW).toCharArray(), 
    ("" + KAF).toCharArray(),
    ("" + FEH).toCharArray(),
    ("" + LAM).toCharArray(),
    ("" + BEH).toCharArray(),
  };
  
  public static final char suffixes[][] = {
    ("" + HEH + ALEF).toCharArray(), 
    ("" + ALEF + NOON).toCharArray(), 
    ("" + ALEF + TEH).toCharArray(), 
    ("" + WAW + NOON).toCharArray(), 
    ("" + YEH + NOON).toCharArray(), 
    ("" + YEH + HEH).toCharArray(),
    ("" + YEH + TEH_MARBUTA).toCharArray(),
    ("" + HEH).toCharArray(),
    ("" + TEH_MARBUTA).toCharArray(),
    ("" + YEH).toCharArray(),
  };
  
  public static final char suffixes_set1[][] = {
    ("" + YEH + ALEF + TEH).toCharArray(), 
    ("" + ALEF + TEH).toCharArray(),
    ("" + ALEF + TEH + HEH).toCharArray(),
  };
  
  public static final char suffixes_set2[][] = {
    ("" + HEH + ALEF).toCharArray(), 
    ("" + WAW + NOON).toCharArray(),
    ("" + WAW + ALEF).toCharArray(),
    ("" + YEH + NOON).toCharArray(),
    ("" + ALEF + NOON).toCharArray(),
    ("" + YEH + HEH).toCharArray(),
    ("" + HEH + MEM).toCharArray(),
    ("" + YEH).toCharArray(),
    ("" + HEH).toCharArray(),
    ("" + TEH_MARBUTA).toCharArray(),
    ("" + ALEF).toCharArray(),
  };
  
  public static final char suffixes_set3[][] = {
    ("" + NOON + ALEF).toCharArray(), 
    ("" + TEH).toCharArray(),
  };
    
  public char[] getStr() {
	return str;
  }

  /**
   * Stem an input buffer of Arabic text.
   * 
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int stem(char s[], int len) {
	str = Arrays.copyOf(s, len);
    boolean foundPrefixRule = lightStemPrefix(str, len);
    if(validated)
      return str.length;
    boolean foundSuffixRule = lightStemSuffix(str, str.length);
    if(foundSuffixRule)
      return str.length;
    boolean foundInfixRule = lightStemInfix(str, str.length);
    if(foundInfixRule)
      return str.length;
    if(foundPrefixRule) {
      return str.length;
    } else {
      //Cannot stem the input. Reset str to the original input and return it.
      str = Arrays.copyOf(s, len);
      return len;
    }
  }
  
  /**
   * Stem a prefix off an Arabic word using light stemmer.
   * @param s input buffer
   * @param len length of input buffer
   * @return true if Prefix is found and false if there is no matched Prefix.
   */
  public boolean lightStemPrefix(char s[], int len) {
    validated = false;
    boolean foundPrefixRule = false;
    //Check for compound prefixes first
    for (int i = 0; i < compoundPrefixes.length; i++) { 
      if (startsWithCheckLength(s, len, compoundPrefixes[i])) {
        foundPrefixRule = true;
        str = Arrays.copyOfRange(s, compoundPrefixes[i].length, len);
    	if(validatedStemList.contains(str)) {
	    		validated = true;
        }
        return foundPrefixRule;
      }
    }
    //No Compound prefixes found.
	for (int i = 0; i < singlePrefixes.length; i++) { 
      if (startsWithCheckLength(s, len, singlePrefixes[i])) {
        foundPrefixRule = true;
        if(validatedStemList.contains(s, singlePrefixes[i].length, len - singlePrefixes[i].length)) {
    	  validated = true;
    	  str = Arrays.copyOfRange(s, singlePrefixes[i].length, len);
    	  return foundPrefixRule;
    	}
    	//Try to remove suffix and check again
        char s2[] = Arrays.copyOfRange(s,singlePrefixes[i].length, len);
        boolean foundSuffixRule = lightStemSuffix(s2, s2.length);
        if(foundSuffixRule && validated) {
          return foundPrefixRule;
        } else {
          str = Arrays.copyOfRange(s, 0, len);
        }
        //Try to remove Infix and check again
        boolean foundInfixRule = lightStemInfix(s2, s2.length);
        if(foundInfixRule && validated) {
          return foundPrefixRule;
        } else {
          str = Arrays.copyOfRange(s, 0, len);
        }
      }
      foundPrefixRule = false;
    }	
    return foundPrefixRule;
  }
  
  /**
   * Stem a suffix off an Arabic word using light stemmer.
   * @param s input buffer
   * @param len length of input buffer
   * @return true if Suffix is found and false if there is no matched Suffix.
   */
  public boolean lightStemSuffix(char s[], int len) {
    validated = false;
    boolean foundSuffixRule = false;
    //Check for suffix set 1 first
    for (int i = 0; i < suffixes_set1.length; i++) {
      if (endsWithCheckLength(s, len, suffixes_set1[i])) {
        foundSuffixRule = true;
        if (validatedStemList.contains(s, 0, len - suffixes_set1[i].length)) {
          str = Arrays.copyOfRange(s, 0, len - suffixes_set1[i].length);
          validated = true;
          return foundSuffixRule;
    	} else {    		  
          //Adding HEH at the end and check again
          s[len - suffixes_set1[i].length] = HEH;
          if (validatedStemList.contains(s, 0, len - suffixes_set1[i].length + 1)) {
            str = Arrays.copyOfRange(s, 0, len - suffixes_set1[i].length +1);
    	    validated = true;
            return foundSuffixRule; 
          } else {
            //Try to stem Infix without HEH and check again
            boolean foundInfixRule = stemInfixVerb(s, len - suffixes_set1[i].length);
            if (foundInfixRule)
              return foundSuffixRule;
    	    //Try to stem Infix with HEH and check again
            foundInfixRule = stemInfixVerb(s, len - suffixes_set1[i].length +1);
            if (foundInfixRule) {
              return foundSuffixRule;
            }
            s[len - suffixes_set1[i].length] = suffixes_set1[i][0];
    	  }
        }
        //Stem the suffix Set 1 even it is not validated if not high accuracy is set
        if (!highAccuracyRequired) {
          str = Arrays.copyOfRange(s, 0, len - suffixes_set1[i].length);
		  return foundSuffixRule;
        }
      }
    }
    //Check for suffix set 2 in order
    for ( int i = 0; i < suffixes_set2.length; i++ ) { 
      if ( endsWithCheckLength(s, len, suffixes_set2[i]) ) {
        foundSuffixRule = true;
        if(validatedStemList.contains(s, 0, len - suffixes_set2[i].length)) {
          str = Arrays.copyOfRange(s, 0, len - suffixes_set2[i].length);
          validated = true;
    	  return foundSuffixRule;
        } else {    		  
          //Check for Teh case
          if (s[len - suffixes_set2[i].length -1] == TEH) {
            s[len - suffixes_set2[i].length -1] = HEH;
            if (validatedStemList.contains(s, 0, len - suffixes_set2[i].length)) {
	    	  str = Arrays.copyOfRange(s, 0, len - suffixes_set2[i].length);
              validated = true;
              return foundSuffixRule; 
            } else {
              //Try to stem Infix and check again
              boolean foundInfixRule = lightStemInfix(s, len - suffixes_set2[i].length);
              if (foundInfixRule) {
                return foundSuffixRule;
              }
              //reset HEH to TEH
              s[len - suffixes_set2[i].length -1] = TEH;
            }	    		  
          }  
    	  //Try to stem verb Infix and check again
          boolean foundInfixRule = stemInfixVerb(s, len - suffixes_set2[i].length);
          if (foundInfixRule) {
            return foundSuffixRule;
          }  
          //Check for YehHamza case
          if( (i == 0 || i == 6 || i == 8 ) && (len > suffixes_set2[i].length +2) &&
        	s[len - suffixes_set2[i].length -1] == YEH_HAMZA && s[len - suffixes_set2[i].length -2] == ALEF) {
    		  s[len - suffixes_set2[i].length -1] = HAMZA;
              if(validatedStemList.contains(s, 0, len - suffixes_set2[i].length)) {
                str = Arrays.copyOfRange(s, 0, len - suffixes_set2[i].length);
                validated = true;
                return foundSuffixRule; 
    		  }
              //Try to stem noun Infix and check again
              foundInfixRule = stemInfixNoun(s, len - suffixes_set2[i].length);
              if (foundInfixRule) {
                return foundSuffixRule;
              }
          }
          //Try to stem Infix and check again
          foundInfixRule = stemInfixNoun(s, len - suffixes_set2[i].length);
          if (foundInfixRule) {
            return foundSuffixRule;
          }
        }
        
        //Check for suffix set 3 for the stemmed suffix set 2
        char s2[] = Arrays.copyOfRange(s, 0, len - suffixes_set2[i].length);
    	for ( int j = 0; j < suffixes_set3.length; j++ ) { 
    	  if ( endsWithCheckLength(s2, s2.length, suffixes_set3[j])) {
    	    if( validatedStemList.contains(s2, 0, s2.length - suffixes_set3[j].length)) {
    	      str = Arrays.copyOfRange(s2, 0, s2.length - suffixes_set3[j].length);
    	      validated = true;
    	      return foundSuffixRule;
    	    } else {
    	      //Try to stem Infix and check again    		  
              boolean foundInfixRule =lightStemInfix(s2, s2.length - suffixes_set3[j].length);
              if(foundInfixRule) {
                return foundSuffixRule;
              }  	    		
    	      if(!highAccuracyRequired) {
                str = Arrays.copyOfRange(s2, 0, s2.length - suffixes_set3[j].length);
	    	    return foundSuffixRule;
              }
    	    }
    	  }
    	}
    	  
    	//Handle Noon in suffix set 3 separately
    	if(suffixes_set2[i].length == 1 && suffixes_set2[i][0] == ALEF 
          && endsWithCheckLength(s2, s2.length, ("" + NOON).toCharArray())) {
	        if(validatedStemList.contains(s2, 0, s2.length - 1)) {
    	      str = Arrays.copyOfRange(s2, 0, s2.length - 1);
    	      validated = true;
              return foundSuffixRule;
	        } else {
	          //Try to stem Infix and check again    		  
	          boolean  foundInfixRule = lightStemInfix(s2, s2.length - 1);
	          if (foundInfixRule) {
	            return foundSuffixRule;
	          }
              if (!highAccuracyRequired) {
                str = Arrays.copyOfRange(s2, 0, s2.length - 1);
                return foundSuffixRule;
	          }
	        }
	   }  
       foundSuffixRule = false;
      }
    }
	
	//Check for suffix set 3 
	for (int i = 0; i < suffixes_set3.length; i++) { 
      if (endsWithCheckLength(s, len, suffixes_set3[i])) {
        foundSuffixRule = true;
        if(validatedStemList.contains(s, 0, len - suffixes_set3[i].length)) {
		  str = Arrays.copyOfRange(s, 0, len - suffixes_set3[i].length);
          validated = true;
		  return foundSuffixRule;
		} else {
		  //Try to stem Infix and check again    		  
		  boolean foundInfixRule = lightStemInfix(s, len - suffixes_set2[i].length);
		  if (foundInfixRule) {
		    return foundSuffixRule;
		  }
		  if (!highAccuracyRequired) {
		    str = Arrays.copyOfRange(s, 0, len - suffixes_set3[i].length);
            return foundSuffixRule;
          }
	    }
	  }
   }
   return foundSuffixRule;
  }
  
  /**
   * Stem a infix off an Arabic word using light stemmer.
   * @param s input buffer
   * @param len length of input buffer
   * @return true if Infix rule found and false if no matched Infix rule.
   */
  public boolean lightStemInfix(char s[], int len) {
    boolean foundInfixRule = false;
    foundInfixRule = stemInfixVerb(s, len);
    if (foundInfixRule)
      return true;
    foundInfixRule = stemInfixNoun(s, len);
    return foundInfixRule;
  }
  
  /**
   * Stem a infix off an Arabic noun using light stemmer.
   * @param s input buffer
   * @param len length of input buffer
   * @return true if Infix rule found and false if no matched Infix rule.
   */
  public boolean stemInfixNoun(char s[], int len) {
	boolean foundInfixRule = false;
	validated = false;
	if (len >=3) {
      //Checking infix rules
	  char s2[] = Arrays.copyOf(s, len);
      //Rule 4
	  if (len == 3) {
	    s2 = Arrays.copyOf(s2, 4);
		s2[3] = HEH;
		if (validatedStemList.contains(s2)) {
          str = Arrays.copyOf(s2, 4);
          foundInfixRule = true;
          validated = true;
		  return true;
		}
      }
      //Rule 16
      if(len == 3 && s[1] == s[2]) {
        s2 = Arrays.copyOf(s, len);
	    s2[2] = HEH;
		if(validatedStemList.contains(s2)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          validated = true;
          return true;
	    }
	  }
      //Rule 3
      if(len >= 5 && endsWithCheckLength(s, len, ("" + ALEF + YEH + ALEF).toCharArray())
		&& s[1] != WAW) {
        s2 = Arrays.copyOf(s, len);
        s2[len-3] = YEH;
        s2[len-2] = HEH;
        if (validatedStemList.contains(s2, 0, len - 1)) { 
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        if(!highAccuracyRequired){
	      str = Arrays.copyOfRange(s2, 0, len -1);
	      foundInfixRule = true;
          return true;
		}
	  }
	  //Rule 12
	  if(len == 5 && s[3] == ALEF && s[4] == HAMZA) {
		s2 = Arrays.copyOf(s, len);
		if (s2[0] == ALEF) {
		  s2[4] = s2[2];
		  s2[3] = YEH;
		  delete(s2, 0, len);
		  if(validatedStemList.contains(s2, 0, len-1)) {
		    foundInfixRule = true;
			str = Arrays.copyOfRange(s2, 0, len -1);
			validated = true;
			return true;
		  }
          s2 = Arrays.copyOf(s, len);
		}
        s2[3] = s2[2];
        s2[2] = YEH;
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        s2[2] = ALEF;
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        s2[2] = s2[1];
        s2[1] = ALEF;
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        delete(s2, 1, len);
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }			
      }
      //Rule 1 
      if (len == 5 && s[3] == YEH_HAMZA && s[2] == ALEF && (s[4] != YEH || s[4] != HEH) && s[1] != WAW) {
        s2 = Arrays.copyOf(s, len);			
        s2[2] = YEH;
        s2[3] = s[4];
        s2[4] = HEH;
		if (validatedStemList.contains(s2)) {		
		  foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          validated = true;
          return true;
        }
		//Remove HEH and check again
		else if (validatedStemList.contains(s2, 0, len - 1)) { 
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        if (!highAccuracyRequired) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          return true;
        }
      }
      //Rule 11
      if(len == 5 && s[1] == WAW && s[2] == ALEF 
        && (s[0] != TEH || s[0] != YEH || s[0] != ALEF) && s[4] != HEH) {
        s2 = Arrays.copyOf(s, len);
        delete(s2, 2, len);
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        //Append HeH and check again
        s2[4] = HEH;
        if (validatedStemList.contains(s2)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          validated = true;
          return true;
        }
        s2 = Arrays.copyOf(s, len);
        delete(s2, 1, len);
        str = Arrays.copyOfRange(s2, 0, len -1);
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          validated = true;
          return true;
        }
        //Append HeH and check again
        s2[4] = HEH;
        if (validatedStemList.contains(s2)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          validated = true;
          return true;
        }
        if (!highAccuracyRequired) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          return true;
		}
      }
      //Rule 6
      if (len == 5 && s[2] == ALEF && s[0] != TEH && s[4] != HEH) {
        s2 = Arrays.copyOf(s, len);
        delete(s2, 2, len);
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        //Append HeH and check again
        s2[4] =  HEH;
        if (validatedStemList.contains(s2)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          validated = true;
          return true;
        }
        if (!highAccuracyRequired) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          return true;
        }
      }
      //Rule 8
      if (len == 5 && s[0] == ALEF && s[4] == HEH && s[2] != ALEF) {
        s2 = Arrays.copyOf(s, len);
        s2[0] = s2[1];
        s2[1] = s2[2];
        s2[2] = ALEF;
        if (s2[1] == YEH_HAMZA)
          s2[1] = WAW_HAMZA;
        if (validatedStemList.contains(s2, 0, len - 1)) { 
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        //Append HeH and check again
        s2[4] = HEH;
        if (validatedStemList.contains(s2)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          validated = true;
          return true;
        }
        if (!highAccuracyRequired) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
		  return true;
		}
      }
      //Rule 9
      if (len == 5 && s[0] == ALEF && s[3] == ALEF && s[1] != YEH ) {
        s2 = Arrays.copyOf(s, len);
        delete(s2, 3, len);
        delete(s2, 0, len);
        if (validatedStemList.contains(s2, 0, len-2)) { 
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -2);
          validated = true;
          return true;
        }
        //Append HeH and check again
        s2[3] = HEH;
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        if (!highAccuracyRequired) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -2);
          return true;
        }
      }
      //Rule 2 
      if (len == 5 && s[3] == YEH_HAMZA && s[2] == ALEF && (s[4] != YEH || s[4] != HEH)) {
        s2 = Arrays.copyOf(s, len);
        s2[1] = s2[2];
        s2[2] = s2[3];
        s2[3] = s2[4];
        s2[4] = HEH;
        if (validatedStemList.contains(s2)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          validated = true;
          return true;
        }
        //Remove HEH and check again
        else if (validatedStemList.contains(s2, 0, len - 1)) { 
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        if (!highAccuracyRequired) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          return true;
        }
      }
      //Rule 13
      if (len == 4 && s[1] == LAM && s[2] == ALEF) {
        s2 = Arrays.copyOf(s, len);
        delete(s2, 2, len);
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        if (s2[1] == s2[2] && validatedStemList.contains(s2, 0, 2)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, 2);
          validated = true;
          return true;
        }
        //Append HeH and check again
        s2[2] = HEH;
        if (validatedStemList.contains(s2, 0, 3)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, 3);
          validated = true;
          return true;
        }
      }
      //Rule 7
      if (len == 4 && s[0] == ALEF) {
        s2 = Arrays.copyOf(s, len);
        delete(s2, 0, len);
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        //Append HeH and check again
		s2[3] = HEH;
        if (validatedStemList.contains(s2)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          validated = true;
          return true;
        }
      }
      //Rule 10
      if (len == 4 && s[2] == WAW && s[1] == s[3]) {
        s2 = Arrays.copyOf(s, len);
        if (validatedStemList.contains(s2, 0, len - 2)) { 	
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -2);
          validated = true;
          return true;
        }
        //Append HeH and check again
        s2[3] = HEH;
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len -1);
          validated = true;
          return true;
        }
        if(!highAccuracyRequired){
	      foundInfixRule = true;
	      str = Arrays.copyOfRange(s2, 0, len -2);
          return true;
        }
      }		
      //Rule 5
      if (len == 4 && s[2] == WAW 
        && !(s[3] == HEH || s[3] == ALEF || s[3] == YEH || s[3] == HAMZA)
        && !(s[0] == YEH || s[0] == WAW || s[0] == ALEF || s[0] == TEH) ) {
    	  s2 = Arrays.copyOf(s, len);
          delete(s2, 2, len);
          if (validatedStemList.contains(s2, 0, len -2)) {
            foundInfixRule = true;
            str = Arrays.copyOfRange(s2, 0, len -2);
            validated = true;
            return true;
          }
          //Append HeH and check again
          s2[3] =  HEH;
          if (validatedStemList.contains(s2,0, len -1)) {
            foundInfixRule = true;
            str = Arrays.copyOfRange(s2, 0, len-1);
            validated = true;
            return true;
          }
          if (!highAccuracyRequired) {
            foundInfixRule = true;
            str = Arrays.copyOfRange(s2, 0, len -2);
            return true;
	      }
      }
      //Rule 14
      if (len == 4 && s[2] == ALEF) {
        s2 = Arrays.copyOf(s, len);
        s2[2] = s2[1];
        s2[1] = ALEF;
        if (validatedStemList.contains(s2)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len);
          validated = true;
          return true;
        }			
      }
      //Rule 15
      if (len == 6 && s[2] == ALEF && s[4] == YEH) {
        s2 = Arrays.copyOf(s, len);
        if (s2[1] == WAW) {
          s2[4] = WAW;
          delete(s2, 2, len);
          if (validatedStemList.contains(s2, 0, len-1)) {
		    foundInfixRule = true;
            str = Arrays.copyOfRange(s2, 0, len-1);
			validated = true;
			return true;
		  }
          //Append HeH and check again
          s2[5] = HEH;
          if (validatedStemList.contains(s2, 0, len)) {
            foundInfixRule = true;
            str = Arrays.copyOfRange(s2, 0, len);
            validated = true;
            return true;
          }			
        }
        s2 = Arrays.copyOf(s, len);
        delete(s2, 2, len);
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len-1);
          validated = true;
          return true;
        }
        s2[3] = ALEF;
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len-1);
          validated = true;
          return true;
        }
        s2[3] = WAW;
        if (validatedStemList.contains(s2, 0, len-1)) {
          foundInfixRule = true;
          str = Arrays.copyOfRange(s2, 0, len-1);
          validated = true;
          return true;
        }	
      }
    }	
    return foundInfixRule;
  }
  
  /**
   * Stem a infix of an Arabic verb using light stemmer.
   * @param s input buffer
   * @param len length of input buffer
   * @return true if Infix rule found and false if no matched Infix rule.
   */
  public boolean stemInfixVerb(char s[], int len) {
    boolean foundInfixRule = false;
    validated = false;
    if (len >=3) {
	  char s2[] = Arrays.copyOf(s, len);
	  //Rule 17
	  if (len >= 3 && s[2] != ALEF && s[len-1] != HEH 
        && (s[0] == YEH || s[0] == TEH)) {
		  s2[0] = ALEF;
          if (validatedStemList.contains(s2)) {
            foundInfixRule = true;
            str = Arrays.copyOfRange(s2, 0, len);
            validated = true;
            return true;
          }
          if (s2[len-2] == YEH) {
        	s2[len-2] = ALEF;
            if (validatedStemList.contains(s2)) {
              foundInfixRule = true;
              str = Arrays.copyOfRange(s2, 0, len);
              validated = true;
              return true;
            }
          }
        }
	}
    return foundInfixRule;
  }
  
  /**
   * Returns true if the prefix matches and can be stemmed
   * @param s input buffer
   * @param len length of input buffer
   * @param prefix prefix to check
   * @return true if the prefix matches and can be stemmed
   */
  boolean startsWithCheckLength(char s[], int len, char prefix[]) {
    if (prefix.length == 1 && len < 4) { // wa- prefix requires at least 3 characters
      return false;
    } else if (len < prefix.length + 2) { // other prefixes require only 2.
      return false;
    } else {
      for (int i = 0; i < prefix.length; i++)
        if (s[i] != prefix[i])
          return false;
        
      return true;
    }
  }
  
  /**
   * Returns true if the suffix matches and can be stemmed
   * @param s input buffer
   * @param len length of input buffer
   * @param suffix suffix to check
   * @return true if the suffix matches and can be stemmed
   */
  boolean endsWithCheckLength(char s[], int len, char suffix[]) {
    if (len < suffix.length + 2) { // all suffixes require at least 2 characters after stemming
      return false;
    } else {
      for (int i = 0; i < suffix.length; i++)
        if (s[len - suffix.length + i] != suffix[i])
          return false;
        
      return true;
    }
  }  
}
