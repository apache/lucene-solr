/**
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
 
package org.apache.solr.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits words into subwords and performs optional transformations on subword groups.
 * Words are split into subwords with the following rules:
 *  - split on intra-word delimiters (by default, all non alpha-numeric characters).
 *     - "Wi-Fi" -> "Wi", "Fi"
 *  - split on case transitions
 *     - "PowerShot" -> "Power", "Shot"
 *  - split on letter-number transitions
 *     - "SD500" -> "SD", "500"
 *  - leading and trailing intra-word delimiters on each subword are ignored
 *     - "//hello---there, 'dude'" -> "hello", "there", "dude"
 *  - trailing "'s" are removed for each subword
 *     - "O'Neil's" -> "O", "Neil"
 *     - Note: this step isn't performed in a separate filter because of possible subword combinations.
 *
 * The <b>combinations</b> parameter affects how subwords are combined:
 *  - combinations="0" causes no subword combinations.
 *     - "PowerShot" -> 0:"Power", 1:"Shot"  (0 and 1 are the token positions)
 *  - combinations="1" means that in addition to the subwords, maximum runs of non-numeric subwords are catenated and produced at the same position of the last subword in the run.
 *     - "PowerShot" -> 0:"Power", 1:"Shot" 1:"PowerShot"
 *     - "A's+B's&C's" -> 0:"A", 1:"B", 2:"C", 2:"ABC"
 *     - "Super-Duper-XL500-42-AutoCoder!" -> 0:"Super", 1:"Duper", 2:"XL", 2:"SuperDuperXL", 3:"500" 4:"42", 5:"Auto", 6:"Coder", 6:"AutoCoder"
 *
 *  One use for WordDelimiterFilter is to help match words with different subword delimiters.
 *  For example, if the source text contained "wi-fi" one may want "wifi" "WiFi" "wi-fi" "wi+fi"
 *  queries to all match.
 *  One way of doing so is to specify combinations="1" in the analyzer
 *  used for indexing, and combinations="0" (the default) in the analyzer
 *  used for querying.  Given that the current StandardTokenizer
 *  immediately removes many intra-word delimiters, it is recommended that
 *  this filter be used after a tokenizer that does not do this
 *  (such as WhitespaceTokenizer).
 *
 *  @version $Id$
 */

final class WordDelimiterFilter extends TokenFilter {
  private final byte[] charTypeTable;

  public static final int         LOWER=0x01;
  public static final int         UPPER=0x02;
  public static final int         DIGIT=0x04;
  public static final int SUBWORD_DELIM=0x08;

  // combinations: for testing, not for setting bits
  public static final int    ALPHA=0x03;
  public static final int    ALPHANUM=0x07;

  // TODO: should there be a WORD_DELIM category for
  // chars that only separate words (no catenation of subwords
  // will be done if separated by these chars?)
  // "," would be an obvious candidate...

  static byte[] defaultWordDelimTable;
  static {
    byte[] tab = new byte[256];
    for (int i=0; i<256; i++) {
      byte code = 0;
      if (Character.isLowerCase(i)) code |= LOWER;
      else if (Character.isUpperCase(i)) code |= UPPER;
      else if (Character.isDigit(i)) code |= DIGIT;
      if (code==0) code=SUBWORD_DELIM;
      tab[i]=code;
    }
    defaultWordDelimTable = tab;
  }

  /**
   * If 1, causes parts of words to be generated:
   * <p/>
   * "PowerShot" => "Power" "Shot"
   */
  final int generateWordParts;

  /**
   * If 1, causes number subwords to be generated:
   * <p/>
   * "500-42" => "500" "42"
   */
  final int generateNumberParts;

  /**
   * If 1, causes maximum runs of word parts to be catenated:
   * <p/>
   * "wi-fi" => "wifi"
   */
  final int catenateWords;

  /**
   * If 1, causes maximum runs of number parts to be catenated:
   * <p/>
   * "500-42" => "50042"
   */
  final int catenateNumbers;

  /**
   * If 1, causes all subword parts to be catenated:
   * <p/>
   * "wi-fi-4000" => "wifi4000"
   */
  final int catenateAll;

  /**
   * If 0, causes case changes to be ignored (subwords will only be generated
   * given SUBWORD_DELIM tokens). (Defaults to 1)
   */
  final int splitOnCaseChange;

  /**
   * If 1, original words are preserved and added to the subword list (Defaults to 0)
   * <p/>
   * "500-42" => "500" "42" "500-42"
   */
  final int preserveOriginal;

  /**
   *
   * @param in Token stream to be filtered.
   * @param charTypeTable
   * @param generateWordParts If 1, causes parts of words to be generated: "PowerShot" => "Power" "Shot"
   * @param generateNumberParts If 1, causes number subwords to be generated: "500-42" => "500" "42"
   * @param catenateWords  1, causes maximum runs of word parts to be catenated: "wi-fi" => "wifi"
   * @param catenateNumbers If 1, causes maximum runs of number parts to be catenated: "500-42" => "50042"
   * @param catenateAll If 1, causes all subword parts to be catenated: "wi-fi-4000" => "wifi4000"
   * @param splitOnCaseChange 1, causes "PowerShot" to be two tokens; ("Power-Shot" remains two parts regards)
   * @param preserveOriginal If 1, includes original words in subwords: "500-42" => "500" "42" "500-42"
   */
  public WordDelimiterFilter(TokenStream in, byte[] charTypeTable, int generateWordParts, int generateNumberParts, int catenateWords, int catenateNumbers, int catenateAll, int splitOnCaseChange, int preserveOriginal) {
    super(in);
    this.generateWordParts = generateWordParts;
    this.generateNumberParts = generateNumberParts;
    this.catenateWords = catenateWords;
    this.catenateNumbers = catenateNumbers;
    this.catenateAll = catenateAll;
    this.splitOnCaseChange = splitOnCaseChange;
    this.preserveOriginal = preserveOriginal;
    this.charTypeTable = charTypeTable;
  }
  /**
   * @param in Token stream to be filtered.
   * @param generateWordParts If 1, causes parts of words to be generated: "PowerShot", "Power-Shot" => "Power" "Shot"
   * @param generateNumberParts If 1, causes number subwords to be generated: "500-42" => "500" "42"
   * @param catenateWords  1, causes maximum runs of word parts to be catenated: "wi-fi" => "wifi"
   * @param catenateNumbers If 1, causes maximum runs of number parts to be catenated: "500-42" => "50042"
   * @param catenateAll If 1, causes all subword parts to be catenated: "wi-fi-4000" => "wifi4000"
   * @param splitOnCaseChange 1, causes "PowerShot" to be two tokens; ("Power-Shot" remains two parts regards)
   * @param preserveOriginal If 1, includes original words in subwords: "500-42" => "500" "42" "500-42"
   */
  public WordDelimiterFilter(TokenStream in, int generateWordParts, int generateNumberParts, int catenateWords, int catenateNumbers, int catenateAll, int splitOnCaseChange, int preserveOriginal) {
    this(in, defaultWordDelimTable, generateWordParts, generateNumberParts, catenateWords, catenateNumbers, catenateAll, splitOnCaseChange, preserveOriginal);
  }
  /** Compatibility constructor */
  @Deprecated
  public WordDelimiterFilter(TokenStream in, byte[] charTypeTable, int generateWordParts, int generateNumberParts, int catenateWords, int catenateNumbers, int catenateAll) {
    this(in, charTypeTable, generateWordParts, generateNumberParts, catenateWords, catenateNumbers, catenateAll, 1, 0);
  }
  /** Compatibility constructor */
  @Deprecated
  public WordDelimiterFilter(TokenStream in, int generateWordParts, int generateNumberParts, int catenateWords, int catenateNumbers, int catenateAll) {
    this(in, defaultWordDelimTable, generateWordParts, generateNumberParts, catenateWords, catenateNumbers, catenateAll, 1, 0);
  }

  int charType(int ch) {
    if (ch<charTypeTable.length) {
      return charTypeTable[ch];
    } else if (Character.isLowerCase(ch)) {
      return LOWER;
    } else if (Character.isLetter(ch)) {
      return UPPER;
    } else {
      return SUBWORD_DELIM;
    }
  }

  // use the type of the first char as the type
  // of the token.
  private int tokType(Token t) {
    return charType(t.termBuffer()[0]);
  }

  // There isn't really an efficient queue class, so we will
  // just use an array for now.
  private ArrayList<Token> queue = new ArrayList<Token>(4);
  private int queuePos=0;

  // temporary working queue
  private ArrayList<Token> tlist = new ArrayList<Token>(4);


  private Token newTok(Token orig, int start, int end) {
    int startOff = orig.startOffset();
    int endOff = orig.endOffset();
    // if length by start + end offsets doesn't match the term text then assume
    // this is a synonym and don't adjust the offsets.
    if (orig.termLength() == endOff-startOff) {
      endOff = startOff + end;
      startOff += start;     
    }

    Token newTok = new Token(startOff,
            endOff,
            orig.type());
    newTok.setTermBuffer(orig.termBuffer(), start, (end - start));
    return newTok;
  }


  public final Token next(Token in) throws IOException {

    // check the queue first
    if (queuePos<queue.size()) {
      return queue.get(queuePos++);
    }

    // reset the queue if it had been previously used
    if (queuePos!=0) {
      queuePos=0;
      queue.clear();
    }


    // optimize for the common case: assume there will be
    // no subwords (just a simple word)
    //
    // Would it actually be faster to check for the common form
    // of isLetter() isLower()*, and then backtrack if it doesn't match?

    int origPosIncrement = 0;
    Token t;
    while(true) {
      // t is either returned, or a new token is made from it, so it should
      // be safe to use the next(Token) method.
      t = input.next(in);
      if (t == null) return null;

      char [] termBuffer = t.termBuffer();
      int len = t.termLength();
      int start=0;
      if (len ==0) continue;

      origPosIncrement += t.getPositionIncrement();

      // Avoid calling charType more than once for each char (basically
      // avoid any backtracking).
      // makes code slightly more difficult, but faster.
      int ch=termBuffer[start];
      int type=charType(ch);

      int numWords=0;

      while (start< len) {
        // first eat delimiters at the start of this subword
        while ((type & SUBWORD_DELIM)!=0 && ++start< len) {
          ch=termBuffer[start];
          type=charType(ch);
        }

        int pos=start;

        // save the type of the first char of the subword
        // as a way to tell what type of subword token this is (number, word, etc)
        int firstType=type;
        int lastType=type;  // type of the previously read char


        while (pos< len) {

          if (type!=lastType) {
            // check and remove "'s" from the end of a token.
            // the pattern to check for is
            //   ALPHA "'" ("s"|"S") (SUBWORD_DELIM | END)
            if ((lastType & ALPHA)!=0) {
              if (ch=='\'' && pos+1< len
                      && (termBuffer[pos+1]=='s' || termBuffer[pos+1]=='S'))
              {
                int subWordEnd=pos;
                if (pos+2>= len) {
                  // end of string detected after "'s"
                  pos+=2;
                } else {
                  // make sure that a delimiter follows "'s"
                  int ch2 = termBuffer[pos+2];
                  int type2 = charType(ch2);
                  if ((type2 & SUBWORD_DELIM)!=0) {
                    // if delimiter, move position pointer
                    // to it (skipping over "'s"
                    ch=ch2;
                    type=type2;
                    pos+=2;
                  }
                }

                queue.add(newTok(t,start,subWordEnd));
                if ((firstType & ALPHA)!=0) numWords++;
                break;
              }
            }

            // For case changes, only split on a transition from
            // lower to upper case, not vice-versa.
            // That will correctly handle the
            // case of a word starting with a capital (won't split).
            // It will also handle pluralization of
            // an uppercase word such as FOOs (won't split).

            if (splitOnCaseChange == 0 && 
                (lastType & ALPHA) != 0 && (type & ALPHA) != 0) {
              // ALPHA->ALPHA: always ignore if case isn't considered.

            } else if ((lastType & UPPER)!=0 && (type & LOWER)!=0) {
              // UPPER->LOWER: Don't split
            } else {
              // NOTE: this code currently assumes that only one flag
              // is set for each character now, so we don't have
              // to explicitly check for all the classes of transitions
              // listed below.

              // LOWER->UPPER
              // ALPHA->NUMERIC
              // NUMERIC->ALPHA
              // *->DELIMITER
              queue.add(newTok(t,start,pos));
              if ((firstType & ALPHA)!=0) numWords++;
              break;
            }
          }

          if (++pos >= len) {
            if (start==0) {
              // the subword is the whole original token, so
              // return it unchanged.
              return t;
            }

            // optimization... if this is the only token,
            // return it immediately.
            if (queue.size()==0 && preserveOriginal == 0) {
              // just adjust the text w/o changing the rest
              // of the original token
              t.setTermBuffer(termBuffer, start, len-start);
              return t;
            }

            Token newtok = newTok(t,start,pos);

            queue.add(newtok);
            if ((firstType & ALPHA)!=0) numWords++;
            break;
          }

          lastType = type;
          ch = termBuffer[pos];
          type = charType(ch);
        }

        // start of the next subword is the current position
        start=pos;
      }

      // System.out.println("##########TOKEN=" + s + " ######### WORD DELIMITER QUEUE=" + str(queue));

      final int numtok = queue.size();

      // We reached the end of the current token.
      // If the queue is empty, we should continue by reading
      // the next token
      if (numtok==0) {
        // the token might have been all delimiters, in which
        // case return it if we're meant to preserve it
        if (preserveOriginal != 0) {
          return t;
        }
        continue;
      }

      // if number of tokens is 1, there are no catenations to be done.
      if (numtok==1) {
        break;
      }


      final int numNumbers = numtok - numWords;

      // check conditions under which the current token
      // queue may be used as-is (no catenations needed)
      if (catenateAll==0    // no "everything" to catenate
        && (catenateWords==0 || numWords<=1)   // no words to catenate
        && (catenateNumbers==0 || numNumbers<=1)    // no numbers to catenate
        && (generateWordParts!=0 || numWords==0)  // word generation is on
        && (generateNumberParts!=0 || numNumbers==0)) // number generation is on
      {
        break;
      }


      // swap queue and the temporary working list, then clear the
      // queue in preparation for adding all combinations back to it.
      ArrayList<Token> tmp=tlist;
      tlist=queue;
      queue=tmp;
      queue.clear();

      if (numWords==0) {
        // all numbers
        addCombos(tlist,0,numtok,generateNumberParts!=0,catenateNumbers!=0 || catenateAll!=0, 1);
        if (queue.size() > 0 || preserveOriginal!=0) break; else continue;
      } else if (numNumbers==0) {
        // all words
        addCombos(tlist,0,numtok,generateWordParts!=0,catenateWords!=0 || catenateAll!=0, 1);
        if (queue.size() > 0 || preserveOriginal!=0) break; else continue;
      } else if (generateNumberParts==0 && generateWordParts==0 && catenateNumbers==0 && catenateWords==0) {
        // catenate all *only*
        // OPT:could be optimized to add to current queue...
        addCombos(tlist,0,numtok,false,catenateAll!=0, 1);
        if (queue.size() > 0 || preserveOriginal!=0) break; else continue;
      }

      //
      // Find all adjacent tokens of the same type.
      //
      Token tok = tlist.get(0);
      boolean isWord = (tokType(tok) & ALPHA) != 0;
      boolean wasWord=isWord;

      for(int i=0; i<numtok;) {
          int j;
          for (j=i+1; j<numtok; j++) {
            wasWord=isWord;
            tok = tlist.get(j);
            isWord = (tokType(tok) & ALPHA) != 0;
            if (isWord != wasWord) break;
          }
          if (wasWord) {
            addCombos(tlist,i,j,generateWordParts!=0,catenateWords!=0,1);
          } else {
            addCombos(tlist,i,j,generateNumberParts!=0,catenateNumbers!=0,1);
          }
          i=j;
      }

      // take care catenating all subwords
      if (catenateAll!=0) {
        addCombos(tlist,0,numtok,false,true,0);
      }

      // NOTE: in certain cases, queue may be empty (for instance, if catenate
      // and generate are both set to false).  Only exit the loop if the queue
      // is not empty.
      if (queue.size() > 0 || preserveOriginal!=0) break;
    }

    // System.out.println("##########AFTER COMBINATIONS:"+ str(queue));

    if (preserveOriginal != 0) {
      queuePos = 0;
      if (queue.size() > 0) {
        // overlap first token with the original
        queue.get(0).setPositionIncrement(0);
      }
      return t;  // return the original
    } else {
      queuePos=1;
      Token tok = queue.get(0);
      tok.setPositionIncrement(origPosIncrement);
      return tok;
    }
  }


  // index "a","b","c" as  pos0="a", pos1="b", pos2="c", pos2="abc"
  private void addCombos(List<Token> lst, int start, int end, boolean generateSubwords, boolean catenateSubwords, int posOffset) {
    if (end-start==1) {
      // always generate a word alone, even if generateSubwords=0 because
      // the catenation of all the subwords *is* the subword.
      queue.add(lst.get(start));
      return;
    }

    StringBuilder sb = null;
    if (catenateSubwords) sb=new StringBuilder();
    Token firstTok=null;
    Token tok=null;
    for (int i=start; i<end; i++) {
      tok = lst.get(i);
      if (catenateSubwords) {
        if (i==start) firstTok=tok;
        sb.append(tok.termBuffer(), 0, tok.termLength());
      }
      if (generateSubwords) {
        queue.add(tok);
      }
    }

    if (catenateSubwords) {
      Token concatTok = new Token(sb.toString(),
              firstTok.startOffset(),
              tok.endOffset(),
              firstTok.type());
      // if we indexed some other tokens, then overlap concatTok with the last.
      // Otherwise, use the value passed in as the position offset.
      concatTok.setPositionIncrement(generateSubwords==true ? 0 : posOffset);
      queue.add(concatTok);
    }
  }


  // questions:
  // negative numbers?  -42 indexed as just 42?
  // dollar sign?  $42
  // percent sign?  33%
  // downsides:  if source text is "powershot" then a query of "PowerShot" won't match!
}
