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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import junit.framework.TestCase;

/**
 * General token testing helper functions
 */
public abstract class BaseTokenTestCase extends AnalysisTestCase
{
  public static String tsToString(TokenStream in) throws IOException {
    StringBuilder out = new StringBuilder();
    Token t = in.next();
    if (null != t)
      out.append(new String(t.termBuffer(), 0, t.termLength()));
    
    for (t = in.next(); null != t; t = in.next()) {
      out.append(" ").append(new String(t.termBuffer(), 0, t.termLength()));
    }
    in.close();
    return out.toString();
  }

  public List<String> tok2str(Iterable<Token> tokLst) {
    ArrayList<String> lst = new ArrayList<String>();
    for ( Token t : tokLst ) {
      lst.add( new String(t.termBuffer(), 0, t.termLength()));
    }
    return lst;
  }


  public void assertTokEqual(List<Token> a, List<Token> b) {
    assertTokEq(a,b,false);
    assertTokEq(b,a,false);
  }

  public void assertTokEqualOff(List<Token> a, List<Token> b) {
    assertTokEq(a,b,true);
    assertTokEq(b,a,true);
  }

  private void assertTokEq(List<Token> a, List<Token> b, boolean checkOff) {
    int pos=0;
    for (Iterator iter = a.iterator(); iter.hasNext();) {
      Token tok = (Token)iter.next();
      pos += tok.getPositionIncrement();
      if (!tokAt(b, new String(tok.termBuffer(), 0, tok.termLength()), pos
              , checkOff ? tok.startOffset() : -1
              , checkOff ? tok.endOffset() : -1
              )) 
      {
        fail(a + "!=" + b);
      }
    }
  }

  public boolean tokAt(List<Token> lst, String val, int tokPos, int startOff, int endOff) {
    int pos=0;
    for (Iterator iter = lst.iterator(); iter.hasNext();) {
      Token tok = (Token)iter.next();
      pos += tok.getPositionIncrement();
      if (pos==tokPos && new String(tok.termBuffer(), 0, tok.termLength()).equals(val)
          && (startOff==-1 || tok.startOffset()==startOff)
          && (endOff  ==-1 || tok.endOffset()  ==endOff  )
           )
      {
        return true;
      }
    }
    return false;
  }


  /***
   * Return a list of tokens according to a test string format:
   * a b c  =>  returns List<Token> [a,b,c]
   * a/b   => tokens a and b share the same spot (b.positionIncrement=0)
   * a,3/b/c => a,b,c all share same position (a.positionIncrement=3, b.positionIncrement=0, c.positionIncrement=0)
   * a,1,10,11  => "a" with positionIncrement=1, startOffset=10, endOffset=11
   */
  public List<Token> tokens(String str) {
    String[] arr = str.split(" ");
    List<Token> result = new ArrayList<Token>();
    for (int i=0; i<arr.length; i++) {
      String[] toks = arr[i].split("/");
      String[] params = toks[0].split(",");

      int posInc;
      int start;
      int end;

      if (params.length > 1) {
        posInc = Integer.parseInt(params[1]);
      } else {
        posInc = 1;
      }

      if (params.length > 2) {
        start = Integer.parseInt(params[2]);
      } else {
        start = 0;
      }

      if (params.length > 3) {
        end = Integer.parseInt(params[3]);
      } else {
        end = start + params[0].length();
      }

      Token t = new Token(params[0],start,end,"TEST");
      t.setPositionIncrement(posInc);
      
      result.add(t);
      for (int j=1; j<toks.length; j++) {
        t = new Token(toks[j],0,0,"TEST");
        t.setPositionIncrement(0);
        result.add(t);
      }
    }
    return result;
  }

  //------------------------------------------------------------------------
  // These may be useful beyond test cases...
  //------------------------------------------------------------------------

  static List<Token> getTokens(TokenStream tstream) throws IOException {
    List<Token> tokens = new ArrayList<Token>();
    while (true) {
      Token t = tstream.next();
      if (t==null) break;
      tokens.add(t);
    }
    return tokens;
  }

  public static class IterTokenStream extends TokenStream {
    Iterator<Token> toks;
    public IterTokenStream(Token... toks) {
      this.toks = Arrays.asList(toks).iterator();
    }
    public IterTokenStream(Iterable<Token> toks) {
      this.toks = toks.iterator();
    }
    public IterTokenStream(Iterator<Token> toks) {
      this.toks = toks;
    }
    public IterTokenStream(String ... text) {
      int off = 0;
      ArrayList<Token> t = new ArrayList<Token>( text.length );
      for( String txt : text ) {
        t.add( new Token( txt, off, off+txt.length() ) );
        off += txt.length() + 2;
      }
      this.toks = t.iterator();
    }
    @Override
    public Token next() {
      if (toks.hasNext()) {
        return toks.next();
      }
      return null;
    }
  }
}
