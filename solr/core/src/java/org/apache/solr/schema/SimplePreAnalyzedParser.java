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
package org.apache.solr.schema;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.AttributeSource.State;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.PreAnalyzedField.ParseResult;
import org.apache.solr.schema.PreAnalyzedField.PreAnalyzedParser;

/**
 * Simple plain text format parser for {@link PreAnalyzedField}.
 * <h2>Serialization format</h2>
 * <p>The format of the serialization is as follows:
 * <pre>
 * content ::= version (stored)? tokens
 * version ::= digit+ " "
 * ; stored field value - any "=" inside must be escaped!
 * stored ::= "=" text "="
 * tokens ::= (token ((" ") + token)*)*
 * token ::= text ("," attrib)*
 * attrib ::= name '=' value
 * name ::= text
 * value ::= text
 * </pre>
 * <p>Special characters in "text" values can be escaped
 * using the escape character \ . The following escape sequences are recognized:
 * <pre>
 * "\ " - literal space character
 * "\," - literal , character
 * "\=" - literal = character
 * "\\" - literal \ character
 * "\n" - newline
 * "\r" - carriage return
 * "\t" - horizontal tab
 * </pre>
 * Please note that Unicode sequences (e.g. &#92;u0001) are not supported.
 * <h2>Supported attribute names</h2>
 * The following token attributes are supported, and identified with short
 * symbolic names:
 * <pre>
 * i - position increment (integer)
 * s - token offset, start position (integer)
 * e - token offset, end position (integer)
 * t - token type (string)
 * f - token flags (hexadecimal integer)
 * p - payload (bytes in hexadecimal format; whitespace is ignored)
 * </pre>
 * Token offsets are tracked and implicitly added to the token stream -
 * the start and end offsets consider only the term text and whitespace,
 * and exclude the space taken by token attributes.
 * <h2>Example token streams</h2>
 * <pre>
 * 1 one two three
  - version 1
  - stored: 'null'
  - tok: '(term=one,startOffset=0,endOffset=3)'
  - tok: '(term=two,startOffset=4,endOffset=7)'
  - tok: '(term=three,startOffset=8,endOffset=13)'
 1 one  two   three 
  - version 1
  - stored: 'null'
  - tok: '(term=one,startOffset=0,endOffset=3)'
  - tok: '(term=two,startOffset=5,endOffset=8)'
  - tok: '(term=three,startOffset=11,endOffset=16)'
1 one,s=123,e=128,i=22  two three,s=20,e=22
  - version 1
  - stored: 'null'
  - tok: '(term=one,positionIncrement=22,startOffset=123,endOffset=128)'
  - tok: '(term=two,positionIncrement=1,startOffset=5,endOffset=8)'
  - tok: '(term=three,positionIncrement=1,startOffset=20,endOffset=22)'
1 \ one\ \,,i=22,a=\, two\=

  \n,\ =\   \
  - version 1
  - stored: 'null'
  - tok: '(term= one ,,positionIncrement=22,startOffset=0,endOffset=6)'
  - tok: '(term=two=

  
 ,positionIncrement=1,startOffset=7,endOffset=15)'
  - tok: '(term=\,positionIncrement=1,startOffset=17,endOffset=18)'
1 ,i=22 ,i=33,s=2,e=20 , 
  - version 1
  - stored: 'null'
  - tok: '(term=,positionIncrement=22,startOffset=0,endOffset=0)'
  - tok: '(term=,positionIncrement=33,startOffset=2,endOffset=20)'
  - tok: '(term=,positionIncrement=1,startOffset=2,endOffset=2)'
1 =This is the stored part with \= 
 \n    \t escapes.=one two three 
  - version 1
  - stored: 'This is the stored part with = 
 \n    \t escapes.'
  - tok: '(term=one,startOffset=0,endOffset=3)'
  - tok: '(term=two,startOffset=4,endOffset=7)'
  - tok: '(term=three,startOffset=8,endOffset=13)'
1 ==
  - version 1
  - stored: ''
  - (no tokens)
1 =this is a test.=
  - version 1
  - stored: 'this is a test.'
  - (no tokens)
 * </pre> 
 */
public final class SimplePreAnalyzedParser implements PreAnalyzedParser {
  static final String VERSION = "1";
  
  private static class Tok {
    StringBuilder token = new StringBuilder();
    Map<String, String> attr = new HashMap<>();
    
    public boolean isEmpty() {
      return token.length() == 0 && attr.size() == 0;
    }
    
    public void reset() {
      token.setLength(0);
      attr.clear();
    }
    
    @Override
    public String toString() {
      return "tok='" + token + "',attr=" + attr;
    }
  }
  
  // parser state
  private static enum S {TOKEN, NAME, VALUE, UNDEF};
  
  private static final byte[] EMPTY_BYTES = new byte[0];
  
  /** Utility method to convert a hex string to a byte array. */
  static byte[] hexToBytes(String hex) {
    if (hex == null) {
      return EMPTY_BYTES;
    }
    hex = hex.replaceAll("\\s+", "");
    if (hex.length() == 0) {
      return EMPTY_BYTES;
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream(hex.length() / 2);
    byte b;
    for (int i = 0; i < hex.length(); i++) {
      int high = charToNibble(hex.charAt(i));
      int low = 0;
      if (i < hex.length() - 1) {
        i++;
        low = charToNibble(hex.charAt(i));
      }
      b = (byte)(high << 4 | low);
      baos.write(b);
    }
    return baos.toByteArray();
  }

  static final int charToNibble(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    } else if (c >= 'a' && c <= 'f') {
      return 0xa + (c - 'a');
    } else if (c >= 'A' && c <= 'F') {
      return 0xA + (c - 'A');
    } else {
      throw new RuntimeException("Not a hex character: '" + c + "'");
    }
  }
  
  static String bytesToHex(byte bytes[], int offset, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = offset; i < offset + length; ++i) {
      sb.append(Integer.toHexString(0x0100 + (bytes[i] & 0x00FF))
                       .substring(1));
    }
    return sb.toString();
  }
  
  public SimplePreAnalyzedParser() {
    
  }

  @Override
  public ParseResult parse(Reader reader, AttributeSource parent) throws IOException {
    ParseResult res = new ParseResult();
    StringBuilder sb = new StringBuilder();
    char[] buf = new char[128];
    int cnt;
    while ((cnt = reader.read(buf)) > 0) {
      sb.append(buf, 0, cnt);
    }
    String val = sb.toString();
    // empty string - accept even without version number
    if (val.length() == 0) {
      return res;
    }
    // first consume the version
    int idx = val.indexOf(' ');
    if (idx == -1) {
      throw new IOException("Missing VERSION token");
    }
    String version = val.substring(0, idx);
    if (!VERSION.equals(version)) {
      throw new IOException("Unknown VERSION " + version);
    }
    val = val.substring(idx + 1);
    // then consume the optional stored part
    int tsStart = 0;
    boolean hasStored = false;
    StringBuilder storedBuf = new StringBuilder();
    if (val.charAt(0) == '=') {
      hasStored = true;
      if (val.length() > 1) {
        for (int i = 1; i < val.length(); i++) {
          char c = val.charAt(i);
          if (c == '\\') {
            if (i < val.length() - 1) {
              c = val.charAt(++i);
              if (c == '=') { // we recognize only \= escape in the stored part
                storedBuf.append('=');
              } else {
                storedBuf.append('\\');
                storedBuf.append(c);
                continue;
              }
            } else {
              storedBuf.append(c);
              continue;
            }
          } else if (c == '=') {
            // end of stored text
            tsStart = i + 1;
            break;
          } else {
            storedBuf.append(c);
          }
        }
        if (tsStart == 0) { // missing end-of-stored marker
          throw new IOException("Missing end marker of stored part");
        }
      } else {
        throw new IOException("Unexpected end of stored field");
      }
    }
    if (hasStored) {
      res.str = storedBuf.toString();
    }
    Tok tok = new Tok();
    StringBuilder attName = new StringBuilder();
    StringBuilder attVal = new StringBuilder();
    // parser state
    S s = S.UNDEF;
    int lastPos = 0;
    for (int i = tsStart; i < val.length(); i++) {
      char c = val.charAt(i);
      if (c == ' ') {
        // collect leftovers
        switch (s) {
        case VALUE :
          if (attVal.length() == 0) {
            throw new IOException("Unexpected character '" + c + "' at position " + i + " - empty value of attribute.");
          }
          if (attName.length() > 0) {
            tok.attr.put(attName.toString(), attVal.toString());
          }
          break;
        case NAME: // attr name without a value ?
          if (attName.length() > 0) {
            throw new IOException("Unexpected character '" + c + "' at position " + i + " - missing attribute value.");
          } else {
            // accept missing att name and value
          }
          break;
        case TOKEN:
        case UNDEF:
          // do nothing, advance to next token
        }
        attName.setLength(0);
        attVal.setLength(0);
        if (!tok.isEmpty() || s == S.NAME) {
          AttributeSource.State state = createState(parent, tok, lastPos);
          if (state != null) res.states.add(state.clone());
        }
        // reset tok
        s = S.UNDEF;
        tok.reset();
        // skip
        lastPos++;
        continue;
      }
      StringBuilder tgt = null;
      switch (s) {
      case TOKEN:
        tgt = tok.token;
        break;
      case NAME:
        tgt = attName;
        break;
      case VALUE:
        tgt = attVal;
        break;
      case UNDEF:
        tgt = tok.token;
        s = S.TOKEN;
      }
      if (c == '\\') {
        if (s == S.TOKEN) lastPos++;
        if (i >= val.length() - 1) { // end
          
          tgt.append(c);
          continue;
        } else {
          c = val.charAt(++i);
          switch (c) {
          case '\\' :
          case '=' :
          case ',' :
          case ' ' :
            tgt.append(c);
            break;
          case 'n':
            tgt.append('\n');
            break;
          case 'r':
            tgt.append('\r');
            break;
          case 't':
            tgt.append('\t');
            break;
          default:
            tgt.append('\\');
            tgt.append(c);
            lastPos++;
          }
        }
      } else {
        // state switch
        if (c == ',') {
          if (s == S.TOKEN) {
            s = S.NAME;
          } else if (s == S.VALUE) { // end of value, start of next attr
            if (attVal.length() == 0) {
              throw new IOException("Unexpected character '" + c + "' at position " + i + " - empty value of attribute.");
            }
            if (attName.length() > 0 && attVal.length() > 0) {
              tok.attr.put(attName.toString(), attVal.toString());
            }
            // reset
            attName.setLength(0);
            attVal.setLength(0);
            s = S.NAME;
          } else {
            throw new IOException("Unexpected character '" + c + "' at position " + i + " - missing attribute value.");
          }
        } else if (c == '=') {
          if (s == S.NAME) {
            s = S.VALUE;
          } else {
            throw new IOException("Unexpected character '" + c + "' at position " + i + " - empty value of attribute.");
          }
        } else {
          tgt.append(c);
          if (s == S.TOKEN) lastPos++;
        }
      }
    }
    // collect leftovers
    if (!tok.isEmpty() || s == S.NAME || s == S.VALUE) {
      // remaining attrib?
      if (s == S.VALUE) {
        if (attName.length() > 0 && attVal.length() > 0) {
          tok.attr.put(attName.toString(), attVal.toString());
        }        
      }
      AttributeSource.State state = createState(parent, tok, lastPos);
      if (state != null) res.states.add(state.clone());
    }
    return res;
  }
  
  private static AttributeSource.State createState(AttributeSource a, Tok state, int tokenEnd) {
    a.clearAttributes();
    CharTermAttribute termAtt = a.addAttribute(CharTermAttribute.class);
    char[] tokChars = state.token.toString().toCharArray();
    termAtt.copyBuffer(tokChars, 0, tokChars.length);
    int tokenStart = tokenEnd - state.token.length();
    for (Entry<String, String> e : state.attr.entrySet()) {
      String k = e.getKey();
      if (k.equals("i")) {
        // position increment
        int incr = Integer.parseInt(e.getValue());
        PositionIncrementAttribute posIncr = a.addAttribute(PositionIncrementAttribute.class);
        posIncr.setPositionIncrement(incr);
      } else if (k.equals("s")) {
        tokenStart = Integer.parseInt(e.getValue());
      } else if (k.equals("e")) {
        tokenEnd = Integer.parseInt(e.getValue());
      } else if (k.equals("y")) {
        TypeAttribute type = a.addAttribute(TypeAttribute.class);
        type.setType(e.getValue());
      } else if (k.equals("f")) {
        FlagsAttribute flags = a.addAttribute(FlagsAttribute.class);
        int f = Integer.parseInt(e.getValue(), 16);
        flags.setFlags(f);
      } else if (k.equals("p")) {
        PayloadAttribute p = a.addAttribute(PayloadAttribute.class);
        byte[] data = hexToBytes(e.getValue());
        if (data != null && data.length > 0) {
          p.setPayload(new BytesRef(data));
        }
      } else {
        // unknown attribute
      }
    }
    // handle offset attr
    OffsetAttribute offset = a.addAttribute(OffsetAttribute.class);
    offset.setOffset(tokenStart, tokenEnd);
    State resState = a.captureState();
    a.clearAttributes();
    return resState;
  }

  @Override
  public String toFormattedString(Field f) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(VERSION + " ");
    if (f.fieldType().stored()) {
      String s = f.stringValue();
      if (s != null) {
        // encode the equals sign
        s = s.replaceAll("=", "\\=");
        sb.append('=');
        sb.append(s);
        sb.append('=');
      }
    }
    TokenStream ts = f.tokenStreamValue();
    if (ts != null) {
      StringBuilder tok = new StringBuilder();
      boolean next = false;
      while (ts.incrementToken()) {
        if (next) {
          sb.append(' ');
        } else {
          next = true;
        }
        tok.setLength(0);
        Iterator<Class<? extends Attribute>> it = ts.getAttributeClassesIterator();
        String cTerm = null;
        String tTerm = null;
        while (it.hasNext()) {
          Class<? extends Attribute> cl = it.next();
          Attribute att = ts.getAttribute(cl);
          if (att == null) {
            continue;
          }
          if (cl.isAssignableFrom(CharTermAttribute.class)) {
            CharTermAttribute catt = (CharTermAttribute)att;
            cTerm = escape(catt.buffer(), catt.length());
          } else if (cl.isAssignableFrom(TermToBytesRefAttribute.class)) {
            TermToBytesRefAttribute tatt = (TermToBytesRefAttribute)att;
            char[] tTermChars = tatt.getBytesRef().utf8ToString().toCharArray();
            tTerm = escape(tTermChars, tTermChars.length);
          } else {
            if (tok.length() > 0) tok.append(',');
            if (cl.isAssignableFrom(FlagsAttribute.class)) {
              tok.append("f=").append(Integer.toHexString(((FlagsAttribute) att).getFlags()));
            } else if (cl.isAssignableFrom(OffsetAttribute.class)) {
              tok.append("s=").append(((OffsetAttribute) att).startOffset()).append(",e=").append(((OffsetAttribute) att).endOffset());
            } else if (cl.isAssignableFrom(PayloadAttribute.class)) {
              BytesRef p = ((PayloadAttribute)att).getPayload();
              if (p != null && p.length > 0) {
                tok.append("p=").append(bytesToHex(p.bytes, p.offset, p.length));
              } else if (tok.length() > 0) {
                tok.setLength(tok.length() - 1); // remove the last comma
              }
            } else if (cl.isAssignableFrom(PositionIncrementAttribute.class)) {
              tok.append("i=").append(((PositionIncrementAttribute) att).getPositionIncrement());
            } else if (cl.isAssignableFrom(TypeAttribute.class)) {
              tok.append("y=").append(escape(((TypeAttribute) att).type()));
            } else {
              
              tok.append(cl.getName()).append('=').append(escape(att.toString()));
            }
          }
        }
        String term = null;
        if (cTerm != null) {
          term = cTerm;
        } else {
          term = tTerm;
        }
        if (term != null && term.length() > 0) {
          if (tok.length() > 0) {
            tok.insert(0, term + ",");
          } else {
            tok.insert(0, term);
          }
        }
        sb.append(tok);
      }
    }
    return sb.toString();
  }
    
  String escape(String val) {
    return escape(val.toCharArray(), val.length());
  }
  
  String escape(char[] val, int len) {
    if (val == null || len == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      switch (val[i]) {
      case '\\' :
      case '=' :
      case ',' :
      case ' ' :
        sb.append('\\');
        sb.append(val[i]);
        break;
      case '\n' :
        sb.append('\\');
        sb.append('n');
        break;
      case '\r' :
        sb.append('\\');
        sb.append('r');
        break;
      case '\t' :
        sb.append('\\');
        sb.append('t');
        break;
      default:
        sb.append(val[i]);
      }
    }
    return sb.toString();
  }
  
}
