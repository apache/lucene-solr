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

import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

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
import org.apache.solr.common.util.Base64;
import org.apache.solr.schema.PreAnalyzedField.ParseResult;
import org.apache.solr.schema.PreAnalyzedField.PreAnalyzedParser;
import org.noggit.JSONUtil;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonPreAnalyzedParser implements PreAnalyzedParser {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final String VERSION = "1";
  
  public static final String VERSION_KEY = "v";
  public static final String STRING_KEY = "str";
  public static final String BINARY_KEY = "bin";
  public static final String TOKENS_KEY = "tokens";
  public static final String TOKEN_KEY = "t";
  public static final String OFFSET_START_KEY = "s";
  public static final String OFFSET_END_KEY = "e";
  public static final String POSINCR_KEY = "i";
  public static final String PAYLOAD_KEY = "p";
  public static final String TYPE_KEY = "y";
  public static final String FLAGS_KEY = "f";

  @SuppressWarnings("unchecked")
  @Override
  public ParseResult parse(Reader reader, AttributeSource parent)
      throws IOException {
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
    Object o = ObjectBuilder.fromJSONStrict(val);
    if (!(o instanceof Map)) {
      throw new IOException("Invalid JSON type " + o.getClass().getName() + ", expected Map");
    }
    Map<String,Object> map = (Map<String,Object>)o;
    // check version
    String version = (String)map.get(VERSION_KEY);
    if (version == null) {
      throw new IOException("Missing VERSION key");
    }
    if (!VERSION.equals(version)) {
      throw new IOException("Unknown VERSION '" + version + "', expected " + VERSION);
    }
    if (map.containsKey(STRING_KEY) && map.containsKey(BINARY_KEY)) {
      throw new IOException("Field cannot have both stringValue and binaryValue");
    }
    res.str = (String)map.get(STRING_KEY);
    String bin = (String)map.get(BINARY_KEY);
    if (bin != null) {
      byte[] data = Base64.base64ToByteArray(bin);
      res.bin = data;
    }
    List<Object> tokens = (List<Object>)map.get(TOKENS_KEY);
    if (tokens == null) {
      return res;
    }
    int tokenStart = 0;
    int tokenEnd = 0;
    parent.clearAttributes();
    for (Object ot : tokens) {
      tokenStart = tokenEnd + 1; // automatic increment by 1 separator
      Map<String,Object> tok = (Map<String,Object>)ot;
      boolean hasOffsetStart = false;
      boolean hasOffsetEnd = false;
      int len = -1;
      for (Entry<String,Object> e : tok.entrySet()) {
        String key = e.getKey();
        if (key.equals(TOKEN_KEY)) {
          CharTermAttribute catt = parent.addAttribute(CharTermAttribute.class);
          String str = String.valueOf(e.getValue());
          catt.append(str);
          len = str.length();
        } else if (key.equals(OFFSET_START_KEY)) {
          Object obj = e.getValue();
          hasOffsetStart = true;
          if (obj instanceof Number) {
            tokenStart = ((Number)obj).intValue();
          } else {
            try {
              tokenStart = Integer.parseInt(String.valueOf(obj));
            } catch (NumberFormatException nfe) {
              log.warn("Invalid {} attribute, skipped: '{}'", OFFSET_START_KEY, obj);
              hasOffsetStart = false;
            }
          }
        } else if (key.equals(OFFSET_END_KEY)) {
          hasOffsetEnd = true;
          Object obj = e.getValue();
          if (obj instanceof Number) {
            tokenEnd = ((Number)obj).intValue();
          } else {
            try {
              tokenEnd = Integer.parseInt(String.valueOf(obj));
            } catch (NumberFormatException nfe) {
              log.warn("Invalid {} attribute, skipped: '{}'", OFFSET_END_KEY, obj);
              hasOffsetEnd = false;
            }
          }
        } else if (key.equals(POSINCR_KEY)) {
          Object obj = e.getValue();
          int posIncr = 1;
          if (obj instanceof Number) {
            posIncr = ((Number)obj).intValue();
          } else {
            try {
              posIncr = Integer.parseInt(String.valueOf(obj));
            } catch (NumberFormatException nfe) {
              log.warn("Invalid {} attribute, skipped: '{}'", POSINCR_KEY, obj);
            }
          }
          PositionIncrementAttribute patt = parent.addAttribute(PositionIncrementAttribute.class);
          patt.setPositionIncrement(posIncr);
        } else if (key.equals(PAYLOAD_KEY)) {
          String str = String.valueOf(e.getValue());
          if (str.length() > 0) {
            byte[] data = Base64.base64ToByteArray(str);
            PayloadAttribute p = parent.addAttribute(PayloadAttribute.class);
            if (data != null && data.length > 0) {
              p.setPayload(new BytesRef(data));
            }
          }
        } else if (key.equals(FLAGS_KEY)) {
          try {
            int f = Integer.parseInt(String.valueOf(e.getValue()), 16);
            FlagsAttribute flags = parent.addAttribute(FlagsAttribute.class);
            flags.setFlags(f);
          } catch (NumberFormatException nfe) {
            log.warn("Invalid {} attribute, skipped: '{}'", FLAGS_KEY, e.getValue());
          }
        } else if (key.equals(TYPE_KEY)) {
          TypeAttribute tattr = parent.addAttribute(TypeAttribute.class);
          tattr.setType(String.valueOf(e.getValue()));
        } else {
          log.warn("Unknown attribute, skipped: {} = {}", e.getKey(), e.getValue());
        }
      }
      // handle offset attr
      OffsetAttribute offset = parent.addAttribute(OffsetAttribute.class);
      if (!hasOffsetEnd && len > -1) {
        tokenEnd = tokenStart + len;
      }
      offset.setOffset(tokenStart, tokenEnd);
      if (!hasOffsetStart) {
        tokenStart = tokenEnd + 1;
      }
      // capture state and add to result
      State state = parent.captureState();
      res.states.add(state.clone());
      // reset for reuse
      parent.clearAttributes();
    }
    return res;
  }

  @Override
  public String toFormattedString(Field f) throws IOException {
    Map<String,Object> map = new LinkedHashMap<>();
    map.put(VERSION_KEY, VERSION);
    if (f.fieldType().stored()) {
      String stringValue = f.stringValue();
      if (stringValue != null) {
        map.put(STRING_KEY, stringValue);
      }
      BytesRef binaryValue = f.binaryValue();
      if (binaryValue != null) {
        map.put(BINARY_KEY, Base64.byteArrayToBase64(binaryValue.bytes, binaryValue.offset, binaryValue.length));
      }
    }
    TokenStream ts = f.tokenStreamValue();
    if (ts != null) {
      List<Map<String,Object>> tokens = new LinkedList<>();
      while (ts.incrementToken()) {
        Iterator<Class<? extends Attribute>> it = ts.getAttributeClassesIterator();
        String cTerm = null;
        String tTerm = null;
        Map<String,Object> tok = new TreeMap<>();
        while (it.hasNext()) {
          Class<? extends Attribute> cl = it.next();
          Attribute att = ts.getAttribute(cl);
          if (att == null) {
            continue;
          }
          if (cl.isAssignableFrom(CharTermAttribute.class)) {
            CharTermAttribute catt = (CharTermAttribute)att;
            cTerm = new String(catt.buffer(), 0, catt.length());
          } else if (cl.isAssignableFrom(TermToBytesRefAttribute.class)) {
            TermToBytesRefAttribute tatt = (TermToBytesRefAttribute)att;
            tTerm = tatt.getBytesRef().utf8ToString();
          } else {
            if (cl.isAssignableFrom(FlagsAttribute.class)) {
              tok.put(FLAGS_KEY, Integer.toHexString(((FlagsAttribute)att).getFlags()));
            } else if (cl.isAssignableFrom(OffsetAttribute.class)) {
              tok.put(OFFSET_START_KEY, ((OffsetAttribute)att).startOffset());
              tok.put(OFFSET_END_KEY, ((OffsetAttribute)att).endOffset());
            } else if (cl.isAssignableFrom(PayloadAttribute.class)) {
              BytesRef p = ((PayloadAttribute)att).getPayload();
              if (p != null && p.length > 0) {
                tok.put(PAYLOAD_KEY, Base64.byteArrayToBase64(p.bytes, p.offset, p.length));
              }
            } else if (cl.isAssignableFrom(PositionIncrementAttribute.class)) {
              tok.put(POSINCR_KEY, ((PositionIncrementAttribute)att).getPositionIncrement());
            } else if (cl.isAssignableFrom(TypeAttribute.class)) {
              tok.put(TYPE_KEY, ((TypeAttribute)att).type());
            } else {
              tok.put(cl.getName(), att.toString());
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
          tok.put(TOKEN_KEY, term);
        }
        tokens.add(tok);
      }
      map.put(TOKENS_KEY, tokens);
    }
    return JSONUtil.toJSON(map, -1);
  }
  
}
