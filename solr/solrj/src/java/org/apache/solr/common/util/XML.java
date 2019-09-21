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
package org.apache.solr.common.util;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class XML {

  //
  // copied from some of my personal code...  -YCS
  // table created from python script.
  // only have to escape quotes in attribute values, and don't really have to escape '>'
  // many chars less than 0x20 are *not* valid XML, even when escaped!
  // for example, <foo>&#0;<foo> is invalid XML.
  private static final String[] chardata_escapes=
  {"#0;","#1;","#2;","#3;","#4;","#5;","#6;","#7;","#8;",null,null,"#11;","#12;",null,"#14;","#15;","#16;","#17;","#18;","#19;","#20;","#21;","#22;","#23;","#24;","#25;","#26;","#27;","#28;","#29;","#30;","#31;",null,null,null,null,null,null,"&amp;",null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,"&lt;",null,"&gt;"};

  private static final String[] attribute_escapes=
  {"#0;","#1;","#2;","#3;","#4;","#5;","#6;","#7;","#8;",null,null,"#11;","#12;",null,"#14;","#15;","#16;","#17;","#18;","#19;","#20;","#21;","#22;","#23;","#24;","#25;","#26;","#27;","#28;","#29;","#30;","#31;",null,null,"&quot;",null,null,null,"&amp;",null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,"&lt;"};

  /*
   #Simple python script used to generate the escape table above.  -YCS
   #
   #use individual char arrays or one big char array for better efficiency
   # or byte array?
   #other={'&':'amp', '<':'lt', '>':'gt', "'":'apos', '"':'quot'}
   #
   other={'&':'amp', '<':'lt'}

   maxi=ord(max(other.keys()))+1
   table=[None] * maxi
   #NOTE: invalid XML chars are "escaped" as #nn; *not* &#nn; because
   #a real XML escape would cause many strict XML parsers to choke.
   for i in range(0x20): table[i]='#%d;' % i
   for i in '\n\r\t ': table[ord(i)]=None
   for k,v in other.items():
    table[ord(k)]='&%s;' % v

   result=""
   for i in range(maxi):
     val=table[i]
     if not val: val='null'
     else: val='"%s"' % val
     result += val + ','

   print result
  */


  public static void escapeCharData(String str, Writer out) throws IOException {
    escape(str, out, chardata_escapes);
  }

  public static void escapeAttributeValue(String str, Writer out) throws IOException {
    escape(str, out, attribute_escapes);
  }

  public static void escapeAttributeValue(char [] chars, int start, int length, Writer out) throws IOException {
    escape(chars, start, length, out, attribute_escapes);
  }

  /** does NOT escape character data in val; it must already be valid XML.  Attributes are always escaped. */
  public final static void writeUnescapedXML(Writer out, String tag, String val, Object... attrs) throws IOException {
    writeXML(out, tag, (writer1) -> writer1.write(val), attrs);
  }

  /** escapes character data in val and attributes */
  public final static void writeXML(Writer out, String tag, String val, Object... attrs) throws IOException {
    final Writable writable = val != null ? (writer1) -> XML.escapeCharData(val, writer1) : null;
    writeXML(out, tag, writable, attrs);
  }

  /** escapes character data in val and attributes */
  public static void writeXML(Writer out, String tag, String val, Map<String, String> attrs) throws IOException {
    writeXML(out, tag, val, attrs.entrySet().stream().flatMap((entry) -> Stream.of(entry.getKey(), entry.getValue())).toArray());
  }

  /** @lucene.internal */
  public final static void writeXML(Writer out, String tag, Writable valWritable, Object... attrs) throws IOException {
    out.write('<');
    out.write(tag);
    final int attrsLen = attrs == null ? 0 : attrs.length;
    for (int i = 0; i< attrsLen; i++) {
      out.write(' ');
      out.write(attrs[i++].toString());
      out.write('=');
      out.write('"');
      escapeAttributeValue(attrs[i].toString(), out);
      out.write('"');
    }
    if (valWritable == null) {
      out.write('/');
      out.write('>');
    } else {
      out.write('>');
      valWritable.write(out);
      out.write('<');
      out.write('/');
      out.write(tag);
      out.write('>');
    }
  }

  @FunctionalInterface
  public interface Writable {
    void write(Writer w) throws IOException;
  }

  private static void escape(char [] chars, int offset, int length, Writer out, String [] escapes) throws IOException{
     for (int i=offset; i<length; i++) {
      char ch = chars[i];
      if (ch<escapes.length) {
        String replacement = escapes[ch];
        if (replacement != null) {
          out.write(replacement);
          continue;
        }
      }
      out.write(ch);
    }
  }

  private static void escape(String str, Writer out, String[] escapes) throws IOException {
    for (int i=0; i<str.length(); i++) {
      char ch = str.charAt(i);
      if (ch<escapes.length) {
        String replacement = escapes[ch];
        if (replacement != null) {
          out.write(replacement);
          continue;
        }
      }
      out.write(ch);
    }
  }
}
