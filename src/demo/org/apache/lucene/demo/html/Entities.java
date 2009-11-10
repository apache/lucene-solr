package org.apache.lucene.demo.html;

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

import java.util.HashMap;
import java.util.Map;

public class Entities {
  static final Map<String,String> decoder = new HashMap<String,String>(300);
  static final String[]  encoder = new String[0x100];

  static final String decode(String entity) {
    if (entity.charAt(entity.length()-1) == ';')  // remove trailing semicolon
      entity = entity.substring(0, entity.length()-1);
    if (entity.charAt(1) == '#') {
      int start = 2;
      int radix = 10;
      if (entity.charAt(2) == 'X' || entity.charAt(2) == 'x') {
	start++;
	radix = 16;
      }
      Character c =
	new Character((char)Integer.parseInt(entity.substring(start), radix));
      return c.toString();
    } else {
      String s = (String)decoder.get(entity);
      if (s != null)
	return s;
      else return "";
    }
  }

  public static final String encode(String s) {
    int length = s.length();
    StringBuffer buffer = new StringBuffer(length * 2);
    for (int i = 0; i < length; i++) {
      char c = s.charAt(i);
      int j = (int)c;
      if (j < 0x100 && encoder[j] != null) {
	buffer.append(encoder[j]);		  // have a named encoding
	buffer.append(';');
      } else if (j < 0x80) {
	buffer.append(c);			  // use ASCII value
      } else {
	buffer.append("&#");			  // use numeric encoding
	buffer.append((int)c);
	buffer.append(';');
      }
    }
    return buffer.toString();
  }

  static final void add(String entity, int value) {
    decoder.put(entity, (new Character((char)value)).toString());
    if (value < 0x100)
      encoder[value] = entity;
  }

  static {
    add("&nbsp",   160);
    add("&iexcl",  161);
    add("&cent",   162);
    add("&pound",  163);
    add("&curren", 164);
    add("&yen",    165);
    add("&brvbar", 166);
    add("&sect",   167);
    add("&uml",    168);
    add("&copy",   169);
    add("&ordf",   170);
    add("&laquo",  171);
    add("&not",    172);
    add("&shy",    173);
    add("&reg",    174);
    add("&macr",   175);
    add("&deg",    176);
    add("&plusmn", 177);
    add("&sup2",   178);
    add("&sup3",   179);
    add("&acute",  180);
    add("&micro",  181);
    add("&para",   182);
    add("&middot", 183);
    add("&cedil",  184);
    add("&sup1",   185);
    add("&ordm",   186);
    add("&raquo",  187);
    add("&frac14", 188);
    add("&frac12", 189);
    add("&frac34", 190);
    add("&iquest", 191);
    add("&Agrave", 192);
    add("&Aacute", 193);
    add("&Acirc",  194);
    add("&Atilde", 195);
    add("&Auml",   196);
    add("&Aring",  197);
    add("&AElig",  198);
    add("&Ccedil", 199);
    add("&Egrave", 200);
    add("&Eacute", 201);
    add("&Ecirc",  202);
    add("&Euml",   203);
    add("&Igrave", 204);
    add("&Iacute", 205);
    add("&Icirc",  206);
    add("&Iuml",   207);
    add("&ETH",    208);
    add("&Ntilde", 209);
    add("&Ograve", 210);
    add("&Oacute", 211);
    add("&Ocirc",  212);
    add("&Otilde", 213);
    add("&Ouml",   214);
    add("&times",  215);
    add("&Oslash", 216);
    add("&Ugrave", 217);
    add("&Uacute", 218);
    add("&Ucirc",  219);
    add("&Uuml",   220);
    add("&Yacute", 221);
    add("&THORN",  222);
    add("&szlig",  223);
    add("&agrave", 224);
    add("&aacute", 225);
    add("&acirc",  226);
    add("&atilde", 227);
    add("&auml",   228);
    add("&aring",  229);
    add("&aelig",  230);
    add("&ccedil", 231);
    add("&egrave", 232);
    add("&eacute", 233);
    add("&ecirc",  234);
    add("&euml",   235);
    add("&igrave", 236);
    add("&iacute", 237);
    add("&icirc",  238);
    add("&iuml",   239);
    add("&eth",    240);
    add("&ntilde", 241);
    add("&ograve", 242);
    add("&oacute", 243);
    add("&ocirc",  244);
    add("&otilde", 245);
    add("&ouml",   246);
    add("&divide", 247);
    add("&oslash", 248);
    add("&ugrave", 249);
    add("&uacute", 250);
    add("&ucirc",  251);
    add("&uuml",   252);
    add("&yacute", 253);
    add("&thorn",  254);
    add("&yuml",   255);
    add("&fnof",   402);
    add("&Alpha",  913);
    add("&Beta",   914);
    add("&Gamma",  915);
    add("&Delta",  916);
    add("&Epsilon",917);
    add("&Zeta",   918);
    add("&Eta",    919);
    add("&Theta",  920);
    add("&Iota",   921);
    add("&Kappa",  922);
    add("&Lambda", 923);
    add("&Mu",     924);
    add("&Nu",     925);
    add("&Xi",     926);
    add("&Omicron",927);
    add("&Pi",     928);
    add("&Rho",    929);
    add("&Sigma",  931);
    add("&Tau",    932);
    add("&Upsilon",933);
    add("&Phi",    934);
    add("&Chi",    935);
    add("&Psi",    936);
    add("&Omega",  937);
    add("&alpha",  945);
    add("&beta",   946);
    add("&gamma",  947);
    add("&delta",  948);
    add("&epsilon",949);
    add("&zeta",   950);
    add("&eta",    951);
    add("&theta",  952);
    add("&iota",   953);
    add("&kappa",  954);
    add("&lambda", 955);
    add("&mu",     956);
    add("&nu",     957);
    add("&xi",     958);
    add("&omicron",959);
    add("&pi",     960);
    add("&rho",    961);
    add("&sigmaf", 962);
    add("&sigma",  963);
    add("&tau",    964);
    add("&upsilon",965);
    add("&phi",    966);
    add("&chi",    967);
    add("&psi",    968);
    add("&omega",  969);
    add("&thetasym",977);
    add("&upsih",  978);
    add("&piv",    982);
    add("&bull",   8226);
    add("&hellip", 8230);
    add("&prime",  8242);
    add("&Prime",  8243);
    add("&oline",  8254);
    add("&frasl",  8260);
    add("&weierp", 8472);
    add("&image",  8465);
    add("&real",   8476);
    add("&trade",  8482);
    add("&alefsym",8501);
    add("&larr",   8592);
    add("&uarr",   8593);
    add("&rarr",   8594);
    add("&darr",   8595);
    add("&harr",   8596);
    add("&crarr",  8629);
    add("&lArr",   8656);
    add("&uArr",   8657);
    add("&rArr",   8658);
    add("&dArr",   8659);
    add("&hArr",   8660);
    add("&forall", 8704);
    add("&part",   8706);
    add("&exist",  8707);
    add("&empty",  8709);
    add("&nabla",  8711);
    add("&isin",   8712);
    add("&notin",  8713);
    add("&ni",     8715);
    add("&prod",   8719);
    add("&sum",    8721);
    add("&minus",  8722);
    add("&lowast", 8727);
    add("&radic",  8730);
    add("&prop",   8733);
    add("&infin",  8734);
    add("&ang",    8736);
    add("&and",    8743);
    add("&or",     8744);
    add("&cap",    8745);
    add("&cup",    8746);
    add("&int",    8747);
    add("&there4", 8756);
    add("&sim",    8764);
    add("&cong",   8773);
    add("&asymp",  8776);
    add("&ne",     8800);
    add("&equiv",  8801);
    add("&le",     8804);
    add("&ge",     8805);
    add("&sub",    8834);
    add("&sup",    8835);
    add("&nsub",   8836);
    add("&sube",   8838);
    add("&supe",   8839);
    add("&oplus",  8853);
    add("&otimes", 8855);
    add("&perp",   8869);
    add("&sdot",   8901);
    add("&lceil",  8968);
    add("&rceil",  8969);
    add("&lfloor", 8970);
    add("&rfloor", 8971);
    add("&lang",   9001);
    add("&rang",   9002);
    add("&loz",    9674);
    add("&spades", 9824);
    add("&clubs",  9827);
    add("&hearts", 9829);
    add("&diams",  9830);
    add("&quot",   34);
    add("&amp",    38);
    add("&lt",     60);
    add("&gt",     62);
    add("&OElig",  338);
    add("&oelig",  339);
    add("&Scaron", 352);
    add("&scaron", 353);
    add("&Yuml",   376);
    add("&circ",   710);
    add("&tilde",  732);
    add("&ensp",   8194);
    add("&emsp",   8195);
    add("&thinsp", 8201);
    add("&zwnj",   8204);
    add("&zwj",    8205);
    add("&lrm",    8206);
    add("&rlm",    8207);
    add("&ndash",  8211);
    add("&mdash",  8212);
    add("&lsquo",  8216);
    add("&rsquo",  8217);
    add("&sbquo",  8218);
    add("&ldquo",  8220);
    add("&rdquo",  8221);
    add("&bdquo",  8222);
    add("&dagger", 8224);
    add("&Dagger", 8225);
    add("&permil", 8240);
    add("&lsaquo", 8249);
    add("&rsaquo", 8250);
    add("&euro",   8364);

  }
}
