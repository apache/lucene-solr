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


CharacterEntities = ( "AElig" | "Aacute" | "Acirc" | "Agrave" | "Alpha"
                    | "Aring" | "Atilde" | "Auml" | "Beta" | "Ccedil" | "Chi"
                    | "Dagger" | "Delta" | "ETH" | "Eacute" | "Ecirc"
                    | "Egrave" | "Epsilon" | "Eta" | "Euml" | "Gamma"
                    | "Iacute" | "Icirc" | "Igrave" | "Iota" | "Iuml" | "Kappa"
                    | "Lambda" | "Mu" | "Ntilde" | "Nu" | "OElig" | "Oacute"
                    | "Ocirc" | "Ograve" | "Omega" | "Omicron" | "Oslash"
                    | "Otilde" | "Ouml" | "Phi" | "Pi" | "Prime" | "Psi"
                    | "Rho" | "Scaron" | "Sigma" | "THORN" | "Tau" | "Theta"
                    | "Uacute" | "Ucirc" | "Ugrave" | "Upsilon" | "Uuml" | "Xi"
                    | "Yacute" | "Yuml" | "Zeta" | "aacute" | "acirc" | "acute"
                    | "aelig" | "agrave" | "alefsym" | "alpha" | "amp" | "AMP"
                    | "and" | "ang" | "apos" | "aring" | "asymp" | "atilde"
                    | "auml" | "bdquo" | "beta" | "brvbar" | "bull" | "cap"
                    | "ccedil" | "cedil" | "cent" | "chi" | "circ" | "clubs"
                    | "cong" | "copy" | "COPY" | "crarr" | "cup" | "curren"
                    | "dArr" | "dagger" | "darr" | "deg" | "delta" | "diams"
                    | "divide" | "eacute" | "ecirc" | "egrave" | "empty"
                    | "emsp" | "ensp" | "epsilon" | "equiv" | "eta" | "eth"
                    | "euml" | "euro" | "exist" | "fnof" | "forall" | "frac12"
                    | "frac14" | "frac34" | "frasl" | "gamma" | "ge" | "gt"
                    | "GT" | "hArr" | "harr" | "hearts" | "hellip" | "iacute"
                    | "icirc" | "iexcl" | "igrave" | "image" | "infin" | "int"
                    | "iota" | "iquest" | "isin" | "iuml" | "kappa" | "lArr"
                    | "lambda" | "lang" | "laquo" | "larr" | "lceil" | "ldquo"
                    | "le" | "lfloor" | "lowast" | "loz" | "lrm" | "lsaquo"
                    | "lsquo" | "lt" | "LT" | "macr" | "mdash" | "micro"
                    | "middot" | "minus" | "mu" | "nabla" | "nbsp" | "ndash"
                    | "ne" | "ni" | "not" | "notin" | "nsub" | "ntilde" | "nu"
                    | "oacute" | "ocirc" | "oelig" | "ograve" | "oline"
                    | "omega" | "omicron" | "oplus" | "or" | "ordf" | "ordm"
                    | "oslash" | "otilde" | "otimes" | "ouml" | "para" | "part"
                    | "permil" | "perp" | "phi" | "pi" | "piv" | "plusmn"
                    | "pound" | "prime" | "prod" | "prop" | "psi" | "quot"
                    | "QUOT" | "rArr" | "radic" | "rang" | "raquo" | "rarr"
                    | "rceil" | "rdquo" | "real" | "reg" | "REG" | "rfloor"
                    | "rho" | "rlm" | "rsaquo" | "rsquo" | "sbquo" | "scaron"
                    | "sdot" | "sect" | "shy" | "sigma" | "sigmaf" | "sim"
                    | "spades" | "sub" | "sube" | "sum" | "sup" | "sup1"
                    | "sup2" | "sup3" | "supe" | "szlig" | "tau" | "there4"
                    | "theta" | "thetasym" | "thinsp" | "thorn" | "tilde"
                    | "times" | "trade" | "uArr" | "uacute" | "uarr" | "ucirc"
                    | "ugrave" | "uml" | "upsih" | "upsilon" | "uuml"
                    | "weierp" | "xi" | "yacute" | "yen" | "yuml" | "zeta"
                    | "zwj" | "zwnj" )
%{
  private static final Map<String,String> upperCaseVariantsAccepted
      = new HashMap<>();
  static {
    upperCaseVariantsAccepted.put("quot", "QUOT");
    upperCaseVariantsAccepted.put("copy", "COPY");
    upperCaseVariantsAccepted.put("gt", "GT");
    upperCaseVariantsAccepted.put("lt", "LT");
    upperCaseVariantsAccepted.put("reg", "REG");
    upperCaseVariantsAccepted.put("amp", "AMP");
  }
  private static final CharArrayMap<Character> entityValues
      = new CharArrayMap<>(253, false);
  static {
    String[] entities = {
      "AElig", "\u00C6", "Aacute", "\u00C1", "Acirc", "\u00C2",
      "Agrave", "\u00C0", "Alpha", "\u0391", "Aring", "\u00C5",
      "Atilde", "\u00C3", "Auml", "\u00C4", "Beta", "\u0392",
      "Ccedil", "\u00C7", "Chi", "\u03A7", "Dagger", "\u2021",
      "Delta", "\u0394", "ETH", "\u00D0", "Eacute", "\u00C9",
      "Ecirc", "\u00CA", "Egrave", "\u00C8", "Epsilon", "\u0395",
      "Eta", "\u0397", "Euml", "\u00CB", "Gamma", "\u0393", "Iacute", "\u00CD",
      "Icirc", "\u00CE", "Igrave", "\u00CC", "Iota", "\u0399",
      "Iuml", "\u00CF", "Kappa", "\u039A", "Lambda", "\u039B", "Mu", "\u039C",
      "Ntilde", "\u00D1", "Nu", "\u039D", "OElig", "\u0152",
      "Oacute", "\u00D3", "Ocirc", "\u00D4", "Ograve", "\u00D2",
      "Omega", "\u03A9", "Omicron", "\u039F", "Oslash", "\u00D8",
      "Otilde", "\u00D5", "Ouml", "\u00D6", "Phi", "\u03A6", "Pi", "\u03A0",
      "Prime", "\u2033", "Psi", "\u03A8", "Rho", "\u03A1", "Scaron", "\u0160",
      "Sigma", "\u03A3", "THORN", "\u00DE", "Tau", "\u03A4", "Theta", "\u0398",
      "Uacute", "\u00DA", "Ucirc", "\u00DB", "Ugrave", "\u00D9",
      "Upsilon", "\u03A5", "Uuml", "\u00DC", "Xi", "\u039E",
      "Yacute", "\u00DD", "Yuml", "\u0178", "Zeta", "\u0396",
      "aacute", "\u00E1", "acirc", "\u00E2", "acute", "\u00B4",
      "aelig", "\u00E6", "agrave", "\u00E0", "alefsym", "\u2135",
      "alpha", "\u03B1", "amp", "\u0026", "and", "\u2227", "ang", "\u2220",
      "apos", "\u0027", "aring", "\u00E5", "asymp", "\u2248",
      "atilde", "\u00E3", "auml", "\u00E4", "bdquo", "\u201E",
      "beta", "\u03B2", "brvbar", "\u00A6", "bull", "\u2022", "cap", "\u2229",
      "ccedil", "\u00E7", "cedil", "\u00B8", "cent", "\u00A2", "chi", "\u03C7",
      "circ", "\u02C6", "clubs", "\u2663", "cong", "\u2245", "copy", "\u00A9",
      "crarr", "\u21B5", "cup", "\u222A", "curren", "\u00A4", "dArr", "\u21D3",
      "dagger", "\u2020", "darr", "\u2193", "deg", "\u00B0", "delta", "\u03B4",
      "diams", "\u2666", "divide", "\u00F7", "eacute", "\u00E9",
      "ecirc", "\u00EA", "egrave", "\u00E8", "empty", "\u2205",
      "emsp", "\u2003", "ensp", "\u2002", "epsilon", "\u03B5",
      "equiv", "\u2261", "eta", "\u03B7", "eth", "\u00F0", "euml", "\u00EB",
      "euro", "\u20AC", "exist", "\u2203", "fnof", "\u0192",
      "forall", "\u2200", "frac12", "\u00BD", "frac14", "\u00BC",
      "frac34", "\u00BE", "frasl", "\u2044", "gamma", "\u03B3", "ge", "\u2265",
      "gt", "\u003E", "hArr", "\u21D4", "harr", "\u2194", "hearts", "\u2665",
      "hellip", "\u2026", "iacute", "\u00ED", "icirc", "\u00EE",
      "iexcl", "\u00A1", "igrave", "\u00EC", "image", "\u2111",
      "infin", "\u221E", "int", "\u222B", "iota", "\u03B9", "iquest", "\u00BF",
      "isin", "\u2208", "iuml", "\u00EF", "kappa", "\u03BA", "lArr", "\u21D0",
      "lambda", "\u03BB", "lang", "\u2329", "laquo", "\u00AB",
      "larr", "\u2190", "lceil", "\u2308", "ldquo", "\u201C", "le", "\u2264",
      "lfloor", "\u230A", "lowast", "\u2217", "loz", "\u25CA", "lrm", "\u200E",
      "lsaquo", "\u2039", "lsquo", "\u2018", "lt", "\u003C", "macr", "\u00AF",
      "mdash", "\u2014", "micro", "\u00B5", "middot", "\u00B7",
      "minus", "\u2212", "mu", "\u03BC", "nabla", "\u2207", "nbsp", " ",
      "ndash", "\u2013", "ne", "\u2260", "ni", "\u220B", "not", "\u00AC",
      "notin", "\u2209", "nsub", "\u2284", "ntilde", "\u00F1", "nu", "\u03BD",
      "oacute", "\u00F3", "ocirc", "\u00F4", "oelig", "\u0153",
      "ograve", "\u00F2", "oline", "\u203E", "omega", "\u03C9",
      "omicron", "\u03BF", "oplus", "\u2295", "or", "\u2228", "ordf", "\u00AA",
      "ordm", "\u00BA", "oslash", "\u00F8", "otilde", "\u00F5",
      "otimes", "\u2297", "ouml", "\u00F6", "para", "\u00B6", "part", "\u2202",
      "permil", "\u2030", "perp", "\u22A5", "phi", "\u03C6", "pi", "\u03C0",
      "piv", "\u03D6", "plusmn", "\u00B1", "pound", "\u00A3",
      "prime", "\u2032", "prod", "\u220F", "prop", "\u221D", "psi", "\u03C8",
      "quot", "\"", "rArr", "\u21D2", "radic", "\u221A", "rang", "\u232A",
      "raquo", "\u00BB", "rarr", "\u2192", "rceil", "\u2309",
      "rdquo", "\u201D", "real", "\u211C", "reg", "\u00AE", "rfloor", "\u230B",
      "rho", "\u03C1", "rlm", "\u200F", "rsaquo", "\u203A", "rsquo", "\u2019",
      "sbquo", "\u201A", "scaron", "\u0161", "sdot", "\u22C5",
      "sect", "\u00A7", "shy", "\u00AD", "sigma", "\u03C3", "sigmaf", "\u03C2",
      "sim", "\u223C", "spades", "\u2660", "sub", "\u2282", "sube", "\u2286",
      "sum", "\u2211", "sup", "\u2283", "sup1", "\u00B9", "sup2", "\u00B2",
      "sup3", "\u00B3", "supe", "\u2287", "szlig", "\u00DF", "tau", "\u03C4",
      "there4", "\u2234", "theta", "\u03B8", "thetasym", "\u03D1",
      "thinsp", "\u2009", "thorn", "\u00FE", "tilde", "\u02DC",
      "times", "\u00D7", "trade", "\u2122", "uArr", "\u21D1",
      "uacute", "\u00FA", "uarr", "\u2191", "ucirc", "\u00FB",
      "ugrave", "\u00F9", "uml", "\u00A8", "upsih", "\u03D2",
      "upsilon", "\u03C5", "uuml", "\u00FC", "weierp", "\u2118",
      "xi", "\u03BE", "yacute", "\u00FD", "yen", "\u00A5", "yuml", "\u00FF",
      "zeta", "\u03B6", "zwj", "\u200D", "zwnj", "\u200C"
    };
    for (int i = 0 ; i < entities.length ; i += 2) {
      Character value = entities[i + 1].charAt(0);
      entityValues.put(entities[i], value);
      String upperCaseVariant = upperCaseVariantsAccepted.get(entities[i]);
      if (upperCaseVariant != null) {
        entityValues.put(upperCaseVariant, value);
      }
    }
  }
%}
