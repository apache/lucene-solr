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
package org.apache.lucene.analysis.el;

import org.apache.lucene.analysis.util.CharArraySet;

import java.util.Arrays;


/**
 * A stemmer for Greek words, according to: <i>Development of a Stemmer for the
 * Greek Language.</i> Georgios Ntais
 * <p>
 * NOTE: Input is expected to be casefolded for Greek (including folding of final
 * sigma to sigma), and with diacritics removed. This can be achieved with 
 * either {@link GreekLowerCaseFilter} or ICUFoldingFilter.
 * @lucene.experimental
 */
public class GreekStemmer {
  
 /**
   * Stems a word contained in a leading portion of a char[] array.
   * The word is passed through a number of rules that modify its length.
   * 
   * @param s A char[] array that contains the word to be stemmed.
   * @param len The length of the char[] array.
   * @return The new length of the stemmed word.
   */
  public int stem(char s[], int len) {
    if (len < 4) // too short
      return len;
    
    final int origLen = len;
    // "short rules": if it hits one of these, it skips the "long list"
    len = rule0(s, len);
    len = rule1(s, len);
    len = rule2(s, len);
    len = rule3(s, len);
    len = rule4(s, len);
    len = rule5(s, len);
    len = rule6(s, len);
    len = rule7(s, len);
    len = rule8(s, len);
    len = rule9(s, len);
    len = rule10(s, len);
    len = rule11(s, len);
    len = rule12(s, len);
    len = rule13(s, len);
    len = rule14(s, len);
    len = rule15(s, len);
    len = rule16(s, len);
    len = rule17(s, len);
    len = rule18(s, len);
    len = rule19(s, len);
    len = rule20(s, len);
    // "long list"
    if (len == origLen)
      len = rule21(s, len);
    
    return rule22(s, len);
  }

  private int rule0(char s[], int len) {
    if (len > 9 && (endsWith(s, len, "καθεστωτοσ")
        || endsWith(s, len, "καθεστωτων")))
      return len - 4;
    
    if (len > 8 && (endsWith(s, len, "γεγονοτοσ")
        || endsWith(s, len, "γεγονοτων")))
      return len - 4;
    
    if (len > 8 && endsWith(s, len, "καθεστωτα"))
      return len - 3;
    
    if (len > 7 && (endsWith(s, len, "τατογιου")
        || endsWith(s, len, "τατογιων")))
      return len - 4;
    
    if (len > 7 && endsWith(s, len, "γεγονοτα"))
      return len - 3;
    
    if (len > 7 && endsWith(s, len, "καθεστωσ"))
      return len - 2;
    
    if (len > 6 && (endsWith(s, len, "σκαγιου"))
        || endsWith(s, len, "σκαγιων")
        || endsWith(s, len, "ολογιου")
        || endsWith(s, len, "ολογιων")
        || endsWith(s, len, "κρεατοσ")
        || endsWith(s, len, "κρεατων")
        || endsWith(s, len, "περατοσ")
        || endsWith(s, len, "περατων")
        || endsWith(s, len, "τερατοσ")
        || endsWith(s, len, "τερατων"))
      return len - 4;
    
    if (len > 6 && endsWith(s, len, "τατογια"))
      return len - 3;
    
    if (len > 6 && endsWith(s, len, "γεγονοσ"))
      return len - 2;
    
    if (len > 5 && (endsWith(s, len, "φαγιου")
        || endsWith(s, len, "φαγιων")
        || endsWith(s, len, "σογιου")
        || endsWith(s, len, "σογιων")))
      return len - 4;
    
    if (len > 5 && (endsWith(s, len, "σκαγια")
        || endsWith(s, len, "ολογια")
        || endsWith(s, len, "κρεατα")
        || endsWith(s, len, "περατα")
        || endsWith(s, len, "τερατα")))
      return len - 3;
    
    if (len > 4 && (endsWith(s, len, "φαγια")
        || endsWith(s, len, "σογια")
        || endsWith(s, len, "φωτοσ")
        || endsWith(s, len, "φωτων")))
      return len - 3;
    
    if (len > 4 && (endsWith(s, len, "κρεασ")
        || endsWith(s, len, "περασ")
        || endsWith(s, len, "τερασ")))
      return len - 2;
    
    if (len > 3 && endsWith(s, len, "φωτα"))
      return len - 2;
    
    if (len > 2 && endsWith(s, len, "φωσ"))
      return len - 1;
    
    return len;
  }

  private int rule1(char s[], int len) {
    if (len > 4 && (endsWith(s, len, "αδεσ") || endsWith(s, len, "αδων"))) {
      len -= 4;
      if (!(endsWith(s, len, "οκ") ||
          endsWith(s, len, "μαμ") ||
          endsWith(s, len, "μαν") ||
          endsWith(s, len, "μπαμπ") ||
          endsWith(s, len, "πατερ") ||
          endsWith(s, len, "γιαγι") ||
          endsWith(s, len, "νταντ") ||
          endsWith(s, len, "κυρ") ||
          endsWith(s, len, "θει") ||
          endsWith(s, len, "πεθερ")))
        len += 2; // add back -αδ
    }
    return len;
  }
  
  private int rule2(char s[], int len) {
    if (len > 4 && (endsWith(s, len, "εδεσ") || endsWith(s, len, "εδων"))) {
      len -= 4;
      if (endsWith(s, len, "οπ") ||
          endsWith(s, len, "ιπ") ||
          endsWith(s, len, "εμπ") ||
          endsWith(s, len, "υπ") ||
          endsWith(s, len, "γηπ") ||
          endsWith(s, len, "δαπ") ||
          endsWith(s, len, "κρασπ") ||
          endsWith(s, len, "μιλ"))
        len += 2; // add back -εδ
    }
    return len;
  }
  
  private int rule3(char s[], int len) {
    if (len > 5 && (endsWith(s, len, "ουδεσ") || endsWith(s, len, "ουδων"))) {
      len -= 5;
      if (endsWith(s, len, "αρκ") ||
          endsWith(s, len, "καλιακ") ||
          endsWith(s, len, "πεταλ") ||
          endsWith(s, len, "λιχ") ||
          endsWith(s, len, "πλεξ") ||
          endsWith(s, len, "σκ") ||
          endsWith(s, len, "σ") ||
          endsWith(s, len, "φλ") ||
          endsWith(s, len, "φρ") ||
          endsWith(s, len, "βελ") ||
          endsWith(s, len, "λουλ") ||
          endsWith(s, len, "χν") ||
          endsWith(s, len, "σπ") ||
          endsWith(s, len, "τραγ") ||
          endsWith(s, len, "φε"))
        len += 3; // add back -ουδ
    }
    return len;
  }
  
  private static final CharArraySet exc4 = new CharArraySet(
      Arrays.asList("θ", "δ", "ελ", "γαλ", "ν", "π", "ιδ", "παρ"),
      false);
  
  private int rule4(char s[], int len) {   
    if (len > 3 && (endsWith(s, len, "εωσ") || endsWith(s, len, "εων"))) {
      len -= 3;
      if (exc4.contains(s, 0, len))
        len++; // add back -ε
    }
    return len;
  }
  
  private int rule5(char s[], int len) {
    if (len > 2 && endsWith(s, len, "ια")) {
      len -= 2;
      if (endsWithVowel(s, len))
        len++; // add back -ι
    } else if (len > 3 && (endsWith(s, len, "ιου") || endsWith(s, len, "ιων"))) {
      len -= 3;
      if (endsWithVowel(s, len))
        len++; // add back -ι
    }
    return len;
  }

  private static final CharArraySet exc6 = new CharArraySet(
      Arrays.asList("αλ", "αδ", "ενδ", "αμαν", "αμμοχαλ", "ηθ", "ανηθ",
          "αντιδ", "φυσ", "βρωμ", "γερ", "εξωδ", "καλπ", "καλλιν", "καταδ",
          "μουλ", "μπαν", "μπαγιατ", "μπολ", "μποσ", "νιτ", "ξικ", "συνομηλ",
          "πετσ", "πιτσ", "πικαντ", "πλιατσ", "ποστελν", "πρωτοδ", "σερτ",
          "συναδ", "τσαμ", "υποδ", "φιλον", "φυλοδ", "χασ"), 
       false);

  private int rule6(char s[], int len) {
    boolean removed = false;
    if (len > 3 && (endsWith(s, len, "ικα") || endsWith(s, len, "ικο"))) {
      len -= 3;
      removed = true;
    } else if (len > 4 && (endsWith(s, len, "ικου") || endsWith(s, len, "ικων"))) {
      len -= 4;
      removed = true;
    }
    
    if (removed) {
      if (endsWithVowel(s, len) || exc6.contains(s, 0, len))
        len += 2; // add back -ικ
    }
    return len;
  }
  
  private static final CharArraySet exc7 = new CharArraySet(
      Arrays.asList("αναπ", "αποθ", "αποκ", "αποστ", "βουβ", "ξεθ", "ουλ",
          "πεθ", "πικρ", "ποτ", "σιχ", "χ"), 
      false);
  
  private int rule7(char s[], int len) {
    if (len == 5 && endsWith(s, len, "αγαμε"))
      return len - 1;
    
    if (len > 7 && endsWith(s, len, "ηθηκαμε"))
      len -= 7;
    else if (len > 6 && endsWith(s, len, "ουσαμε"))
      len -= 6;
    else if (len > 5 && (endsWith(s, len, "αγαμε") ||
             endsWith(s, len, "ησαμε") ||
             endsWith(s, len, "ηκαμε")))
      len -= 5;
    
    if (len > 3 && endsWith(s, len, "αμε")) {
      len -= 3;
      if (exc7.contains(s, 0, len))
        len += 2; // add back -αμ
    }

    return len;
  }

  private static final CharArraySet exc8a = new CharArraySet(
      Arrays.asList("τρ", "τσ"),
      false);

  private static final CharArraySet exc8b = new CharArraySet(
      Arrays.asList("βετερ", "βουλκ", "βραχμ", "γ", "δραδουμ", "θ", "καλπουζ",
          "καστελ", "κορμορ", "λαοπλ", "μωαμεθ", "μ", "μουσουλμ", "ν", "ουλ",
          "π", "πελεκ", "πλ", "πολισ", "πορτολ", "σαρακατσ", "σουλτ",
          "τσαρλατ", "ορφ", "τσιγγ", "τσοπ", "φωτοστεφ", "χ", "ψυχοπλ", "αγ",
          "ορφ", "γαλ", "γερ", "δεκ", "διπλ", "αμερικαν", "ουρ", "πιθ",
          "πουριτ", "σ", "ζωντ", "ικ", "καστ", "κοπ", "λιχ", "λουθηρ", "μαιντ",
          "μελ", "σιγ", "σπ", "στεγ", "τραγ", "τσαγ", "φ", "ερ", "αδαπ",
          "αθιγγ", "αμηχ", "ανικ", "ανοργ", "απηγ", "απιθ", "ατσιγγ", "βασ",
          "βασκ", "βαθυγαλ", "βιομηχ", "βραχυκ", "διατ", "διαφ", "ενοργ",
          "θυσ", "καπνοβιομηχ", "καταγαλ", "κλιβ", "κοιλαρφ", "λιβ",
          "μεγλοβιομηχ", "μικροβιομηχ", "νταβ", "ξηροκλιβ", "ολιγοδαμ",
          "ολογαλ", "πενταρφ", "περηφ", "περιτρ", "πλατ", "πολυδαπ", "πολυμηχ",
          "στεφ", "ταβ", "τετ", "υπερηφ", "υποκοπ", "χαμηλοδαπ", "ψηλοταβ"),
      false);
  
  private int rule8(char s[], int len) {
    boolean removed = false;
    
    if (len > 8 && endsWith(s, len, "ιουντανε")) {
      len -= 8;
      removed = true;
    } else if (len > 7 && endsWith(s, len, "ιοντανε") ||
        endsWith(s, len, "ουντανε") ||
        endsWith(s, len, "ηθηκανε")) {
      len -= 7;
      removed = true;
    } else if (len > 6 && endsWith(s, len, "ιοτανε") ||
        endsWith(s, len, "οντανε") ||
        endsWith(s, len, "ουσανε")) {
      len -= 6;
      removed = true;
    } else if (len > 5 && endsWith(s, len, "αγανε") ||
        endsWith(s, len, "ησανε") ||
        endsWith(s, len, "οτανε") ||
        endsWith(s, len, "ηκανε")) {
      len -= 5;
      removed = true;
    }
    
    if (removed && exc8a.contains(s, 0, len)) {
      // add -αγαν (we removed > 4 chars so it's safe)
      len += 4;
      s[len - 4] = 'α';
      s[len - 3] = 'γ';
      s[len - 2] = 'α';
      s[len - 1] = 'ν';
    }
    
    if (len > 3 && endsWith(s, len, "ανε")) {
      len -= 3;
      if (endsWithVowelNoY(s, len) || exc8b.contains(s, 0, len)) {
        len += 2; // add back -αν
      }
    }
    
    return len;
  }
  
  private static final CharArraySet exc9 = new CharArraySet(
      Arrays.asList("αβαρ", "βεν", "εναρ", "αβρ", "αδ", "αθ", "αν", "απλ",
          "βαρον", "ντρ", "σκ", "κοπ", "μπορ", "νιφ", "παγ", "παρακαλ", "σερπ",
          "σκελ", "συρφ", "τοκ", "υ", "δ", "εμ", "θαρρ", "θ"), 
      false);
  
  private int rule9(char s[], int len) {
    if (len > 5 && endsWith(s, len, "ησετε"))
      len -= 5;
    
    if (len > 3 && endsWith(s, len, "ετε")) {
      len -= 3;
      if (exc9.contains(s, 0, len) ||
          endsWithVowelNoY(s, len) ||
          endsWith(s, len, "οδ") ||
          endsWith(s, len, "αιρ") ||
          endsWith(s, len, "φορ") ||
          endsWith(s, len, "ταθ") ||
          endsWith(s, len, "διαθ") ||
          endsWith(s, len, "σχ") ||
          endsWith(s, len, "ενδ") ||
          endsWith(s, len, "ευρ") ||
          endsWith(s, len, "τιθ") ||
          endsWith(s, len, "υπερθ") ||
          endsWith(s, len, "ραθ") ||
          endsWith(s, len, "ενθ") ||
          endsWith(s, len, "ροθ") ||
          endsWith(s, len, "σθ") ||
          endsWith(s, len, "πυρ") ||
          endsWith(s, len, "αιν") ||
          endsWith(s, len, "συνδ") ||
          endsWith(s, len, "συν") ||
          endsWith(s, len, "συνθ") ||
          endsWith(s, len, "χωρ") ||
          endsWith(s, len, "πον") ||
          endsWith(s, len, "βρ") ||
          endsWith(s, len, "καθ") ||
          endsWith(s, len, "ευθ") ||
          endsWith(s, len, "εκθ") ||
          endsWith(s, len, "νετ") ||
          endsWith(s, len, "ρον") ||
          endsWith(s, len, "αρκ") ||
          endsWith(s, len, "βαρ") ||
          endsWith(s, len, "βολ") ||
          endsWith(s, len, "ωφελ")) {
        len += 2; // add back -ετ
      }
    }
    
    return len;
  }

  private int rule10(char s[], int len) {
    if (len > 5 && (endsWith(s, len, "οντασ") || endsWith(s, len, "ωντασ"))) {
      len -= 5;
      if (len == 3 && endsWith(s, len, "αρχ")) {
        len += 3; // add back *ντ
        s[len - 3] = 'ο';
      }
      if (endsWith(s, len, "κρε")) {
        len += 3; // add back *ντ
        s[len - 3] = 'ω';
      }
    }
    
    return len;
  }
  
  private int rule11(char s[], int len) {
    if (len > 6 && endsWith(s, len, "ομαστε")) {
      len -= 6;
      if (len == 2 && endsWith(s, len, "ον")) {
        len += 5; // add back -ομαστ
      }
    } else if (len > 7 && endsWith(s, len, "ιομαστε")) {
      len -= 7;
      if (len == 2 && endsWith(s, len, "ον")) {
        len += 5;
        s[len - 5] = 'ο';
        s[len - 4] = 'μ';
        s[len - 3] = 'α';
        s[len - 2] = 'σ';
        s[len - 1] = 'τ';
      }
    }
    return len;
  }

  private static final CharArraySet exc12a = new CharArraySet(
      Arrays.asList("π", "απ", "συμπ", "ασυμπ", "ακαταπ", "αμεταμφ"),
      false);

  private static final CharArraySet exc12b = new CharArraySet(
      Arrays.asList("αλ", "αρ", "εκτελ", "ζ", "μ", "ξ", "παρακαλ", "αρ", "προ", "νισ"),
      false);
  
  private int rule12(char s[], int len) {
    if (len > 5 && endsWith(s, len, "ιεστε")) {
      len -= 5;
      if (exc12a.contains(s, 0, len))   
        len += 4; // add back -ιεστ
    }
    
    if (len > 4 && endsWith(s, len, "εστε")) {
      len -= 4;
      if (exc12b.contains(s, 0, len))
        len += 3; // add back -εστ
    }
    
    return len;
  }
  
  private static final CharArraySet exc13 = new CharArraySet(
      Arrays.asList("διαθ", "θ", "παρακαταθ", "προσθ", "συνθ"),
      false);
  
  private int rule13(char s[], int len) {
    if (len > 6 && endsWith(s, len, "ηθηκεσ")) {
      len -= 6;
    } else if (len > 5 && (endsWith(s, len, "ηθηκα") || endsWith(s, len, "ηθηκε"))) {
      len -= 5;
    }
    
    boolean removed = false;
    
    if (len > 4 && endsWith(s, len, "ηκεσ")) {
      len -= 4;
      removed = true;
    } else if (len > 3 && (endsWith(s, len, "ηκα") || endsWith(s, len, "ηκε"))) {
      len -= 3;
      removed = true;
    }

    if (removed && (exc13.contains(s, 0, len) 
        || endsWith(s, len, "σκωλ")
        || endsWith(s, len, "σκουλ")
        || endsWith(s, len, "ναρθ")
        || endsWith(s, len, "σφ")
        || endsWith(s, len, "οθ")
        || endsWith(s, len, "πιθ"))) { 
      len += 2; // add back the -ηκ
    }
    
    return len;
  }
  
  private static final CharArraySet exc14 = new CharArraySet(
      Arrays.asList("φαρμακ", "χαδ", "αγκ", "αναρρ", "βρομ", "εκλιπ", "λαμπιδ",
          "λεχ", "μ", "πατ", "ρ", "λ", "μεδ", "μεσαζ", "υποτειν", "αμ", "αιθ",
          "ανηκ", "δεσποζ", "ενδιαφερ", "δε", "δευτερευ", "καθαρευ", "πλε",
          "τσα"), 
      false);

  private int rule14(char s[], int len) {
    boolean removed = false;
    
    if (len > 5 && endsWith(s, len, "ουσεσ")) {
      len -= 5;
      removed = true;
    } else if (len > 4 && (endsWith(s, len, "ουσα") || endsWith(s, len, "ουσε"))) {
      len -= 4;
      removed = true;
    }
    
    if (removed && (exc14.contains(s, 0, len) 
        || endsWithVowel(s, len)
        || endsWith(s, len, "ποδαρ")
        || endsWith(s, len, "βλεπ")
        || endsWith(s, len, "πανταχ")
        || endsWith(s, len, "φρυδ") 
        || endsWith(s, len, "μαντιλ")
        || endsWith(s, len, "μαλλ")
        || endsWith(s, len, "κυματ")
        || endsWith(s, len, "λαχ")
        || endsWith(s, len, "ληγ")
        || endsWith(s, len, "φαγ")
        || endsWith(s, len, "ομ")
        || endsWith(s, len, "πρωτ"))) {
      len += 3; // add back -ουσ
    }

   return len;
  }
  
  private static final CharArraySet exc15a = new CharArraySet(
      Arrays.asList("αβαστ", "πολυφ", "αδηφ", "παμφ", "ρ", "ασπ", "αφ", "αμαλ",
          "αμαλλι", "ανυστ", "απερ", "ασπαρ", "αχαρ", "δερβεν", "δροσοπ",
          "ξεφ", "νεοπ", "νομοτ", "ολοπ", "ομοτ", "προστ", "προσωποπ", "συμπ",
          "συντ", "τ", "υποτ", "χαρ", "αειπ", "αιμοστ", "ανυπ", "αποτ",
          "αρτιπ", "διατ", "εν", "επιτ", "κροκαλοπ", "σιδηροπ", "λ", "ναυ",
          "ουλαμ", "ουρ", "π", "τρ", "μ"), 
      false);
  
  private static final CharArraySet exc15b = new CharArraySet(
      Arrays.asList("ψοφ", "ναυλοχ"),
      false);
  
  private int rule15(char s[], int len) {
    boolean removed = false;
    if (len > 4 && endsWith(s, len, "αγεσ")) {
      len -= 4;
      removed = true;
    } else if (len > 3 && (endsWith(s, len, "αγα") || endsWith(s, len, "αγε"))) {
      len -= 3;
      removed = true;
    }
    
    if (removed) {
      final boolean cond1 = exc15a.contains(s, 0, len) 
        || endsWith(s, len, "οφ")
        || endsWith(s, len, "πελ")
        || endsWith(s, len, "χορτ")
        || endsWith(s, len, "λλ")
        || endsWith(s, len, "σφ")
        || endsWith(s, len, "ρπ")
        || endsWith(s, len, "φρ")
        || endsWith(s, len, "πρ")
        || endsWith(s, len, "λοχ")
        || endsWith(s, len, "σμην");
      
      final boolean cond2 = exc15b.contains(s, 0, len)
        || endsWith(s, len, "κολλ");
      
      if (cond1 && !cond2)
        len += 2; // add back -αγ  
    }
    
    return len;
  }
  
  private static final CharArraySet exc16 = new CharArraySet(
      Arrays.asList("ν", "χερσον", "δωδεκαν", "ερημον", "μεγαλον", "επταν"),
      false);
  
  private int rule16(char s[], int len) {
    boolean removed = false;
    if (len > 4 && endsWith(s, len, "ησου")) {
      len -= 4;
      removed = true;
    } else if (len > 3 && (endsWith(s, len, "ησε") || endsWith(s, len, "ησα"))) {
      len -= 3;
      removed = true;
    }
    
    if (removed && exc16.contains(s, 0, len))
      len += 2; // add back -ησ
    
    return len;
  }
  
  private static final CharArraySet exc17 = new CharArraySet(
      Arrays.asList("ασβ", "σβ", "αχρ", "χρ", "απλ", "αειμν", "δυσχρ", "ευχρ", "κοινοχρ", "παλιμψ"),
      false);
  
  private int rule17(char s[], int len) {
    if (len > 4 && endsWith(s, len, "ηστε")) {
      len -= 4;
      if (exc17.contains(s, 0, len))
        len += 3; // add back the -ηστ
    }
    
    return len;
  }
  
  private static final CharArraySet exc18 = new CharArraySet(
      Arrays.asList("ν", "ρ", "σπι", "στραβομουτσ", "κακομουτσ", "εξων"),
      false);
  
  private int rule18(char s[], int len) {
    boolean removed = false;
    
    if (len > 6 && (endsWith(s, len, "ησουνε") || endsWith(s, len, "ηθουνε"))) {
      len -= 6;
      removed = true;
    } else if (len > 4 && endsWith(s, len, "ουνε")) {
      len -= 4;
      removed = true;
    }
    
    if (removed && exc18.contains(s, 0, len)) {
      len += 3;
      s[len - 3] = 'ο';
      s[len - 2] = 'υ';
      s[len - 1] = 'ν';
    }
    return len;
  }
  
  private static final CharArraySet exc19 = new CharArraySet(
      Arrays.asList("παρασουσ", "φ", "χ", "ωριοπλ", "αζ", "αλλοσουσ", "ασουσ"),
      false);
  
  private int rule19(char s[], int len) {
    boolean removed = false;
    
    if (len > 6 && (endsWith(s, len, "ησουμε") || endsWith(s, len, "ηθουμε"))) {
      len -= 6;
      removed = true;
    } else if (len > 4 && endsWith(s, len, "ουμε")) {
      len -= 4;
      removed = true;
    }
    
    if (removed && exc19.contains(s, 0, len)) {
      len += 3;
      s[len - 3] = 'ο';
      s[len - 2] = 'υ';
      s[len - 1] = 'μ';
    }
    return len;
  }
  
  private int rule20(char s[], int len) {
    if (len > 5 && (endsWith(s, len, "ματων") || endsWith(s, len, "ματοσ")))
      len -= 3;
    else if (len > 4 && endsWith(s, len, "ματα"))
      len -= 2;
    return len;
  }

  private int rule21(char s[], int len) {
    if (len > 9 && endsWith(s, len, "ιοντουσαν"))
      return len - 9;
    
    if (len > 8 && (endsWith(s, len, "ιομασταν") ||
        endsWith(s, len, "ιοσασταν") ||
        endsWith(s, len, "ιουμαστε") ||
        endsWith(s, len, "οντουσαν")))
      return len - 8;
    
    if (len > 7 && (endsWith(s, len, "ιεμαστε") ||
        endsWith(s, len, "ιεσαστε") ||
        endsWith(s, len, "ιομουνα") ||
        endsWith(s, len, "ιοσαστε") ||
        endsWith(s, len, "ιοσουνα") ||
        endsWith(s, len, "ιουνται") ||
        endsWith(s, len, "ιουνταν") ||
        endsWith(s, len, "ηθηκατε") ||
        endsWith(s, len, "ομασταν") ||
        endsWith(s, len, "οσασταν") ||
        endsWith(s, len, "ουμαστε")))
      return len - 7;
    
    if (len > 6 && (endsWith(s, len, "ιομουν") ||
        endsWith(s, len, "ιονταν") ||
        endsWith(s, len, "ιοσουν") ||
        endsWith(s, len, "ηθειτε") ||
        endsWith(s, len, "ηθηκαν") ||
        endsWith(s, len, "ομουνα") ||
        endsWith(s, len, "οσαστε") ||
        endsWith(s, len, "οσουνα") ||
        endsWith(s, len, "ουνται") ||
        endsWith(s, len, "ουνταν") ||
        endsWith(s, len, "ουσατε")))
      return len - 6;
    
    if (len > 5 && (endsWith(s, len, "αγατε") ||
        endsWith(s, len, "ιεμαι") ||
        endsWith(s, len, "ιεται") ||
        endsWith(s, len, "ιεσαι") ||
        endsWith(s, len, "ιοταν") ||
        endsWith(s, len, "ιουμα") ||
        endsWith(s, len, "ηθεισ") ||
        endsWith(s, len, "ηθουν") ||
        endsWith(s, len, "ηκατε") ||
        endsWith(s, len, "ησατε") ||
        endsWith(s, len, "ησουν") ||
        endsWith(s, len, "ομουν") ||
        endsWith(s, len, "ονται") ||
        endsWith(s, len, "ονταν") ||
        endsWith(s, len, "οσουν") ||
        endsWith(s, len, "ουμαι") ||
        endsWith(s, len, "ουσαν")))
      return len - 5;
    
    if (len > 4 && (endsWith(s, len, "αγαν") ||
        endsWith(s, len, "αμαι") ||
        endsWith(s, len, "ασαι") ||
        endsWith(s, len, "αται") ||
        endsWith(s, len, "ειτε") ||
        endsWith(s, len, "εσαι") ||
        endsWith(s, len, "εται") ||
        endsWith(s, len, "ηδεσ") ||
        endsWith(s, len, "ηδων") ||
        endsWith(s, len, "ηθει") ||
        endsWith(s, len, "ηκαν") ||
        endsWith(s, len, "ησαν") ||
        endsWith(s, len, "ησει") ||
        endsWith(s, len, "ησεσ") ||
        endsWith(s, len, "ομαι") ||
        endsWith(s, len, "οταν")))
      return len - 4;
    
    if (len > 3 && (endsWith(s, len, "αει") ||
        endsWith(s, len, "εισ") ||
        endsWith(s, len, "ηθω") ||
        endsWith(s, len, "ησω") ||
        endsWith(s, len, "ουν") ||
        endsWith(s, len, "ουσ")))
      return len - 3;
    
    if (len > 2 && (endsWith(s, len, "αν") ||
        endsWith(s, len, "ασ") ||
        endsWith(s, len, "αω") ||
        endsWith(s, len, "ει") ||
        endsWith(s, len, "εσ") ||
        endsWith(s, len, "ησ") ||
        endsWith(s, len, "οι") ||
        endsWith(s, len, "οσ") ||
        endsWith(s, len, "ου") ||
        endsWith(s, len, "υσ") ||
        endsWith(s, len, "ων")))
      return len - 2;
    
    if (len > 1 && endsWithVowel(s, len))
      return len - 1;

    return len;
  }
  
  private int rule22(char s[], int len) {
    if (endsWith(s, len, "εστερ") ||
        endsWith(s, len, "εστατ"))
      return len - 5;
    
    if (endsWith(s, len, "οτερ") ||
        endsWith(s, len, "οτατ") ||
        endsWith(s, len, "υτερ") ||
        endsWith(s, len, "υτατ") ||
        endsWith(s, len, "ωτερ") ||
        endsWith(s, len, "ωτατ"))
      return len - 4;

    return len;
  }

 /**
   * Checks if the word contained in the leading portion of char[] array , 
   * ends with the suffix given as parameter.
   * 
   * @param s A char[] array that represents a word.
   * @param len The length of the char[] array.
   * @param suffix A {@link String} object to check if the word given ends with these characters.
   * @return True if the word ends with the suffix given , false otherwise.
   */
  private boolean endsWith(char s[], int len, String suffix) {
    final int suffixLen = suffix.length();
    if (suffixLen > len)
      return false;
    for (int i = suffixLen - 1; i >= 0; i--)
      if (s[len -(suffixLen - i)] != suffix.charAt(i))
        return false;
    
    return true;
  }
  
 /**
   * Checks if the word contained in the leading portion of char[] array , 
   * ends with a Greek vowel.
   * 
   * @param s A char[] array that represents a word.
   * @param len The length of the char[] array.
   * @return True if the word contained in the leading portion of char[] array , 
   * ends with a vowel , false otherwise.
   */
  private boolean endsWithVowel(char s[], int len) {
    if (len == 0)
      return false;
    switch(s[len - 1]) {
      case 'α':
      case 'ε':
      case 'η':
      case 'ι':
      case 'ο':
      case 'υ':
      case 'ω':
        return true;
      default:
        return false;
    }
  }
  
 /**
   * Checks if the word contained in the leading portion of char[] array , 
   * ends with a Greek vowel.
   * 
   * @param s A char[] array that represents a word.
   * @param len The length of the char[] array.
   * @return True if the word contained in the leading portion of char[] array , 
   * ends with a vowel , false otherwise.
   */
  private boolean endsWithVowelNoY(char s[], int len) {
    if (len == 0)
      return false;
    switch(s[len - 1]) {
      case 'α':
      case 'ε':
      case 'η':
      case 'ι':
      case 'ο':
      case 'ω':
        return true;
      default:
        return false;
    }
  }
}
