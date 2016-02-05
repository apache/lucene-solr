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


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

public class TestGreekStemmer extends BaseTokenStreamTestCase {
  private Analyzer a;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new GreekAnalyzer();
  }
  
  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }

  public void testMasculineNouns() throws Exception {
    // -ος
    checkOneTerm(a, "άνθρωπος", "ανθρωπ");
    checkOneTerm(a, "ανθρώπου", "ανθρωπ");
    checkOneTerm(a, "άνθρωπο", "ανθρωπ");
    checkOneTerm(a, "άνθρωπε", "ανθρωπ");
    checkOneTerm(a, "άνθρωποι", "ανθρωπ");
    checkOneTerm(a, "ανθρώπων", "ανθρωπ");
    checkOneTerm(a, "ανθρώπους", "ανθρωπ");
    checkOneTerm(a, "άνθρωποι", "ανθρωπ");
    
    // -ης
    checkOneTerm(a, "πελάτης", "πελατ");
    checkOneTerm(a, "πελάτη", "πελατ");
    checkOneTerm(a, "πελάτες", "πελατ");
    checkOneTerm(a, "πελατών", "πελατ");
    
    // -ας/-ες
    checkOneTerm(a, "ελέφαντας", "ελεφαντ");
    checkOneTerm(a, "ελέφαντα", "ελεφαντ");
    checkOneTerm(a, "ελέφαντες", "ελεφαντ");
    checkOneTerm(a, "ελεφάντων", "ελεφαντ");
    
    // -ας/-αδες
    checkOneTerm(a, "μπαμπάς", "μπαμπ");
    checkOneTerm(a, "μπαμπά", "μπαμπ");
    checkOneTerm(a, "μπαμπάδες", "μπαμπ");
    checkOneTerm(a, "μπαμπάδων", "μπαμπ");
    
    // -ης/-ηδες
    checkOneTerm(a, "μπακάλης", "μπακαλ");
    checkOneTerm(a, "μπακάλη", "μπακαλ");
    checkOneTerm(a, "μπακάληδες", "μπακαλ");
    checkOneTerm(a, "μπακάληδων", "μπακαλ");
    
    // -ες
    checkOneTerm(a, "καφές", "καφ");
    checkOneTerm(a, "καφέ", "καφ");
    checkOneTerm(a, "καφέδες", "καφ");
    checkOneTerm(a, "καφέδων", "καφ");
    
    // -έας/είς
    checkOneTerm(a, "γραμματέας", "γραμματε");
    checkOneTerm(a, "γραμματέα", "γραμματε");
    // plural forms conflate w/ each other, not w/ the sing forms
    checkOneTerm(a, "γραμματείς", "γραμματ");
    checkOneTerm(a, "γραμματέων", "γραμματ");
    
    // -ους/οι
    checkOneTerm(a, "απόπλους", "αποπλ");
    checkOneTerm(a, "απόπλου", "αποπλ");
    checkOneTerm(a, "απόπλοι", "αποπλ");
    checkOneTerm(a, "απόπλων", "αποπλ");
    
    // -ους/-ουδες
    checkOneTerm(a, "παππούς", "παππ");
    checkOneTerm(a, "παππού", "παππ");
    checkOneTerm(a, "παππούδες", "παππ");
    checkOneTerm(a, "παππούδων", "παππ");
    
    // -ης/-εις
    checkOneTerm(a, "λάτρης", "λατρ");
    checkOneTerm(a, "λάτρη", "λατρ");
    checkOneTerm(a, "λάτρεις", "λατρ");
    checkOneTerm(a, "λάτρεων", "λατρ");
    
    // -υς
    checkOneTerm(a, "πέλεκυς", "πελεκ");
    checkOneTerm(a, "πέλεκυ", "πελεκ");
    checkOneTerm(a, "πελέκεις", "πελεκ");
    checkOneTerm(a, "πελέκεων", "πελεκ");
    
    // -ωρ
    // note: nom./voc. doesn't conflate w/ the rest
    checkOneTerm(a, "μέντωρ", "μεντωρ");
    checkOneTerm(a, "μέντορος", "μεντορ");
    checkOneTerm(a, "μέντορα", "μεντορ");
    checkOneTerm(a, "μέντορες", "μεντορ");
    checkOneTerm(a, "μεντόρων", "μεντορ");
    
    // -ων
    checkOneTerm(a, "αγώνας", "αγων");
    checkOneTerm(a, "αγώνος", "αγων");
    checkOneTerm(a, "αγώνα", "αγων");
    checkOneTerm(a, "αγώνα", "αγων");
    checkOneTerm(a, "αγώνες", "αγων");
    checkOneTerm(a, "αγώνων", "αγων");
    
    // -ας/-ηδες
    checkOneTerm(a, "αέρας", "αερ");
    checkOneTerm(a, "αέρα", "αερ");
    checkOneTerm(a, "αέρηδες", "αερ");
    checkOneTerm(a, "αέρηδων", "αερ");
    
    // -ης/-ητες
    checkOneTerm(a, "γόης", "γο");
    checkOneTerm(a, "γόη", "γοη"); // too short
    // the two plural forms conflate
    checkOneTerm(a, "γόητες", "γοητ");
    checkOneTerm(a, "γοήτων", "γοητ");
  }
  
  public void testFeminineNouns() throws Exception {
    // -α/-ες,-ών
    checkOneTerm(a, "φορά", "φορ");
    checkOneTerm(a, "φοράς", "φορ");
    checkOneTerm(a, "φορές", "φορ");
    checkOneTerm(a, "φορών", "φορ");
    
    // -α/-ες,-ων
    checkOneTerm(a, "αγελάδα", "αγελαδ");
    checkOneTerm(a, "αγελάδας", "αγελαδ");
    checkOneTerm(a, "αγελάδες", "αγελαδ");
    checkOneTerm(a, "αγελάδων", "αγελαδ");
    
    // -η/-ες
    checkOneTerm(a, "ζάχαρη", "ζαχαρ");
    checkOneTerm(a, "ζάχαρης", "ζαχαρ");
    checkOneTerm(a, "ζάχαρες", "ζαχαρ");
    checkOneTerm(a, "ζαχάρεων", "ζαχαρ");
    
    // -η/-εις
    checkOneTerm(a, "τηλεόραση", "τηλεορασ");
    checkOneTerm(a, "τηλεόρασης", "τηλεορασ");
    checkOneTerm(a, "τηλεοράσεις", "τηλεορασ");
    checkOneTerm(a, "τηλεοράσεων", "τηλεορασ");
    
    // -α/-αδες
    checkOneTerm(a, "μαμά", "μαμ");
    checkOneTerm(a, "μαμάς", "μαμ");
    checkOneTerm(a, "μαμάδες", "μαμ");
    checkOneTerm(a, "μαμάδων", "μαμ");
    
    // -ος
    checkOneTerm(a, "λεωφόρος", "λεωφορ");
    checkOneTerm(a, "λεωφόρου", "λεωφορ");
    checkOneTerm(a, "λεωφόρο", "λεωφορ");
    checkOneTerm(a, "λεωφόρε", "λεωφορ");
    checkOneTerm(a, "λεωφόροι", "λεωφορ");
    checkOneTerm(a, "λεωφόρων", "λεωφορ");
    checkOneTerm(a, "λεωφόρους", "λεωφορ");
    
    // -ου
    checkOneTerm(a, "αλεπού", "αλεπ");
    checkOneTerm(a, "αλεπούς", "αλεπ");
    checkOneTerm(a, "αλεπούδες", "αλεπ");
    checkOneTerm(a, "αλεπούδων", "αλεπ");
    
    // -έας/είς
    // note: not all forms conflate
    checkOneTerm(a, "γραμματέας", "γραμματε");
    checkOneTerm(a, "γραμματέως", "γραμματ");
    checkOneTerm(a, "γραμματέα", "γραμματε");
    checkOneTerm(a, "γραμματείς", "γραμματ");
    checkOneTerm(a, "γραμματέων", "γραμματ");
  }
  
  public void testNeuterNouns() throws Exception {
    // ending with -ο
    // note: nom doesnt conflate
    checkOneTerm(a, "βιβλίο", "βιβλι");
    checkOneTerm(a, "βιβλίου", "βιβλ");
    checkOneTerm(a, "βιβλία", "βιβλ");
    checkOneTerm(a, "βιβλίων", "βιβλ");
    
    // ending with -ι
    checkOneTerm(a, "πουλί", "πουλ");
    checkOneTerm(a, "πουλιού", "πουλ");
    checkOneTerm(a, "πουλιά", "πουλ");
    checkOneTerm(a, "πουλιών", "πουλ");
    
    // ending with -α
    // note: nom. doesnt conflate
    checkOneTerm(a, "πρόβλημα", "προβλημ");
    checkOneTerm(a, "προβλήματος", "προβλημα");
    checkOneTerm(a, "προβλήματα", "προβλημα");
    checkOneTerm(a, "προβλημάτων", "προβλημα");
    
    // ending with -ος/-ους
    checkOneTerm(a, "πέλαγος", "πελαγ");
    checkOneTerm(a, "πελάγους", "πελαγ");
    checkOneTerm(a, "πελάγη", "πελαγ");
    checkOneTerm(a, "πελάγων", "πελαγ");
    
    // ending with -ός/-ότος
    checkOneTerm(a, "γεγονός", "γεγον");
    checkOneTerm(a, "γεγονότος", "γεγον");
    checkOneTerm(a, "γεγονότα", "γεγον");
    checkOneTerm(a, "γεγονότων", "γεγον");
    
    // ending with -υ/-ιου
    checkOneTerm(a, "βράδυ", "βραδ");
    checkOneTerm(a, "βράδι", "βραδ");
    checkOneTerm(a, "βραδιού", "βραδ");
    checkOneTerm(a, "βράδια", "βραδ");
    checkOneTerm(a, "βραδιών", "βραδ");
    
    // ending with -υ/-ατος
    // note: nom. doesnt conflate
    checkOneTerm(a, "δόρυ", "δορ");
    checkOneTerm(a, "δόρατος", "δορατ");
    checkOneTerm(a, "δόρατα", "δορατ");
    checkOneTerm(a, "δοράτων", "δορατ");
    
    // ending with -ας
    checkOneTerm(a, "κρέας", "κρε");
    checkOneTerm(a, "κρέατος", "κρε");
    checkOneTerm(a, "κρέατα", "κρε");
    checkOneTerm(a, "κρεάτων", "κρε");
    
    // ending with -ως
    checkOneTerm(a, "λυκόφως", "λυκοφω");
    checkOneTerm(a, "λυκόφωτος", "λυκοφω");
    checkOneTerm(a, "λυκόφωτα", "λυκοφω");
    checkOneTerm(a, "λυκοφώτων", "λυκοφω");
    
    // ending with -ον/-ου
    // note: nom. doesnt conflate
    checkOneTerm(a, "μέσον", "μεσον");
    checkOneTerm(a, "μέσου", "μεσ");
    checkOneTerm(a, "μέσα", "μεσ");
    checkOneTerm(a, "μέσων", "μεσ");
    
    // ending in -ον/-οντος
    // note: nom. doesnt conflate
    checkOneTerm(a, "ενδιαφέρον", "ενδιαφερον");
    checkOneTerm(a, "ενδιαφέροντος", "ενδιαφεροντ");
    checkOneTerm(a, "ενδιαφέροντα", "ενδιαφεροντ");
    checkOneTerm(a, "ενδιαφερόντων", "ενδιαφεροντ");
    
    // ending with -εν/-εντος
    checkOneTerm(a, "ανακοινωθέν", "ανακοινωθεν");
    checkOneTerm(a, "ανακοινωθέντος", "ανακοινωθεντ");
    checkOneTerm(a, "ανακοινωθέντα", "ανακοινωθεντ");
    checkOneTerm(a, "ανακοινωθέντων", "ανακοινωθεντ");
    
    // ending with -αν/-αντος
    checkOneTerm(a, "σύμπαν", "συμπ");
    checkOneTerm(a, "σύμπαντος", "συμπαντ");
    checkOneTerm(a, "σύμπαντα", "συμπαντ");
    checkOneTerm(a, "συμπάντων", "συμπαντ");
    
    // ending with  -α/-ακτος
    checkOneTerm(a, "γάλα", "γαλ");
    checkOneTerm(a, "γάλακτος", "γαλακτ");
    checkOneTerm(a, "γάλατα", "γαλατ");
    checkOneTerm(a, "γαλάκτων", "γαλακτ");
  }
  
  public void testAdjectives() throws Exception {
    // ending with -ής, -ές/-είς, -ή
    checkOneTerm(a, "συνεχής", "συνεχ");
    checkOneTerm(a, "συνεχούς", "συνεχ");
    checkOneTerm(a, "συνεχή", "συνεχ");
    checkOneTerm(a, "συνεχών", "συνεχ");
    checkOneTerm(a, "συνεχείς", "συνεχ");
    checkOneTerm(a, "συνεχές", "συνεχ");
    
    // ending with -ης, -ες/-εις, -η
    checkOneTerm(a, "συνήθης", "συνηθ");
    checkOneTerm(a, "συνήθους", "συνηθ");
    checkOneTerm(a, "συνήθη", "συνηθ");
    // note: doesn't conflate
    checkOneTerm(a, "συνήθεις", "συν");
    checkOneTerm(a, "συνήθων", "συνηθ");
    checkOneTerm(a, "σύνηθες", "συνηθ");
    
    // ending with -υς, -υ/-εις, -ια
    checkOneTerm(a, "βαθύς", "βαθ");
    checkOneTerm(a, "βαθέος", "βαθε");
    checkOneTerm(a, "βαθύ", "βαθ");
    checkOneTerm(a, "βαθείς", "βαθ");
    checkOneTerm(a, "βαθέων", "βαθ");
    
    checkOneTerm(a, "βαθιά", "βαθ");
    checkOneTerm(a, "βαθιάς", "βαθι");
    checkOneTerm(a, "βαθιές", "βαθι");
    checkOneTerm(a, "βαθιών", "βαθ");
    
    checkOneTerm(a, "βαθέα", "βαθε");
    
    // comparative/superlative
    checkOneTerm(a, "ψηλός", "ψηλ");
    checkOneTerm(a, "ψηλότερος", "ψηλ");
    checkOneTerm(a, "ψηλότατος", "ψηλ");
    
    checkOneTerm(a, "ωραίος", "ωραι");
    checkOneTerm(a, "ωραιότερος", "ωραι");
    checkOneTerm(a, "ωραιότατος", "ωραι");
    
    checkOneTerm(a, "επιεικής", "επιεικ");
    checkOneTerm(a, "επιεικέστερος", "επιεικ");
    checkOneTerm(a, "επιεικέστατος", "επιεικ");
  }
  

  public void testVerbs() throws Exception {
    // note, past/present verb stems will not conflate (from the paper)
    //-ω,-α/-.ω,-.α
    checkOneTerm(a, "ορίζω", "οριζ");
    checkOneTerm(a, "όριζα", "οριζ");
    checkOneTerm(a, "όριζε", "οριζ");
    checkOneTerm(a, "ορίζοντας", "οριζ");
    checkOneTerm(a, "ορίζομαι", "οριζ");
    checkOneTerm(a, "οριζόμουν", "οριζ");
    checkOneTerm(a, "ορίζεσαι", "οριζ");
    
    checkOneTerm(a, "όρισα", "ορισ");
    checkOneTerm(a, "ορίσω", "ορισ");
    checkOneTerm(a, "όρισε", "ορισ");
    checkOneTerm(a, "ορίσει", "ορισ");
    
    checkOneTerm(a, "ορίστηκα", "οριστ");
    checkOneTerm(a, "οριστώ", "οριστ");
    checkOneTerm(a, "οριστείς", "οριστ");
    checkOneTerm(a, "οριστεί", "οριστ");
    
    checkOneTerm(a, "ορισμένο", "ορισμεν");
    checkOneTerm(a, "ορισμένη", "ορισμεν");
    checkOneTerm(a, "ορισμένος", "ορισμεν");
    
    // -ω,-α/-ξω,-ξα
    checkOneTerm(a, "ανοίγω", "ανοιγ");
    checkOneTerm(a, "άνοιγα", "ανοιγ");
    checkOneTerm(a, "άνοιγε", "ανοιγ");
    checkOneTerm(a, "ανοίγοντας", "ανοιγ");
    checkOneTerm(a, "ανοίγομαι", "ανοιγ");
    checkOneTerm(a, "ανοιγόμουν", "ανοιγ");
    
    checkOneTerm(a, "άνοιξα", "ανοιξ");
    checkOneTerm(a, "ανοίξω", "ανοιξ");
    checkOneTerm(a, "άνοιξε", "ανοιξ");
    checkOneTerm(a, "ανοίξει", "ανοιξ");
    
    checkOneTerm(a, "ανοίχτηκα", "ανοιχτ");
    checkOneTerm(a, "ανοιχτώ", "ανοιχτ");
    checkOneTerm(a, "ανοίχτηκα", "ανοιχτ");
    checkOneTerm(a, "ανοιχτείς", "ανοιχτ");
    checkOneTerm(a, "ανοιχτεί", "ανοιχτ");
    
    checkOneTerm(a, "ανοίξου", "ανοιξ");
    
    //-ώ/-άω,-ούσα/-άσω,-ασα
    checkOneTerm(a, "περνώ", "περν");
    checkOneTerm(a, "περνάω", "περν");
    checkOneTerm(a, "περνούσα", "περν");
    checkOneTerm(a, "πέρναγα", "περν");
    checkOneTerm(a, "πέρνα", "περν");
    checkOneTerm(a, "περνώντας", "περν");
    
    checkOneTerm(a, "πέρασα", "περασ");
    checkOneTerm(a, "περάσω", "περασ");
    checkOneTerm(a, "πέρασε", "περασ");
    checkOneTerm(a, "περάσει", "περασ");
    
    checkOneTerm(a, "περνιέμαι", "περν");
    checkOneTerm(a, "περνιόμουν", "περν");
   
    checkOneTerm(a, "περάστηκα", "περαστ");
    checkOneTerm(a, "περαστώ", "περαστ");
    checkOneTerm(a, "περαστείς", "περαστ");
    checkOneTerm(a, "περαστεί", "περαστ");

    checkOneTerm(a, "περασμένο", "περασμεν");
    checkOneTerm(a, "περασμένη", "περασμεν");
    checkOneTerm(a, "περασμένος", "περασμεν");
    
    // -ώ/-άω,-ούσα/-άξω,-αξα
    checkOneTerm(a, "πετώ", "πετ");
    checkOneTerm(a, "πετάω", "πετ");
    checkOneTerm(a, "πετούσα", "πετ");
    checkOneTerm(a, "πέταγα", "πετ");
    checkOneTerm(a, "πέτα", "πετ");
    checkOneTerm(a, "πετώντας", "πετ");
    checkOneTerm(a, "πετιέμαι", "πετ");
    checkOneTerm(a, "πετιόμουν", "πετ");
    
    checkOneTerm(a, "πέταξα", "πεταξ");
    checkOneTerm(a, "πετάξω", "πεταξ");
    checkOneTerm(a, "πέταξε", "πεταξ");
    checkOneTerm(a, "πετάξει", "πεταξ");

    checkOneTerm(a, "πετάχτηκα", "πεταχτ");
    checkOneTerm(a, "πεταχτώ", "πεταχτ");
    checkOneTerm(a, "πεταχτείς", "πεταχτ");
    checkOneTerm(a, "πεταχτεί", "πεταχτ");
    
    checkOneTerm(a, "πεταμένο", "πεταμεν");
    checkOneTerm(a, "πεταμένη", "πεταμεν");
    checkOneTerm(a, "πεταμένος", "πεταμεν");
    
    // -ώ/-άω,-ούσα / -έσω,-εσα
    checkOneTerm(a, "καλώ", "καλ");
    checkOneTerm(a, "καλούσα", "καλ");
    checkOneTerm(a, "καλείς", "καλ");
    checkOneTerm(a, "καλώντας", "καλ");
    
    checkOneTerm(a, "καλούμαι", "καλ");
    // pass. imperfect /imp. progressive doesnt conflate
    checkOneTerm(a, "καλούμουν", "καλουμ");
    checkOneTerm(a, "καλείσαι", "καλεισα");
    
    checkOneTerm(a, "καλέστηκα", "καλεστ");
    checkOneTerm(a, "καλεστώ", "καλεστ");
    checkOneTerm(a, "καλεστείς", "καλεστ");
    checkOneTerm(a, "καλεστεί", "καλεστ");
    
    checkOneTerm(a, "καλεσμένο", "καλεσμεν");
    checkOneTerm(a, "καλεσμένη", "καλεσμεν");
    checkOneTerm(a, "καλεσμένος", "καλεσμεν");
    
    checkOneTerm(a, "φορώ", "φορ");
    checkOneTerm(a, "φοράω", "φορ");
    checkOneTerm(a, "φορούσα", "φορ");
    checkOneTerm(a, "φόραγα", "φορ");
    checkOneTerm(a, "φόρα", "φορ");
    checkOneTerm(a, "φορώντας", "φορ");
    checkOneTerm(a, "φοριέμαι", "φορ");
    checkOneTerm(a, "φοριόμουν", "φορ");
    checkOneTerm(a, "φοριέσαι", "φορ");
    
    checkOneTerm(a, "φόρεσα", "φορεσ");
    checkOneTerm(a, "φορέσω", "φορεσ");
    checkOneTerm(a, "φόρεσε", "φορεσ");
    checkOneTerm(a, "φορέσει", "φορεσ");
    
    checkOneTerm(a, "φορέθηκα", "φορεθ");
    checkOneTerm(a, "φορεθώ", "φορεθ");
    checkOneTerm(a, "φορεθείς", "φορεθ");
    checkOneTerm(a, "φορεθεί", "φορεθ");
    
    checkOneTerm(a, "φορεμένο", "φορεμεν");
    checkOneTerm(a, "φορεμένη", "φορεμεν");
    checkOneTerm(a, "φορεμένος", "φορεμεν");
    
    // -ώ/-άω,-ούσα / -ήσω,-ησα
    checkOneTerm(a, "κρατώ", "κρατ");
    checkOneTerm(a, "κρατάω", "κρατ");
    checkOneTerm(a, "κρατούσα", "κρατ");
    checkOneTerm(a, "κράταγα", "κρατ");
    checkOneTerm(a, "κράτα", "κρατ");
    checkOneTerm(a, "κρατώντας", "κρατ");
    
    checkOneTerm(a, "κράτησα", "κρατ");
    checkOneTerm(a, "κρατήσω", "κρατ");
    checkOneTerm(a, "κράτησε", "κρατ");
    checkOneTerm(a, "κρατήσει", "κρατ");
    
    checkOneTerm(a, "κρατούμαι", "κρατ");
    checkOneTerm(a, "κρατιέμαι", "κρατ");
    // this imperfect form doesnt conflate 
    checkOneTerm(a, "κρατούμουν", "κρατουμ");
    checkOneTerm(a, "κρατιόμουν", "κρατ");
    // this imp. prog form doesnt conflate
    checkOneTerm(a, "κρατείσαι", "κρατεισα");

    checkOneTerm(a, "κρατήθηκα", "κρατ");
    checkOneTerm(a, "κρατηθώ", "κρατ");
    checkOneTerm(a, "κρατηθείς", "κρατ");
    checkOneTerm(a, "κρατηθεί", "κρατ");
    checkOneTerm(a, "κρατήσου", "κρατ");
    
    checkOneTerm(a, "κρατημένο", "κρατημεν");
    checkOneTerm(a, "κρατημένη", "κρατημεν");
    checkOneTerm(a, "κρατημένος", "κρατημεν");
    
    // -.μαι,-.μουν / -.ώ,-.ηκα
    checkOneTerm(a, "κοιμάμαι", "κοιμ");
    checkOneTerm(a, "κοιμόμουν", "κοιμ");
    checkOneTerm(a, "κοιμάσαι", "κοιμ");
    
    checkOneTerm(a, "κοιμήθηκα", "κοιμ");
    checkOneTerm(a, "κοιμηθώ", "κοιμ");
    checkOneTerm(a, "κοιμήσου", "κοιμ");
    checkOneTerm(a, "κοιμηθεί", "κοιμ");
    
    checkOneTerm(a, "κοιμισμένο", "κοιμισμεν");
    checkOneTerm(a, "κοιμισμένη", "κοιμισμεν");
    checkOneTerm(a, "κοιμισμένος", "κοιμισμεν");
  }
  
  public void testExceptions() throws Exception {
    checkOneTerm(a, "καθεστώτα", "καθεστ");
    checkOneTerm(a, "καθεστώτος", "καθεστ");
    checkOneTerm(a, "καθεστώς", "καθεστ");
    checkOneTerm(a, "καθεστώτων", "καθεστ");
    
    checkOneTerm(a, "χουμε", "χουμ");
    checkOneTerm(a, "χουμ", "χουμ");
    
    checkOneTerm(a, "υποταγεσ", "υποταγ");
    checkOneTerm(a, "υποταγ", "υποταγ");
    
    checkOneTerm(a, "εμετε", "εμετ");
    checkOneTerm(a, "εμετ", "εμετ");
    
    checkOneTerm(a, "αρχοντασ", "αρχοντ");
    checkOneTerm(a, "αρχοντων", "αρχοντ");
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new GreekStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
