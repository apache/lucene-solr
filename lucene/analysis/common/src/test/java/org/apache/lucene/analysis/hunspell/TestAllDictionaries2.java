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
package org.apache.lucene.analysis.hunspell;


import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.RamUsageTester;
import org.apache.lucene.util.TestUtil;
import org.junit.Ignore;

/**
 * These thunderbird dictionaries can be retrieved via:
 * https://addons.mozilla.org/en-US/thunderbird/language-tools/
 * You must click and download every file: sorry!
 */
@Ignore("enable manually")
@SuppressSysoutChecks(bugUrl = "prints important memory utilization stats per dictionary")
public class TestAllDictionaries2 extends LuceneTestCase {
  
  // set this to the location of where you downloaded all the files
  static final Path DICTIONARY_HOME = Paths.get("/data/thunderbirdDicts");
  
  final String tests[] = {
    /* zip file */                                                                    /* dictionary */                      /* affix */
    "addon-0.4.5-an+fx+tb+fn+sm.xpi",                                                 "dictionaries/ru.dic",                "dictionaries/ru.aff",
    "addon-0.5.5-fx+tb.xpi",                                                          "dictionaries/ko-KR.dic",             "dictionaries/ko-KR.aff",
    "afrikaans_spell_checker-20110323-fx+tb+fn+sm.xpi",                               "dictionaries/af-ZA.dic",             "dictionaries/af-ZA.aff",
    "albanisches_worterbuch-1.6.9-fx+tb+sm+fn.xpi",                                   "dictionaries/sq.dic",                "dictionaries/sq.aff",
    "amharic_spell_checker-0.4-fx+fn+tb+sm.xpi",                                      "dictionaries/am_ET.dic",             "dictionaries/am_ET.aff",
    "arabic_spell_checking_dictionary-3.2.20120321-fx+tb.xpi",                        "dictionaries/ar.dic",                "dictionaries/ar.aff",
    "armenian_spell_checker_dictionary-0.32-fx+tb+sm.xpi",                            "dictionaries/hy_AM.dic",             "dictionaries/hy_AM.aff",
    "azerbaijani_spell_checker-0.3-fx+tb+fn+sm+sb.xpi",                               "dictionaries/az-Latn-AZ.dic",        "dictionaries/az-Latn-AZ.aff",
    "belarusian_classic_dictionary-0.1.2-tb+fx+sm.xpi",                               "dictionaries/be-classic.dic",        "dictionaries/be-classic.aff",
    "belarusian_dictionary-0.1.2-fx+sm+tb.xpi",                                       "dictionaries/be.dic",                "dictionaries/be.aff",
    "bengali_bangladesh_dictionary-0.08-sm+tb+fx.xpi",                                "dictionaries/bn-BD.dic",             "dictionaries/bn-BD.aff",
    "brazilian_portuguese_dictionary_former_spelling-28.20140203-tb+sm+fx.xpi",       "dictionaries/pt-BR-antigo.dic",      "dictionaries/pt-BR-antigo.aff",
    "brazilian_portuguese_dictionary_new_spelling-28.20140203-fx+sm+tb.xpi",          "dictionaries/pt-BR.dic",             "dictionaries/pt-BR.aff",
    "british_english_dictionary_updated-1.19.5-sm+fx+tb.xpi",                         "dictionaries/en-GB.dic",             "dictionaries/en-GB.aff",
    "bulgarian_dictionary-4.3-fx+tb+sm.xpi",                                          "dictionaries/bg.dic",                "dictionaries/bg.aff",
    "canadian_english_dictionary-2.0.8-fx+sm+tb.xpi",                                 "dictionaries/en-CA.dic",             "dictionaries/en-CA.aff",
    "ceske_slovniky_pro_kontrolu_pravopisu-1.0.4-tb+sm+fx.xpi",                       "dictionaries/cs.dic",                "dictionaries/cs.aff",
    "chichewa_spell_checker-0.3-fx+tb+fn+sm+sb.xpi",                                  "dictionaries/ny_MW.dic",             "dictionaries/ny_MW.aff",
    "corrector_de_galego-13.10.0-fn+sm+tb+fx.xpi",                                    "dictionaries/gl_ES.dic",             "dictionaries/gl_ES.aff",
//BUG: broken flags "corrector_orthographic_de_interlingua-6.0-fn+sm+tb+fx.xpi",                      "dictionaries/ia-ia.dic",             "dictionaries/ia-ia.aff",
    "corrector_ortografico_aragones-0.2-fx+tb+sm.xpi",                                "dictionaries/an_ES.dic",             "dictionaries/an_ES.aff",
    "croatian_dictionary_-_hrvatski_rjecnik-1.0.1-firefox+thunderbird+seamonkey.xpi", "dictionaries/hr.dic",                "dictionaries/hr.aff",
    "croatian_dictionary_hrvatski_rjecnik-1.0.9-an+fx+fn+tb+sm.xpi",                  "dictionaries/hr-HR.dic",             "dictionaries/hr-HR.aff",
    "dansk_ordbog_til_stavekontrollen-2.2.1-sm+tb+fx.xpi",                            "dictionaries/da.dic",                "dictionaries/da.aff",
    "deutsches_worterbuch_de_de_alte_rechtschreibung-2.1.8-sm.xpi",                   "dictionaries/de-DE-1901.dic",        "dictionaries/de-DE-1901.aff",
    "diccionario_de_espanolespana-1.7-sm+tb+fn+fx.xpi",                               "dictionaries/es-ES.dic",             "dictionaries/es-ES.aff",
    "diccionario_en_espanol_para_venezuela-1.1.17-sm+an+tb+fn+fx.xpi",                "dictionaries/es_VE.dic",             "dictionaries/es_VE.aff",
    "diccionario_espanol_argentina-2.5.1-tb+fx+sm.xpi",                               "dictionaries/es_AR.dic",             "dictionaries/es_AR.aff",
    "diccionario_espanol_mexico-1.1.3-fn+tb+fx+sm.xpi",                               "dictionaries/es_MX.dic",             "dictionaries/es_MX.aff",
    "diccionario_ortografico_valenciano-2.2.0-fx+tb+fn+sm.xpi",                       "dictionaries/roa-ES-val.dic",        "dictionaries/roa-ES-val.aff",
    "diccionario_papiamentoaruba-0.2-fn+sm+tb+fx.xpi",                                "dictionaries/Papiamento.dic",        "dictionaries/Papiamento.aff",
    "dictionnaires_francais-5.0.2-fx+tb+sm.xpi",                                      "dictionaries/fr-classic-reform.dic", "dictionaries/fr-classic-reform.aff",
    "dictionnaires_francais-5.0.2-fx+tb+sm.xpi",                                      "dictionaries/fr-classic.dic",        "dictionaries/fr-classic.aff",
    "dictionnaires_francais-5.0.2-fx+tb+sm.xpi",                                      "dictionaries/fr-modern.dic",         "dictionaries/fr-modern.aff",
    "dictionnaires_francais-5.0.2-fx+tb+sm.xpi",                                      "dictionaries/fr-reform.dic",         "dictionaries/fr-reform.aff",
    "difazier_an_drouizig-0.12-tb+sm+fx.xpi",                                         "dictionaries/br.dic",                "dictionaries/br.aff",
    "dikshonario_papiamentuantia_hulandes-0.5-fx+tb+fn+sb+sm.xpi",                    "dictionaries/Papiamentu.dic",        "dictionaries/Papiamentu.aff",
    "dizionari_furlan-3.1-tb+fx+sm.xpi",                                              "dictionaries/fur-IT.dic",            "dictionaries/fur-IT.aff",
    "dizionario_italiano-3.3.2-fx+sm+tb.xpi",                                         "dictionaries/it_IT.dic",             "dictionaries/it_IT.aff",
    "eesti_keele_speller-3.2-fx+tb+sm.xpi",                                           "dictionaries/et-EE.dic",             "dictionaries/et-EE.aff",
    "english_australian_dictionary-2.1.2-tb+fx+sm.xpi",                               "dictionaries/en-AU.dic",             "dictionaries/en-AU.aff",
    "esperanta_vortaro-1.0.2-fx+tb+sm.xpi",                                           "dictionaries/eo-EO.dic",             "dictionaries/eo-EO.aff",
    "european_portuguese_spellchecker-14.1.1.1-tb+fx.xpi",                            "dictionaries/pt-PT.dic",             "dictionaries/pt-PT.aff",
    "faroese_spell_checker_faroe_islands-2.0-tb+sm+fx+fn.xpi",                        "dictionaries/fo_FO.dic",             "dictionaries/fo_FO.aff",
    "frysk_wurdboek-2.1.1-fn+sm+fx+an+tb.xpi",                                        "dictionaries/fy.dic",                "dictionaries/fy.aff",
    "geiriadur_cymraeg-1.08-tb+sm+fx.xpi",                                            "dictionaries/cy_GB.dic",             "dictionaries/cy_GB.aff",
    "general_catalan_dictionary-2.5.0-tb+sm+fn+fx.xpi",                               "dictionaries/ca.dic",                "dictionaries/ca.aff",
    "german_dictionary-2.0.3-fn+fx+sm+tb.xpi",                                        "dictionaries/de-DE.dic",             "dictionaries/de-DE.aff",
    "german_dictionary_de_at_new_orthography-20130905-tb+fn+an+fx+sm.xpi",            "dictionaries/de-AT.dic",             "dictionaries/de-AT.aff",
    "german_dictionary_de_ch_new_orthography-20130905-fx+tb+fn+sm+an.xpi",            "dictionaries/de-CH.dic",             "dictionaries/de-CH.aff",
    "german_dictionary_de_de_new_orthography-20130905-tb+sm+an+fn+fx.xpi",            "dictionaries/de-DE.dic",             "dictionaries/de-DE.aff",
    "german_dictionary_extended_for_austria-2.0.3-fx+fn+sm+tb.xpi",                   "dictionaries/de-AT.dic",             "dictionaries/de-AT.aff",
    "german_dictionary_switzerland-2.0.3-sm+fx+tb+fn.xpi",                            "dictionaries/de-CH.dic",             "dictionaries/de-CH.aff",
    "greek_spelling_dictionary-0.8.5-fx+tb+sm.xpi",                                   "dictionaries/el-GR.dic",             "dictionaries/el-GR.aff",
    "gujarati_spell_checker-0.3-fx+tb+fn+sm+sb.xpi",                                  "dictionaries/gu_IN.dic",             "dictionaries/gu_IN.aff",
    "haitian_creole_spell_checker-0.08-tb+sm+fx.xpi",                                 "dictionaries/ht-HT.dic",             "dictionaries/ht-HT.aff",
    "hausa_spelling_dictionary-0.2-tb+fx.xpi",                                        "dictionaries/ha-GH.dic",             "dictionaries/ha-GH.aff",
    "hebrew_spell_checking_dictionary_from_hspell-1.2.0.1-fx+sm+tb.xpi",              "dictionaries/he.dic",                "dictionaries/he.aff",
    "hindi_spell_checker-0.4-fx+tb+sm+sb+fn.xpi",                                     "dictionaries/hi_IN.dic",             "dictionaries/hi_IN.aff",
    "hungarian_dictionary-1.6.1.1-fx+tb+sm+fn.xpi",                                   "dictionaries/hu.dic",                "dictionaries/hu.aff",
//BUG: has no encoding declaration "icelandic_dictionary-1.3-fx+tb+sm.xpi",                                          "dictionaries/is.dic",                "dictionaries/is.aff",
    "kamus_pengecek_ejaan_bahasa_indonesia-1.1-fx+tb.xpi",                            "dictionaries/id.dic",                "dictionaries/id.aff",
    "kannada_spell_checker-2.0.1-tb+sm+fn+an+fx.xpi",                                 "dictionaries/kn.dic",                "dictionaries/kn.aff",
    "kashubian_spell_checker_poland-0.9-sm+tb+fx.xpi",                                "dictionaries/Kaszebsczi.dic",        "dictionaries/Kaszebsczi.aff",
    "kiswahili_spell_checker-0.3-sb+tb+fn+fx+sm.xpi",                                 "dictionaries/sw_TZ.dic",             "dictionaries/sw_TZ.aff",
    "kurdish_spell_checker-0.96-fx+tb+sm.xpi",                                        "dictionaries/ku-TR.dic",             "dictionaries/ku-TR.aff",
    "lao_spellchecking_dictionary-0-fx+tb+sm+fn+an.xpi",                              "dictionaries/lo_LA.dic",             "dictionaries/lo_LA.aff",
    "latviesu_valodas_pareizrakstibas_parbaudes_vardnica-1.0.0-fn+fx+tb+sm.xpi",      "dictionaries/lv_LV.dic",             "dictionaries/lv_LV.aff",
    "lithuanian_spelling_check_dictionary-1.3-fx+tb+sm+fn.xpi",                       "dictionaries/lt.dic",                "dictionaries/lt.aff",
    "litreoir_gaelspell_do_mhozilla-4.7-tb+fx+sm+fn.xpi",                             "dictionaries/ga.dic",                "dictionaries/ga.aff",
    "litreoir_na_liongailise-0.03-fx+sm+tb.xpi",                                      "dictionaries/ln-CD.dic",             "dictionaries/ln-CD.aff",
    "macedonian_mk_mk_spellchecker-1.2-fn+tb+fx+sm+sb.xpi",                           "dictionaries/mk-MK-Cyrl.dic",        "dictionaries/mk-MK-Cyrl.aff",
    "macedonian_mk_mk_spellchecker-1.2-fn+tb+fx+sm+sb.xpi",                           "dictionaries/mk-MK-Latn.dic",        "dictionaries/mk-MK-Latn.aff",
    "malagasy_spell_checker-0.3-fn+tb+fx+sm+sb.xpi",                                  "dictionaries/mg_MG.dic",             "dictionaries/mg_MG.aff",
    "marathi_dictionary-9.3-sm+tb+sb+fx.xpi",                                         "dictionaries/mr-IN.dic",             "dictionaries/mr-IN.aff",
    "ndebele_south_spell_checker-20110323-tb+fn+fx+sm.xpi",                           "dictionaries/nr-ZA.dic",             "dictionaries/nr-ZA.aff",
    "nepali_dictionary-1.2-fx+tb.xpi",                                                "dictionaries/ne_NP.dic",             "dictionaries/ne_NP.aff",
    "norsk_bokmal_ordliste-2.0.10.2-fx+tb+sm.xpi",                                    "dictionaries/nb.dic",                "dictionaries/nb.aff",
    "norsk_nynorsk_ordliste-2.1.0-sm+fx+tb.xpi",                                      "dictionaries/nn.dic",                "dictionaries/nn.aff",
    "northern_sotho_spell_checker-20110323-tb+fn+fx+sm.xpi",                          "dictionaries/nso-ZA.dic",            "dictionaries/nso-ZA.aff",
    "oriya_spell_checker-0.3-fn+tb+fx+sm+sb.xpi",                                     "dictionaries/or-IN.dic",             "dictionaries/or-IN.aff",
    "polski_slownik_poprawnej_pisowni-1.0.20110621-fx+tb+sm.xpi",                     "dictionaries/pl.dic",                "dictionaries/pl.aff",
    "punjabi_spell_checker-0.3-fx+tb+sm+sb+fn.xpi",                                   "dictionaries/pa-IN.dic",             "dictionaries/pa-IN.aff",
    "romanian_spellchecking_dictionary-1.14-sm+tb+fx.xpi",                            "dictionaries/ro_RO-ante1993.dic",    "dictionaries/ro_RO-ante1993.aff",
    "russian_hunspell_dictionary-1.0.20131101-tb+sm+fn+fx.xpi",                       "dictionaries/ru_RU.dic",             "dictionaries/ru_RU.aff",
    "sanskrit_spell_checker-1.1-fx+tb+sm+sb+fn.xpi",                                  "dictionaries/sa_IN.dic",             "dictionaries/sa_IN.aff",
    "scottish_gaelic_spell_checker-2.7-tb+fx+sm.xpi",                                 "dictionaries/gd-GB.dic",             "dictionaries/gd-GB.aff",
    "serbian_dictionary-0.18-fx+tb+sm.xpi",                                           "dictionaries/sr-RS-Cyrl.dic",        "dictionaries/sr-RS-Cyrl.aff",
    "serbian_dictionary-0.18-fx+tb+sm.xpi",                                           "dictionaries/sr-RS-Latn.dic",        "dictionaries/sr-RS-Latn.aff",
    "slovak_spell_checking_dictionary-2.04.0-tb+fx+sm.xpi",                           "dictionaries/sk-SK.dic",             "dictionaries/sk-SK.aff",
    "slovak_spell_checking_dictionary-2.04.0-tb+fx+sm.xpi",                           "dictionaries/sk-SK-ascii.dic",       "dictionaries/sk-SK-ascii.aff",
    "slovar_za_slovenski_jezik-0.1.1.1-fx+tb+sm.xpi",                                 "dictionaries/sl.dic",                "dictionaries/sl.aff",
    "songhay_spell_checker-0.03-fx+tb+sm.xpi",                                        "dictionaries/Songhay - Mali.dic",    "dictionaries/Songhay - Mali.aff",
    "southern_sotho_spell_checker-20110323-tb+fn+fx+sm.xpi",                          "dictionaries/st-ZA.dic",             "dictionaries/st-ZA.aff",
    "sownik_acinski-0.41.20110603-tb+fx+sm.xpi",                                      "dictionaries/la.dic",                "dictionaries/la.aff",
    "sownik_jezyka_dolnouzyckiego-1.4.8-an+fx+tb+fn+sm.xpi",                          "dictionaries/dsb.dic",               "dictionaries/dsb.aff",
    "srpska_latinica-0.1-fx+tb+sm.xpi",                                               "dictionaries/Srpski_latinica.dic",   "dictionaries/Srpski_latinica.aff",
    "svenska_fria_ordlistan-1.1-tb+sm+fx.xpi",                                        "dictionaries/sv.dic",                "dictionaries/sv.aff",
    "svenska_fria_ordlistan-1.1-tb+sm+fx.xpi",                                        "dictionaries/sv_FI.dic",             "dictionaries/sv_FI.aff",
    "swati_spell_checker-20110323-tb+sm+fx+fn.xpi",                                   "dictionaries/ss-ZA.dic",             "dictionaries/ss-ZA.aff",
    "tamil_spell_checker_for_firefox-0.4-tb+fx.xpi",                                  "dictionaries/ta-TA.dic",             "dictionaries/ta-TA.aff",
    "telugu_spell_checker-0.3-tb+fx+sm.xpi",                                          "dictionaries/te_IN.dic",             "dictionaries/te_IN.aff",
    "te_papakupu_m__ori-0.9.9.20080630-fx+tb.xpi",                                    "dictionaries/mi-x-Tai Tokerau.dic",  "dictionaries/mi-x-Tai Tokerau.aff",
    "te_papakupu_m__ori-0.9.9.20080630-fx+tb.xpi",                                    "dictionaries/mi.dic",                "dictionaries/mi.aff",
//BUG: broken file (hunspell refuses to load, too)    "thamizha_solthiruthitamil_spellchecker-0.8-fx+tb.xpi",                           "dictionaries/ta_IN.dic",             "dictionaries/ta_IN.aff",
    "tsonga_spell_checker-20110323-tb+sm+fx+fn.xpi",                                  "dictionaries/ts-ZA.dic",             "dictionaries/ts-ZA.aff",
    "tswana_spell_checker-20110323-tb+sm+fx+fn.xpi",                                  "dictionaries/tn-ZA.dic",             "dictionaries/tn-ZA.aff",
//BUG: missing FLAG declaration "turkce_yazm_denetimi-3.5-sm+tb+fx.xpi",                                          "dictionaries/tr.dic",                "dictionaries/tr.aff",
    "turkmen_spell_checker_dictionary-0.1.6-tb+fx+sm.xpi",                            "dictionaries/tk_TM.dic",             "dictionaries/tk_TM.aff",
    "ukrainian_dictionary-1.7.0-sm+an+fx+fn+tb.xpi",                                  "dictionaries/uk-UA.dic",             "dictionaries/uk-UA.aff",
    "united_states_english_spellchecker-7.0.1-sm+tb+fx+an.xpi",                       "dictionaries/en-US.dic",             "dictionaries/en-US.aff",
    "upper_sorbian_spelling_dictionary-0.0.20060327.3-tb+fx+sm.xpi",                  "dictionaries/hsb.dic",               "dictionaries/hsb.aff",
    "urdu_dictionary-0.64-fx+tb+sm+sb.xpi",                                           "dictionaries/ur.dic",                "dictionaries/ur.aff",
    "uzbek_spell_checker-0.3-fn+tb+fx+sm+sb.xpi",                                     "dictionaries/uz.dic",                "dictionaries/uz.aff",
    "valencian_catalan_dictionary-2.5.0-tb+fn+sm+fx.xpi",                             "dictionaries/ca-ES-valencia.dic",    "dictionaries/ca-ES-valencia.aff",
    "venda_spell_checker-20110323-tb+fn+fx+sm.xpi",                                   "dictionaries/ve-ZA.dic",             "dictionaries/ve-ZA.aff",
    "verificador_ortografico_para_portugues_do_brasil-2.3-3.2b1-tb+sm+fn+fx.xpi",     "dictionaries/pt_BR.dic",             "dictionaries/pt_BR.aff",
    "vietnamese_dictionary-2.1.0.159-an+sm+tb+fx+fn.xpi",                             "dictionaries/vi-DauCu.dic",          "dictionaries/vi-DauCu.aff",
    "vietnamese_dictionary-2.1.0.159-an+sm+tb+fx+fn.xpi",                             "dictionaries/vi-DauMoi.dic",         "dictionaries/vi-DauMoi.aff",
    "woordenboek_nederlands-3.1.1-sm+tb+fx+fn.xpi",                                   "dictionaries/nl.dic",                "dictionaries/nl.aff",
    "xhosa_spell_checker-20110323-tb+fn+fx+sm.xpi",                                   "dictionaries/xh-ZA.dic",             "dictionaries/xh-ZA.aff",
    "xuxen-4.0.1-fx+tb+sm.xpi",                                                       "dictionaries/eu.dic",                "dictionaries/eu.aff",
    "yiddish_spell_checker_yivo-0.0.3-sm+fn+fx+tb.xpi",                               "dictionaries/yi.dic",                "dictionaries/yi.aff",
    "zulu_spell_checker-20110323-tb+fn+fx+sm.xpi",                                    "dictionaries/zu-ZA.dic",             "dictionaries/zu-ZA.aff"
  };
  
  public void test() throws Exception {
    Path tmp = LuceneTestCase.createTempDir();
    
    for (int i = 0; i < tests.length; i += 3) {
      Path f = DICTIONARY_HOME.resolve(tests[i]);
      assert Files.exists(f);
      
      IOUtils.rm(tmp);
      Files.createDirectory(tmp);
      
      try (InputStream in = Files.newInputStream(f)) {
        TestUtil.unzip(in, tmp);
        Path dicEntry = tmp.resolve(tests[i+1]);
        Path affEntry = tmp.resolve(tests[i+2]);
      
        try (InputStream dictionary = Files.newInputStream(dicEntry);
             InputStream affix = Files.newInputStream(affEntry);
             Directory tempDir = newDirectory()) {
          Dictionary dic = new Dictionary(tempDir, "dictionary", affix, dictionary);
          System.out.println(tests[i] + "\t" + RamUsageTester.humanSizeOf(dic) + "\t(" +
                             "words=" + RamUsageTester.humanSizeOf(dic.words) + ", " +
                             "flags=" + RamUsageTester.humanSizeOf(dic.flagLookup) + ", " +
                             "strips=" + RamUsageTester.humanSizeOf(dic.stripData) + ", " +
                             "conditions=" + RamUsageTester.humanSizeOf(dic.patterns) + ", " +
                             "affixData=" + RamUsageTester.humanSizeOf(dic.affixData) + ", " +
                             "prefixes=" + RamUsageTester.humanSizeOf(dic.prefixes) + ", " +
                             "suffixes=" + RamUsageTester.humanSizeOf(dic.suffixes) + ")");
        }
      }
    }
  }
  
  public void testOneDictionary() throws Exception {
    Path tmp = LuceneTestCase.createTempDir();

    String toTest = "hungarian_dictionary-1.6.1.1-fx+tb+sm+fn.xpi";
    for (int i = 0; i < tests.length; i++) {
      if (tests[i].equals(toTest)) {
        Path f = DICTIONARY_HOME.resolve(tests[i]);
        assert Files.exists(f);
        
        IOUtils.rm(tmp);
        Files.createDirectory(tmp);
        
        try (InputStream in = Files.newInputStream(f)) {
          TestUtil.unzip(in, tmp);
          Path dicEntry = tmp.resolve(tests[i+1]);
          Path affEntry = tmp.resolve(tests[i+2]);
        
          try (InputStream dictionary = Files.newInputStream(dicEntry);
               InputStream affix = Files.newInputStream(affEntry);
               Directory tempDir = newDirectory()) {
            new Dictionary(tempDir, "dictionary", affix, dictionary);
          } 
        }
      }
    }    
  }
}
