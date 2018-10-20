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
 * Can be retrieved via:
 * wget --mirror -np http://archive.services.openoffice.org/pub/mirror/OpenOffice.org/contrib/dictionaries/
 * Note some of the files differ only in case. This may be a problem on your operating system!
 */
@Ignore("enable manually")
@SuppressSysoutChecks(bugUrl = "prints important memory utilization stats per dictionary")
public class TestAllDictionaries extends LuceneTestCase {
  
  // set this to the location of where you downloaded all the files
  static final Path DICTIONARY_HOME = 
      Paths.get("/data/archive.services.openoffice.org/pub/mirror/OpenOffice.org/contrib/dictionaries");
  
  final String tests[] = {
    /* zip file */               /* dictionary */       /* affix */
    "af_ZA.zip",                 "af_ZA.dic",           "af_ZA.aff",
    "ak_GH.zip",                 "ak_GH.dic",           "ak_GH.aff",
    "bg_BG.zip",                 "bg_BG.dic",           "bg_BG.aff",
    "ca_ANY.zip",                "catalan.dic",         "catalan.aff",
    "ca_ES.zip",                 "ca_ES.dic",           "ca_ES.aff",
// BUG: broken flag "cop_EG.zip",                "cop_EG.dic",          "cop_EG.aff",
    "cs_CZ.zip",                 "cs_CZ.dic",           "cs_CZ.aff",
    "cy_GB.zip",                 "cy_GB.dic",           "cy_GB.aff",
    "da_DK.zip",                 "da_DK.dic",           "da_DK.aff",
    "de_AT.zip",                 "de_AT.dic",           "de_AT.aff",
    "de_CH.zip",                 "de_CH.dic",           "de_CH.aff",
    "de_DE.zip",                 "de_DE.dic",           "de_DE.aff",
    "de_DE_comb.zip",            "de_DE_comb.dic",      "de_DE_comb.aff",
    "de_DE_frami.zip",           "de_DE_frami.dic",     "de_DE_frami.aff",
    "de_DE_neu.zip",             "de_DE_neu.dic",       "de_DE_neu.aff",
    "el_GR.zip",                 "el_GR.dic",           "el_GR.aff",
    "en_AU.zip",                 "en_AU.dic",           "en_AU.aff",
    "en_CA.zip",                 "en_CA.dic",           "en_CA.aff",
    "en_GB-oed.zip",             "en_GB-oed.dic",       "en_GB-oed.aff",
    "en_GB.zip",                 "en_GB.dic",           "en_GB.aff",
    "en_NZ.zip",                 "en_NZ.dic",           "en_NZ.aff",
    "eo.zip",                    "eo_l3.dic",           "eo_l3.aff",
    "eo_EO.zip",                 "eo_EO.dic",           "eo_EO.aff",
    "es_AR.zip",                 "es_AR.dic",           "es_AR.aff",
    "es_BO.zip",                 "es_BO.dic",           "es_BO.aff",
    "es_CL.zip",                 "es_CL.dic",           "es_CL.aff",
    "es_CO.zip",                 "es_CO.dic",           "es_CO.aff",
    "es_CR.zip",                 "es_CR.dic",           "es_CR.aff",
    "es_CU.zip",                 "es_CU.dic",           "es_CU.aff",
    "es_DO.zip",                 "es_DO.dic",           "es_DO.aff",
    "es_EC.zip",                 "es_EC.dic",           "es_EC.aff",
    "es_ES.zip",                 "es_ES.dic",           "es_ES.aff",
    "es_GT.zip",                 "es_GT.dic",           "es_GT.aff",
    "es_HN.zip",                 "es_HN.dic",           "es_HN.aff",
    "es_MX.zip",                 "es_MX.dic",           "es_MX.aff",
    "es_NEW.zip",                "es_NEW.dic",          "es_NEW.aff",
    "es_NI.zip",                 "es_NI.dic",           "es_NI.aff",
    "es_PA.zip",                 "es_PA.dic",           "es_PA.aff",
    "es_PE.zip",                 "es_PE.dic",           "es_PE.aff",
    "es_PR.zip",                 "es_PR.dic",           "es_PR.aff",
    "es_PY.zip",                 "es_PY.dic",           "es_PY.aff",
    "es_SV.zip",                 "es_SV.dic",           "es_SV.aff",
    "es_UY.zip",                 "es_UY.dic",           "es_UY.aff",
    "es_VE.zip",                 "es_VE.dic",           "es_VE.aff",
    "et_EE.zip",                 "et_EE.dic",           "et_EE.aff",
    "fo_FO.zip",                 "fo_FO.dic",           "fo_FO.aff",
    "fr_FR-1990_1-3-2.zip",      "fr_FR-1990.dic",      "fr_FR-1990.aff",
    "fr_FR-classique_1-3-2.zip", "fr_FR-classique.dic", "fr_FR-classique.aff",
    "fr_FR_1-3-2.zip",           "fr_FR.dic",           "fr_FR.aff",
    "fy_NL.zip",                 "fy_NL.dic",           "fy_NL.aff",
    "ga_IE.zip",                 "ga_IE.dic",           "ga_IE.aff",
    "gd_GB.zip",                 "gd_GB.dic",           "gd_GB.aff",
    "gl_ES.zip",                 "gl_ES.dic",           "gl_ES.aff",
    "gsc_FR.zip",                "gsc_FR.dic",          "gsc_FR.aff",
    "gu_IN.zip",                 "gu_IN.dic",           "gu_IN.aff",
    "he_IL.zip",                 "he_IL.dic",           "he_IL.aff",
    "hi_IN.zip",                 "hi_IN.dic",           "hi_IN.aff",
    "hil_PH.zip",                "hil_PH.dic",          "hil_PH.aff",
    "hr_HR.zip",                 "hr_HR.dic",           "hr_HR.aff",
    "hu_HU.zip",                 "hu_HU.dic",           "hu_HU.aff",
    "hu_HU_comb.zip",            "hu_HU.dic",           "hu_HU.aff",
    "ia.zip",                    "ia.dic",              "ia.aff",
    "id_ID.zip",                 "id_ID.dic",           "id_ID.aff",
    "it_IT.zip",                 "it_IT.dic",           "it_IT.aff",
    "ku_TR.zip",                 "ku_TR.dic",           "ku_TR.aff",
    "la.zip",                    "la.dic",              "la.aff",
    "lt_LT.zip",                 "lt_LT.dic",           "lt_LT.aff",
    "lv_LV.zip",                 "lv_LV.dic",           "lv_LV.aff",
    "mg_MG.zip",                 "mg_MG.dic",           "mg_MG.aff",
    "mi_NZ.zip",                 "mi_NZ.dic",           "mi_NZ.aff",
    "mk_MK.zip",                 "mk_MK.dic",           "mk_MK.aff",
    "mos_BF.zip",                "mos_BF.dic",          "mos_BF.aff",
    "mr_IN.zip",                 "mr_IN.dic",           "mr_IN.aff",
    "ms_MY.zip",                 "ms_MY.dic",           "ms_MY.aff",
    "nb_NO.zip",                 "nb_NO.dic",           "nb_NO.aff",
    "ne_NP.zip",                 "ne_NP.dic",           "ne_NP.aff",
    "nl_NL.zip",                 "nl_NL.dic",           "nl_NL.aff",
    "nl_med.zip",                "nl_med.dic",          "nl_med.aff",
    "nn_NO.zip",                 "nn_NO.dic",           "nn_NO.aff",
    "nr_ZA.zip",                 "nr_ZA.dic",           "nr_ZA.aff",
    "ns_ZA.zip",                 "ns_ZA.dic",           "ns_ZA.aff",
    "ny_MW.zip",                 "ny_MW.dic",           "ny_MW.aff",
    "oc_FR.zip",                 "oc_FR.dic",           "oc_FR.aff",
    "pl_PL.zip",                 "pl_PL.dic",           "pl_PL.aff",
    "pt_BR.zip",                 "pt_BR.dic",           "pt_BR.aff",
    "pt_PT.zip",                 "pt_PT.dic",           "pt_PT.aff",
    "ro_RO.zip",                 "ro_RO.dic",           "ro_RO.aff",
    "ru_RU.zip",                 "ru_RU.dic",           "ru_RU.aff",
    "ru_RU_ye.zip",              "ru_RU_ie.dic",        "ru_RU_ie.aff",
    "ru_RU_yo.zip",              "ru_RU_yo.dic",        "ru_RU_yo.aff",
    "rw_RW.zip",                 "rw_RW.dic",           "rw_RW.aff",
    "sk_SK.zip",                 "sk_SK.dic",           "sk_SK.aff",
    "sl_SI.zip",                 "sl_SI.dic",           "sl_SI.aff",
    "sq_AL.zip",                 "sq_AL.dic",           "sq_AL.aff",
    "ss_ZA.zip",                 "ss_ZA.dic",           "ss_ZA.aff",
    "st_ZA.zip",                 "st_ZA.dic",           "st_ZA.aff",
    "sv_SE.zip",                 "sv_SE.dic",           "sv_SE.aff",
    "sw_KE.zip",                 "sw_KE.dic",           "sw_KE.aff",
    "tet_ID.zip",                "tet_ID.dic",          "tet_ID.aff",
    "th_TH.zip",                 "th_TH.dic",           "th_TH.aff",
    "tl_PH.zip",                 "tl_PH.dic",           "tl_PH.aff",
    "tn_ZA.zip",                 "tn_ZA.dic",           "tn_ZA.aff",
    "ts_ZA.zip",                 "ts_ZA.dic",           "ts_ZA.aff",
    "uk_UA.zip",                 "uk_UA.dic",           "uk_UA.aff",
    "ve_ZA.zip",                 "ve_ZA.dic",           "ve_ZA.aff",
    "vi_VN.zip",                 "vi_VN.dic",           "vi_VN.aff",
    "xh_ZA.zip",                 "xh_ZA.dic",           "xh_ZA.aff",
    "zu_ZA.zip",                 "zu_ZA.dic",           "zu_ZA.aff",
  };
  
  public void test() throws Exception {
    Path tmp = LuceneTestCase.createTempDir();
    
    for (int i = 0; i < tests.length; i += 3) {
      Path f = DICTIONARY_HOME.resolve(tests[i]);
      assert Files.exists(f);
      
      IOUtils.rm(tmp);
      Files.createDirectory(tmp);
      
      try (InputStream in = Files.newInputStream(f); Directory tempDir = getDirectory()) {
        TestUtil.unzip(in, tmp);
        Path dicEntry = tmp.resolve(tests[i+1]);
        Path affEntry = tmp.resolve(tests[i+2]);
      
        try (InputStream dictionary = Files.newInputStream(dicEntry);
             InputStream affix = Files.newInputStream(affEntry)) {
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

    String toTest = "zu_ZA.zip";
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
               Directory tempDir = getDirectory()) {
            new Dictionary(tempDir, "dictionary", affix, dictionary);
          } 
        }
      }
    }    
  }

  private Directory getDirectory() {
    return newDirectory();
  }
}
