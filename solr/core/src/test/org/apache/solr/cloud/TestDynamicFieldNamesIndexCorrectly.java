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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.hamcrest.core.IsCollectionContaining;
import org.hamcrest.core.IsEqual;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SolrTestCaseJ4.SuppressSSL
// Tests https://issues.apache.org/jira/browse/SOLR-13963
public class TestDynamicFieldNamesIndexCorrectly extends AbstractFullDistribZkTestBase {

  private static final String COLLECTION = "test";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  @BaseDistributedSearchTestCase.ShardsFixed(num = 3)
  public void test() throws Exception {
    waitForThingsToLevelOut(30, TimeUnit.SECONDS);

    createCollection(COLLECTION, "conf1", 4, 1, 4);
    final int numRuns = 10;
    populateIndex(numRuns);
  }

  void populateIndex(int numRuns) throws IOException, SolrServerException {
    try {
      for (int i = 0; i < numRuns; i++) {
        log.debug("Iteration number: {}", i);
        cloudClient.deleteByQuery(COLLECTION, "*:*");
        cloudClient.commit(COLLECTION);

        final Collection<SolrInputDocument> solrDocs = generateRandomizedFieldDocuments();
        addToSolr(solrDocs);

        final SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(solrDocs.size());
        final SolrDocumentList resultDocs = getSolrResponse(solrQuery, COLLECTION);
        log.debug("{}", resultDocs);
        assertThatDocsHaveCorrectFields(solrDocs, resultDocs);
      }
    } finally {
      cloudClient.close();
    }
  }

  private void assertThatDocsHaveCorrectFields(final Collection<SolrInputDocument> solrDocs,
      final SolrDocumentList resultDocs) {
    assertEquals("Wrong number of docs found", resultDocs.getNumFound(), solrDocs.size());
    final Map<Object,SolrDocument> resultMap = resultDocs.stream()
        .collect(Collectors.toMap(doc -> doc.getFieldValue("id"), doc -> doc));
    Iterator<SolrInputDocument> it = solrDocs.iterator();
    while (it.hasNext()) {
      final SolrInputDocument inDoc = it.next();
      final String id = inDoc.getField("id").getValue().toString();
      final SolrDocument resultDoc = resultMap.get(id);
      final Collection<String> resultFieldNames = resultDoc.getFieldNames();
      inDoc
          .getFieldNames()
          .forEach(
              fieldName -> {
                assertThat(
                    String.format(Locale.ROOT, "Doc %s does not have field %s, it has %s", id, fieldName,
                        resultFieldNames),
                    resultFieldNames, new IsCollectionContaining<>(new IsEqual<>(fieldName)));
              });
    }
  }

  public SolrDocumentList getSolrResponse(SolrQuery solrQuery, String collection)
      throws SolrServerException, IOException {
    final QueryResponse response;
    SolrDocumentList list = null;
    final QueryRequest req = new QueryRequest(solrQuery);
    cloudClient.setDefaultCollection(collection);
    response = req.process(cloudClient);
    list = response.getResults();
    return list;
  }

  private void addToSolr(Collection<SolrInputDocument> solrDocs)
      throws IOException, SolrServerException {
    cloudClient.add(COLLECTION, solrDocs.iterator());
    cloudClient.commit(COLLECTION);
  }

  public static List<SolrInputDocument> generateRandomizedFieldDocuments() {
    final List<SolrInputDocument> solrDocs = new ArrayList<>();

    final Iterator<String> iterator = FIELD_NAMES.iterator();
    int id = 0;
    while (iterator.hasNext()) {
      solrDocs.add(nextDoc(id++, iterator));
    }
    return solrDocs;
  }

  public static SolrInputDocument nextDoc(int id, Iterator<String> iterator) {
    final SolrInputDocument solrDoc = new SolrInputDocument();
    solrDoc.addField("id", id);
    final String nameField = iterator.next();
    solrDoc.addField(nameField, "Somebody");
    return solrDoc;
  }

  private static final List<String> FIELD_NAMES = Arrays.asList(
      new String[] {
          "name_DfsqCIYgwMpJnc_prop_s",
          "name_VHzHTZWnqGALJJ_prop_s",
          "name_OyKmIqynBbK_prop_s",
          "name_JofvOXUMYQs_prop_s",
          "name_SaAfmgHXbCIUethh_prop_s",
          "name_CMajAPNHivraqKBmYxH_prop_s",
          "name_OpJFcSZHuOFVKs_prop_s",
          "name_fTaolBrXTGpJ_prop_s",
          "name_hlgpuaRTRmYjBNmzHBI_prop_s",
          "name_DGSzgfeiMouuTgbaklJ_prop_s",
          "name_hTAZuAysueB_prop_s",
          "name_VqztpEqzBCXEhVM_prop_s",
          "name_CaJSsxLqxhq_prop_s",
          "name_JjEYNobdJiyAJ_prop_s",
          "name_GGpLbFvxdFyBH_prop_s",
          "name_NIfhcAmufHRwaGNuO_prop_s",
          "name_wRzKYNtwiUapyzjQh_prop_s",
          "name_UonaDljKBYUMgMV_prop_s",
          "name_sByosZWJLlrrFYVXaT_prop_s",
          "name_HKHToAtQQkPMwNyGr_prop_s",
          "name_HJBQHPKbxHvPGp_prop_s",
          "name_UtERukPiRHzqv_prop_s",
          "name_WIevbvmoKJkcr_prop_s",
          "name_YjoCtbikMRaY_prop_s",
          "name_OwuVrwcxslmiWMylkuH_prop_s",
          "name_eEoZobamQfJLad_prop_s",
          "name_IWkfNtxsTRbuPIT_prop_s",
          "name_rZphZcqVQN_prop_s",
          "name_QbePjDfrPkiUySUfSS_prop_s",
          "name_ABCPaNPQXBwVJh_prop_s",
          "name_OitLZpkeOXrOAeITlAc_prop_s",
          "name_GlGQselWNwuHUSPy_prop_s",
          "name_XDNBBpHaxD_prop_s",
          "name_NkSQtvNhCwgPxnuRGGK_prop_s",
          "name_mkYuyjFfWjEb_prop_s",
          "name_JUOzeuNelNHbQVlj_prop_s",
          "name_CuzbqxBlEJEnBdeJo_prop_s",
          "name_GbpIJAqoVP_prop_s",
          "name_oPozbuiwFXFoCQ_prop_s",
          "name_QPcamTHGpEgYGW_prop_s",
          "name_QfgfGrTZZkqIbLq_prop_s",
          "name_UtkepJfqAPQQZvDnB_prop_s",
          "name_ShipLvibadhd_prop_s",
          "name_wAdEXOEAydT_prop_s",
          "name_YiquTYZxxNsxanQ_prop_s",
          "name_hJfuWEBCYIdtcixldUy_prop_s",
          "name_PzYofpLhvtw_prop_s",
          "name_rhkJFHishBuS_prop_s",
          "name_GNUoUCaqqfGErM_prop_s",
          "name_hSrbCrBUEs_prop_s",
          "name_xJANZEGtTrIXMDLBgL_prop_s",
          "name_pOhSitCAKl_prop_s",
          "name_PkBHXUceEgVP_prop_s",
          "name_fvDrPKkegWr_prop_s",
          "name_HVzmAutUrUoicr_prop_s",
          "name_ouFhihsihDk_prop_s",
          "name_eeFcnImKkXiKXDTIPC_prop_s",
          "name_NMEsrYgSBoIEwp_prop_s",
          "name_yqCQGPzCamFqBwLZiiC_prop_s",
          "name_JlHlxPykBl_prop_s",
          "name_lYGskGWJfNhnd_prop_s",
          "name_ifXTlDnYqUmjFNhKOxq_prop_s",
          "name_uaCtJcjZWu_prop_s",
          "name_LzSXDKQdhQ_prop_s",
          "name_TpvZetClsYcJenPCdW_prop_s",
          "name_NPsQNyfkDCgNus_prop_s",
          "name_zMZnwFtVnbdlGncBEf_prop_s",
          "name_dGDCXTxABxh_prop_s",
          "name_JIOxBoRhiZLD_prop_s",
          "name_smVTZaCZZMiSmYq_prop_s",
          "name_VgCZTMfOHpfAlGUjDxT_prop_s",
          "name_HhtLeCOGJMNLMXFBgI_prop_s",
          "name_QpzFZXNIpk_prop_s",
          "name_obTfzXxBoCXpiGFGWuz_prop_s",
          "name_VrBTsQmfJoqNI_prop_s",
          "name_QeXnmsrvSYZBtkWwDxs_prop_s",
          "name_vtvvKPfpTBBBMuMTZZ_prop_s",
          "name_VvPvDbWJXsXIAUSNWgW_prop_s",
          "name_BYCAfIaRKVUvHHBIut_prop_s",
          "name_srwPMPauluyfyM_prop_s",
          "name_YlrFboTEUfq_prop_s",
          "name_vIPAkvspnnT_prop_s",
          "name_XWVkDyVpkZvo_prop_s",
          "name_tJDzyfWZtOrzwvuw_prop_s",
          "name_mvfaMcKLduLXcvol_prop_s",
          "name_OKvQYLTaCWwGTXDboK_prop_s",
          "name_VkMXjFZGUQgNWDbKbgp_prop_s",
          "name_IixctxAiJdqQQlPwV_prop_s",
          "name_LbOxzyxGVrsKyZgCHKi_prop_s",
          "name_YtJheZqyzPhpuAAitN_prop_s",
          "name_IsctRhBopyx_prop_s",
          "name_xrfxhlkidKabA_prop_s",
          "name_MFqGPFbIOrneplmaOK_prop_s",
          "name_fXOsAXXtMnLy_prop_s",
          "name_ATQmfQzgdOlFPuDp_prop_s",
          "name_rFrgtZZDVFGuHjteUX_prop_s",
          "name_qcPrtNSRKfBPvtdXWJJ_prop_s",
          "name_UpInzgFgMlfOuMuffOa_prop_s",
          "name_cmwSPLLLuiv_prop_s",
          "name_WDQjhkEHQabWvK_prop_s",
          "name_BqSJaaLDBTTVy_prop_s",
          "name_nqXaRkhFXV_prop_s",
          "name_GJBYZZXOOlyJ_prop_s",
          "name_khgXzOmSxxrerikblPC_prop_s",
          "name_uFNMtGvQQJljSgk_prop_s",
          "name_yoZRduwiqx_prop_s",
          "name_GqqWeEyYXEwT_prop_s",
          "name_tzhVSqoPKt_prop_s",
          "name_ensyGAXGQSuW_prop_s",
          "name_LQJmrvSWKQHc_prop_s",
          "name_KPpikIjkpciF_prop_s",
          "name_mplQAMNcigYEwNEBT_prop_s",
          "name_idmsrYlJGoizvsllQsW_prop_s",
          "name_rMPMEsrySqUVwcDaUE_prop_s",
          "name_febnQEKdThaqhnghZ_prop_s",
          "name_XxtOzKGvvSguMNS_prop_s",
          "name_VtFlQvelTPyz_prop_s",
          "name_PQYUOnhHJsSaqVDH_prop_s",
          "name_qQEIWMsRNQAV_prop_s",
          "name_rPPHpYLbLoUiLYQ_prop_s",
          "name_wZaRlynJFNvWJKjyyuA_prop_s",
          "name_sOwZhIRXUlCvaqRn_prop_s",
          "name_omkQRxJuYPLTeB_prop_s",
          "name_fVJbGrSpMpO_prop_s",
          "name_wLYQtojRTtWeQfz_prop_s",
          "name_dlQxbbzWoAEDbRPFy_prop_s",
          "name_SkYKoVihqWDXnsH_prop_s",
          "name_whlpGhuMeZA_prop_s",
          "name_iOsqSwnNKSNrjLmkpvo_prop_s",
          "name_dWYzrxvJttwv_prop_s",
          "name_stOcVzqQedeqagmynaG_prop_s",
          "name_NENunrnlQI_prop_s",
          "name_HqeTpJDHOsfpawjehIq_prop_s",
          "name_RPwyjltiltvDOqpsYi_prop_s",
          "name_znVAkUDVYWMIoLr_prop_s",
          "name_jTzTSvTRyguN_prop_s",
          "name_ySeOANIBnMabQvaru_prop_s",
          "name_SadaPaYJaxkwHkRMuE_prop_s",
          "name_JQVolDkiGeuvA_prop_s",
          "name_NtxjSaBccGJWoK_prop_s",
          "name_WvwitdcFXPUQny_prop_s",
          "name_JQGUUVnzyMCJs_prop_s",
          "name_GqDdyBcHznboeW_prop_s",
          "name_RlRSvAFEykA_prop_s",
          "name_TvNERqviFBnOCtemES_prop_s",
          "name_DUlAWwbaslagWbIImdd_prop_s",
          "name_gWILZCZRlbjBoQdrP_prop_s",
          "name_ftNrYHWFvhuGYHuJt_prop_s",
          "name_QYBKgeSLCQeRUX_prop_s",
          "name_PYUIqToJNDgWASGFr_prop_s",
          "name_zBZIhwwifmRTOXe_prop_s",
          "name_hnPUucMPfhUuJoO_prop_s",
          "name_agZLOYIoOWl_prop_s",
          "name_SEgmWAjhjJ_prop_s",
          "name_pUclNPUSiDZtMg_prop_s",
          "name_LjIrSIDJqoqL_prop_s",
          "name_vjHbgxEULpsQiZlUaM_prop_s",
          "name_eymEZtHNKYjWFEUlbR_prop_s",
          "name_tQmOnPEwkIMJlzPRG_prop_s",
          "name_ogsTpGUlFLOvLzl_prop_s",
          "name_jJfXDLSaOuHI_prop_s",
          "name_tBfQFKUYmaAeR_prop_s",
          "name_rzFgVahQrXezOIMy_prop_s",
          "name_qdjFkPulsPpMLXVPp_prop_s",
          "name_rWetgUNXaoxXIfbPDz_prop_s",
          "name_OrSAGeTkkrRUygOLG_prop_s",
          "name_LoeOnHUogQnvFHbvXCQ_prop_s",
          "name_wCbfoExoqlldz_prop_s",
          "name_mAyvGeccKbSpO_prop_s",
          "name_LAlRNXNqtwdF_prop_s",
          "name_CzQuGtKdZviLIh_prop_s",
          "name_pkfyyloJeQLCiclF_prop_s",
          "name_BBabvpGlueqCqEAJq_prop_s",
          "name_yMyCCNWJarW_prop_s",
          "name_rXyBgzPnWqnU_prop_s",
          "name_yjTcYotQfUVXVp_prop_s",
          "name_iQulShIGGjlJuGtkOk_prop_s",
          "name_EAMjjKBtOri_prop_s",
          "name_cKKMdfEVvOY_prop_s",
          "name_HCgMMUWJhAPUcSYEw_prop_s",
          "name_QxAiEPSPFcGdpbsAN_prop_s",
          "name_uRFDixdPAlsNiZ_prop_s",
          "name_ctffdxcrVBN_prop_s",
          "name_mdXIbwncmwHgDmfsiAM_prop_s",
          "name_gKlSaxAfDdYgt_prop_s",
          "name_juaOrDYjSfvcmkd_prop_s",
          "name_YadqjaxLPXUpJCIMdNm_prop_s",
          "name_jlNcOgAYUBoj_prop_s",
          "name_AKNbQWFRzzYbAhOlqAI_prop_s",
          "name_JAzAPnrljRhqbNfdoh_prop_s",
          "name_kXYgLRfqrYiQxRo_prop_s",
          "name_AfZylgVaZgvaIQgR_prop_s",
          "name_XaOBvJEVEw_prop_s",
          "name_hDwJONxcscyJuzYRH_prop_s",
          "name_SvogOicRPq_prop_s",
          "name_RIsXETbdCtBuL_prop_s",
          "name_jOxeorqpGcdkp_prop_s",
          "name_IBzKXorZDdowJujJkC_prop_s",
          "name_kWfsavjmSEIyGxeoz_prop_s",
          "name_DhaoVQSvJZfy_prop_s",
          "name_dWSNRommSreSW_prop_s",
          "name_LWqiEKFOMPVklmFwyoX_prop_s",
          "name_GVazWdylOnyamFiz_prop_s",
          "name_CcgGFeiwORNbAw_prop_s",
          "name_mVgxCpHfjhofqaOA_prop_s",
          "name_nzuAietKmfmjnXalz_prop_s",
          "name_YzYAGdOoaxwgSh_prop_s",
          "name_jLMshjzscpgU_prop_s",
          "name_JaLKPfULNIeWysimJf_prop_s",
          "name_KehwwGmAULqXtNCrwhX_prop_s",
          "name_mxpwZrDktTLXUzkdKa_prop_s",
          "name_bPmedbyCSSjDC_prop_s",
          "name_LYbCFtmQiC_prop_s",
          "name_cLrVLzwMcMnAT_prop_s",
          "name_HeOUpecBxVvHEERlPUk_prop_s",
          "name_jCVSgiNewmDB_prop_s",
          "name_jOLtAVRUFrs_prop_s",
          "name_gfWTsWEVeVSXwGMgUT_prop_s",
          "name_BPbiEWmyizADxNIV_prop_s",
          "name_VYSwGIOIarPmGWVKenS_prop_s",
          "name_QpAHpTcxrzVYYfWYT_prop_s",
          "name_tOBVdVTRBMmCXfnxrNa_prop_s",
          "name_iCaKxgfTXuvgCT_prop_s",
          "name_kdPWzVZHslaijNrKbKU_prop_s",
          "name_wmJmiiWghggUHmNiQAg_prop_s",
          "name_ZpzaQMGuMfOjw_prop_s",
          "name_cgOqMOeYMHJ_prop_s",
          "name_EnguvcJhre_prop_s",
          "name_edeevGMabTDek_prop_s",
          "name_vmJEHidWgTTUvioGhi_prop_s",
          "name_CHYfwnIHxQzPwEFJ_prop_s",
          "name_KXpUaenwfjlj_prop_s",
          "name_eVGHumUijQhFvzGjaV_prop_s",
          "name_XorPzBArSbSTCHpz_prop_s",
          "name_RRLESavujqcxblljkn_prop_s",
          "name_YftgbzYxUNUCMXt_prop_s",
          "name_IqEDQHVFGIyQSS_prop_s",
          "name_XbStVPkHwGYmQB_prop_s",
          "name_JyWCZhERjLOtqw_prop_s",
          "name_dDiuFMzjhrJGyqqud_prop_s",
          "name_uCCDpPtxkdQNDq_prop_s",
          "name_ohZQKMOVeb_prop_s",
          "name_gBTzxrPwsX_prop_s",
          "name_RLkUwPFSVjqB_prop_s",
          "name_CXlPWeBunQDGtBXqo_prop_s",
          "name_kGvCPheDzjir_prop_s",
          "name_cvcOAZkaTZsTyrrWxvQ_prop_s",
          "name_sftNHXiElgbUQxtYDI_prop_s",
          "name_aqGEmvTBCqdFKyfa_prop_s",
          "name_myCtzywMPzQyJhHwsEy_prop_s",
          "name_TmvkTzpWLtPEDUfmg_prop_s",
          "name_XnYvWLQJdcjdOBmfJ_prop_s",
          "name_toYeyORNQWA_prop_s",
          "name_hcWpqATuIiUbyfiHPaJ_prop_s",
          "name_EAelPZjFpiThB_prop_s",
          "name_aEfokIQMbKI_prop_s",
          "name_YMbTCeRRipELjF_prop_s",
          "name_yIbPmIvnUNFsKaEk_prop_s",
          "name_PVsusfJldMrTq_prop_s",
          "name_BhGnYInbCoBcRxbkh_prop_s",
          "name_LvywGWGeDmCnwYM_prop_s",
          "name_bJwdGFMfTyRhI_prop_s",
          "name_durkyUrNKHx_prop_s",
          "name_RdeZaAlmttQzNDZCb_prop_s",
          "name_VdzHkraZKezBjY_prop_s",
          "name_rAhOeyHbDuW_prop_s",
          "name_SNzylGssYOA_prop_s",
          "name_vHqZyqgwfD_prop_s",
          "name_DPnKKQlfkn_prop_s",
          "name_PQFtvTrPezVRLL_prop_s",
          "name_YkOCraZfkuCyx_prop_s",
          "name_glGgplQXQzqaHbT_prop_s",
          "name_OqpvyNHqeQUANE_prop_s",
          "name_EYRKsQekVHcYlWf_prop_s",
          "name_RFuZbCWIOu_prop_s",
          "name_ekHWLiTVyNjYdl_prop_s",
          "name_vezpACcbFw_prop_s",
          "name_oQQXcPzeODviDC_prop_s",
          "name_wZkyzXscqPGWiEzwR_prop_s",
          "name_eYywOQdxMbAwHNC_prop_s",
          "name_gvEXKFXEAQMaYm_prop_s",
          "name_vofoikKFpZsOZfY_prop_s",
          "name_aXNocadbQQO_prop_s",
          "name_pzzPuuliByDjLm_prop_s",
          "name_dIOSQFOVldP_prop_s",
          "name_sbplpizxCQWndsBpoU_prop_s",
          "name_uogQaerZVBnV_prop_s",
          "name_WsDhwfdJivmMKO_prop_s",
          "name_RjJjIrPGWGFgCbT_prop_s",
          "name_sKymsAbmFqwyzKRSH_prop_s",
          "name_wIHDafXfvOunVi_prop_s",
          "name_pWEWMRdqgvuGdqwztct_prop_s",
          "name_aFDHZXHKgnVo_prop_s",
          "name_dAdcQYTvmRZ_prop_s",
          "name_zQsaOcogPYNqypDPYjS_prop_s",
          "name_KOtJNECCHjLxKZqHZ_prop_s",
          "name_wfdxykXSBRcrfUv_prop_s",
          "name_kGJgFephxkeH_prop_s",
          "name_peispafiMLgmE_prop_s",
          "name_CJTnCuCsOSCvj_prop_s",
          "name_xpOyokirtcJoFPKyH_prop_s",
          "name_nhmhQePxBvNT_prop_s",
          "name_vPxdJTwHkzDdvaK_prop_s",
          "name_dAGyfZWSkTaCCt_prop_s",
          "name_CYaZJGFolJqNhmKgsV_prop_s",
          "name_vboqCHtthOPMRHU_prop_s",
          "name_fqrgYweKbBNzlYJk_prop_s",
          "name_SwOSQemwasu_prop_s",
          "name_dRDJlPUxSgvIS_prop_s",
          "name_DYjfbnkMhnMyL_prop_s",
          "name_REAirSXdUlsq_prop_s",
          "name_aPLpQwhWGCcjk_prop_s",
          "name_LWlbDafEriuRGmJYW_prop_s",
          "name_bTFLYGqAHYvnpFvzd_prop_s",
          "name_emIonaQRdfsjmVCjUn_prop_s",
          "name_RdMOfMWlqKmKuxYawG_prop_s",
          "name_NmvxkGBDyJ_prop_s",
          "name_veeKFlgaBqTXINdlbi_prop_s",
          "name_JEMSCgBWKwpd_prop_s",
          "name_RKsEwiClkYAENVkO_prop_s",
          "name_QSfmaqphip_prop_s",
          "name_DhcOPbKnWrv_prop_s",
          "name_AhEfQCMTWtrdjBV_prop_s",
          "name_EtAMDtJVTd_prop_s",
          "name_qVxNUttsduupj_prop_s",
          "name_BeFWYHBfnSNqVEPz_prop_s",
          "name_wPevXszAQZWZwe_prop_s",
          "name_oJYcnxrshAkjJYnXyn_prop_s",
          "name_nffSJxMrhrIlQw_prop_s",
          "name_ZrHpSfuzHHIin_prop_s",
          "name_rdMnHMmgEQaGLmXRPiD_prop_s",
          "name_huldbnqnXwop_prop_s",
          "name_jduhQpDoYv_prop_s",
          "name_NBOKEducirzNsSSy_prop_s",
          "name_xJzWfJrMIY_prop_s",
          "name_VMZbxqOHwfQDaGT_prop_s",
          "name_syUXJprVoLTZYebB_prop_s",
          "name_prZEUbNoTysB_prop_s",
          "name_RfvJRIoQeGSu_prop_s",
          "name_BBshBWkaLopZ_prop_s",
          "name_YOAVKRdkRspIVaLva_prop_s",
          "name_RrTctdPJnMoMw_prop_s",
          "name_TEoYvqSeBmaUHflB_prop_s",
          "name_IwsxROIVgJ_prop_s",
          "name_ktQwKjuCLYAmOnyj_prop_s",
          "name_MZrmJkYFkHsU_prop_s",
          "name_bdagQHBFmoIo_prop_s",
          "name_zmoxFeHMBwkyEO_prop_s",
          "name_wenNdlQvlHItqflx_prop_s",
          "name_XprqFpXiYoHzEfd_prop_s",
          "name_ogZQmtfQOfvP_prop_s",
          "name_QOsBJGNDUzbHWHrQ_prop_s",
          "name_jfrfWIuCWSFXQumtm_prop_s",
          "name_VFWIKhommZaTVuzphSb_prop_s",
          "name_RVhGwEvGjdOnzR_prop_s",
          "name_FQlxoQLZIkZCyfiVx_prop_s",
          "name_MPbQJjgBGMUR_prop_s",
          "name_SbbTGVASSkYHiNwV_prop_s",
          "name_MntYiMNrHQ_prop_s",
          "name_yjcZRVwITRLXb_prop_s",
          "name_aSKYqqhexuo_prop_s",
          "name_TfzoLKDlIhDun_prop_s",
          "name_KeKTrXfMFglbN_prop_s",
          "name_iIdfUsKoIlf_prop_s",
          "name_FPQqtNlVCLSgwgNhf_prop_s",
          "name_PkYUzUADmq_prop_s",
          "name_nXAJwIhWfESKdZ_prop_s",
          "name_faXLvuLCiq_prop_s",
          "name_zarHYCyYIr_prop_s",
          "name_sowzONSDytjGEZuv_prop_s",
          "name_zyWCVstnSnLz_prop_s",
          "name_anncXfqvveOWy_prop_s",
          "name_TbvIhvzhkLAXm_prop_s",
          "name_tBWzDGmZocLjPRFMIF_prop_s",
          "name_JgCrqPcPNiVdrRRbf_prop_s",
          "name_FBtKmopbwHOPPoMjDRA_prop_s",
          "name_BOEyOhYKOUSQFQPxwDL_prop_s",
          "name_uVosPVYbIF_prop_s",
          "name_eQOiKlnUNZ_prop_s",
          "name_lYYQBjpaIjMXYRH_prop_s",
          "name_FyFvEcZfRrnx_prop_s",
          "name_rNiSOAGXkMPBY_prop_s",
          "name_tylcSBADvLvAKkzv_prop_s",
          "name_KvoxbuKdiqLGUo_prop_s",
          "name_FDZfmbIjXBiKoeWImxj_prop_s",
          "name_NULbsIjjyysWdXGAxy_prop_s",
          "name_RVtYeHUXaxVSBJUCX_prop_s",
          "name_jlNFgVZgDAFKqHxR_prop_s",
          "name_uIhSJwItLLKHa_prop_s",
          "name_lEMFtKhGZjrjnLlW_prop_s",
          "name_avEoREwfXmm_prop_s",
          "name_IiXRqkZmvNAqf_prop_s",
          "name_dKzqqsjZzTgxHTpZiA_prop_s",
          "name_jilMmwVsaTkUgJ_prop_s",
          "name_xYNTFgaEEluQ_prop_s",
          "name_WFkNIiGzzfHous_prop_s",
          "name_ztXfmQXTTNuXjPSCYC_prop_s",
          "name_jyGvFWOSfs_prop_s",
          "name_jRpEJIPQzYKLR_prop_s",
          "name_FIUqxuPiWpMMTuZ_prop_s",
          "name_ttkqBQpFtwHL_prop_s",
          "name_bqYmgceeoJZSZbW_prop_s",
          "name_ctRkHATHrFlnEKmSRLd_prop_s",
          "name_wZorXwBeanELgv_prop_s",
          "name_jXiyBDjpCKe_prop_s",
          "name_sRvLkwUSBIsrt_prop_s",
          "name_yEHNabvaqyAGa_prop_s",
          "name_cwmgaKpzluwJOBvphxY_prop_s",
          "name_cOXSTpgjzFEjfbJPVM_prop_s",
          "name_ikkFRyBgGfWbg_prop_s",
          "name_dEKLFEgvjHFo_prop_s",
          "name_HJZRtrGjmPlc_prop_s",
          "name_hMpazPhQVkTUE_prop_s",
          "name_VKnOJLBqMVzkxD_prop_s",
          "name_zKPBHVcuULlMTRy_prop_s",
          "name_LzbMOhdcPnvcF_prop_s",
          "name_euHYSgnsustyR_prop_s",
          "name_IvuYSeiYicgpmboJW_prop_s",
          "name_yGrlGoiNHNIt_prop_s",
          "name_tpDceZWQvat_prop_s",
          "name_iaDXoHUSwG_prop_s",
          "name_fJXmNNxUHggajGl_prop_s",
          "name_qdzxqokVXHjNBORhW_prop_s",
          "name_DxoLvhVEbDcXb_prop_s",
          "name_bFHhHakPJd_prop_s",
          "name_hVrFxShinIeN_prop_s",
          "name_XKPhskHDDg_prop_s",
          "name_JjbLlVDrWA_prop_s",
          "name_xOJcUebWcopYLGKGYhH_prop_s",
          "name_VJTvLToaSyFUm_prop_s",
          "name_civISGYkrfwD_prop_s",
          "name_kSPizRJqJZ_prop_s",
          "name_gmmUBdiHNFVBzpqukdi_prop_s",
          "name_jSGXVJsJPmESYy_prop_s",
          "name_AbyytYHuJyn_prop_s",
          "name_YGNtCMfmLqE_prop_s",
          "name_siCxrMEiFjwoEqfcc_prop_s",
          "name_yWlyMAenZiTylpYzW_prop_s",
          "name_XWOZYkmhzHmOF_prop_s",
          "name_FlCjaUETSllVHEwmoR_prop_s",
          "name_ZaXOAZXrKGXs_prop_s",
          "name_wveujGHeUQ_prop_s",
          "name_KhSPQFkCmHuScj_prop_s",
          "name_cBXYezKthhDfoVOnIo_prop_s",
          "name_rOVAKNTsPprlUDDlCa_prop_s",
          "name_fgWaLCfjuDnbH_prop_s",
          "name_ekxAMazIGJgLCCMox_prop_s",
          "name_iCbNCfPSYKZ_prop_s",
          "name_rULXErnmZoIMARdsEL_prop_s",
          "name_MjtGLUmEVFFRKydbJ_prop_s",
          "name_DzLQfXBPWppyPjj_prop_s",
          "name_xxNOkzscmZ_prop_s",
          "name_VAiCBAZUeEnA_prop_s",
          "name_ftdPuTtNtpLoRmtqQB_prop_s",
          "name_ebNmBmAGnjhDwEMkWN_prop_s",
          "name_eZVGYMBDaN_prop_s",
          "name_hxykcxgsIAfxfupix_prop_s",
          "name_XEDImtbSKXAeLyEop_prop_s",
          "name_yOxGFWeePpUIc_prop_s",
          "name_RzqLTLciLlaundr_prop_s",
          "name_UtCQadSTlNF_prop_s",
          "name_ORSaWMOVQhZZWxkv_prop_s",
          "name_qCgQYTeGGSJf_prop_s",
          "name_AlIZOvRFcZPbZwU_prop_s",
          "name_vdqdlYetlciyb_prop_s",
          "name_dmJIAXeXYjJhwacpkLZ_prop_s",
          "name_mCOjAATZrgxJ_prop_s",
          "name_RJsQfzfqbZGXp_prop_s",
          "name_XMiImCbTVJAoKSfEo_prop_s",
          "name_kDCCVcALrCx_prop_s",
          "name_VmkGYGugHqaA_prop_s",
          "name_jvZilzavGvyq_prop_s",
          "name_CCDtRrXmOTmc_prop_s",
          "name_UGbllGSvifotji_prop_s",
          "name_JOfVgyuwzbIriJg_prop_s",
          "name_cJCGLUbaZcrJXGCcZyE_prop_s",
          "name_yKXkqdoNhbkSPSBUv_prop_s",
          "name_QSrzBIUBQVUrdzM_prop_s",
          "name_ulgjGcvaqh_prop_s",
          "name_JaQtXbimGQW_prop_s",
          "name_xYrQHDXvVbzq_prop_s",
          "name_wSxZHthLVwKjuBWR_prop_s",
          "name_mEefJyzMBqdSbQ_prop_s",
          "name_GGJivsaoxiirx_prop_s",
          "name_CACALOHPQCrf_prop_s",
          "name_GBrQDvusDOWuvClhYa_prop_s",
          "name_vqZfUUBIkd_prop_s",
          "name_mXGYvfrccKBFymNB_prop_s",
          "name_wZhiLMSbHcweTy_prop_s",
          "name_fFPlXgVZKVHosY_prop_s",
          "name_wAFjlOGjIQJBOBgsg_prop_s",
          "name_diTnXDoUYaBiVnc_prop_s",
          "name_DyufnSBeLVPwDSPBi_prop_s",
          "name_TlGahXVDZeZdT_prop_s",
          "name_jDnUlzuoxtWKe_prop_s",
          "name_MCnYrKrvAa_prop_s",
          "name_HZtNEzgsgQpgPULw_prop_s",
          "name_sZZJIdHfiEnPvbgdoK_prop_s",
          "name_aehzQLgzPf_prop_s",
          "name_uOhtALYSlV_prop_s",
          "name_tLmeLHpBwzP_prop_s",
          "name_tMDTGUzelwUQrqPaN_prop_s",
          "name_wMUhdfLFRaXOOLeKL_prop_s",
          "name_XqpDZerjeDqrzzhsw_prop_s",
          "name_zrmxpkEbOGPIEzqM_prop_s",
          "name_dEOcHvQShe_prop_s",
          "name_QbZyQMReoJJG_prop_s",
          "name_gsYPhgkPfTOiPDEAVD_prop_s",
          "name_rbNswYkvBvqmA_prop_s",
          "name_LNmSBXRpRnvKFpqbT_prop_s",
          "name_rHuaoaqVkaAz_prop_s",
          "name_zjeWWuFoacJuuxETiD_prop_s",
          "name_BhAKoPqFSVlA_prop_s",
          "name_JjjcumppysyXsTldO_prop_s",
          "name_bMjIQaLeLZ_prop_s",
          "name_ujEVEGDoYpXmg_prop_s",
          "name_xeFvZrHvmONeM_prop_s",
          "name_vgNWlNzSOGo_prop_s",
          "name_AvdOuoFmghMsCklVua_prop_s",
          "name_KWDQpWtFvwxJWNNj_prop_s",
          "name_llVSmiZlLiNdippBgzm_prop_s",
          "name_BLmEHGblGXQHbywh_prop_s",
          "name_rfoqACJQVUHBuJZDjdx_prop_s",
          "name_kDQGbSJbyD_prop_s",
          "name_PLSTfOtIQx_prop_s",
          "name_NiIIwJLdfGlAcwzfT_prop_s",
          "name_kPGazwKspZmewuiaZVB_prop_s",
          "name_cuhoZBMdAMi_prop_s",
          "name_XbZqGnehJGT_prop_s",
          "name_yfCAGBvZufEM_prop_s",
          "name_GHuXtHZtwTY_prop_s",
          "name_jFtdTtkJbvHrsgGQ_prop_s",
          "name_iBcSefLjrrEyHOfqpx_prop_s",
          "name_GBJRIYHEbuwJmzLdxtm_prop_s",
          "name_VPiXQrwycQPT_prop_s",
          "name_XJstMKshDibmiHoZMd_prop_s",
          "name_wiGBycapxeIXtTrvW_prop_s",
          "name_rJPHaUEbgraQ_prop_s",
          "name_rGxylqGVHinLjO_prop_s",
          "name_GXeVgdEWBmv_prop_s",
          "name_HnYKYhHxZlpGIwdIVQ_prop_s",
          "name_FIOSdBvncmSeMiH_prop_s",
          "name_FCOLTVOghkVRBXhh_prop_s",
          "name_iZknWYaTKn_prop_s",
          "name_bQhwLkthwP_prop_s",
          "name_GJKLUOxgFtxMdbpeN_prop_s",
          "name_uCUdhLIXQKheDpQMB_prop_s",
          "name_knArOLgcybDsJsor_prop_s",
          "name_vgoNwqvzshUKeOPUSYk_prop_s",
          "name_YzIaNlWjqBqwoJcA_prop_s",
          "name_hDYFmiHwhPCL_prop_s",
          "name_fEAcVIqAfAIXehyOoGU_prop_s",
          "name_KwUSxCHFWiXOTqk_prop_s",
          "name_KRUSuEYGaQgWJmnGm_prop_s",
          "name_PpWwLjvaGoR_prop_s",
          "name_skVILQlxWYQowRGw_prop_s",
          "name_bcbBLimvTIGQp_prop_s",
          "name_vYQrLudbiua_prop_s",
          "name_nuDloTTlKFpeoV_prop_s",
          "name_RhbixfcpVSMOPfK_prop_s",
          "name_fRRDlXHyOAGhwJ_prop_s",
          "name_PGTPucoCVbz_prop_s",
          "name_TTOIQLLAUIMUqE_prop_s",
          "name_kXJQwDYAdc_prop_s",
          "name_VlYMFsIAfv_prop_s",
          "name_OThsmraSBTydoPfu_prop_s",
          "name_WhEccUbWgvObJoS_prop_s",
          "name_bxJtNPHBleHNhfat_prop_s",
          "name_aLJcfxHporPCXBiF_prop_s",
          "name_BbBwSzFKovNubMsv_prop_s",
          "name_ZoaCLmepYLkTCLddGPn_prop_s",
          "name_jYflHPNvrnzB_prop_s",
          "name_SGqftBmurcbCEMn_prop_s",
          "name_PqiMioFAtKOjkan_prop_s",
          "name_ZeazKbMtVMB_prop_s",
          "name_sgQyAUHsEg_prop_s",
          "name_EAIUmQCWbiQbZI_prop_s",
          "name_FNcVUavfHz_prop_s",
          "name_ViUmtAvjlwKCeFb_prop_s",
          "name_FYjubApKwXxQnNUIxB_prop_s",
          "name_WLPEmGTQAisfXsq_prop_s",
          "name_CyrnsHyuyFBx_prop_s",
          "name_zMGfDpWzqfZMAF_prop_s",
          "name_NILxzDPIbmoxOwQtuQP_prop_s",
          "name_JJCEpGqGVjJa_prop_s",
          "name_CtTFvRpyzKguMdZ_prop_s",
          "name_qiGhKGSMzMMp_prop_s",
          "name_QLUJBWXryHb_prop_s",
          "name_sMJePABydcVoQk_prop_s",
          "name_tfpbMNRLaXuyLuexLGy_prop_s",
          "name_rYoMoMLacxWlS_prop_s",
          "name_vWDCkyzmEi_prop_s",
          "name_RkKjeQtYycWC_prop_s",
          "name_xfDfirUchdkxKIDJOt_prop_s",
          "name_mEWCBmdvyhON_prop_s",
          "name_uLtsxsjXOGQZkCChL_prop_s",
          "name_UYjWVNCvGE_prop_s",
          "name_JJxhmSNcmsN_prop_s",
          "name_fYqlzMmhQdoecsvx_prop_s",
          "name_MxXoomSYegfmoEy_prop_s",
          "name_hKITNVMXrrjaeFpwfh_prop_s",
          "name_bhTKjWsdWDdonwi_prop_s",
          "name_XWjLvIfzoorQRqBmo_prop_s",
          "name_UqLAinOoswSeBVh_prop_s",
          "name_mQzjXAidhWpqqG_prop_s",
          "name_ytxaqwLBrvJYolqi_prop_s",
          "name_daTgAYVVJQsmO_prop_s",
          "name_xCmENbUDoiZ_prop_s",
          "name_eZTpxzkHHLjKUGuV_prop_s",
          "name_XdJsjHWRNMnQeC_prop_s",
          "name_tTSOfpdJTOsZkcTH_prop_s",
          "name_ridXaoCaPNoyFx_prop_s",
          "name_HVIFmePdpnAcvjba_prop_s",
          "name_osQVkiJtkHiBVP_prop_s",
          "name_ikTrXQFmMpAw_prop_s",
          "name_CtPYdlsrBtsuRkU_prop_s",
          "name_BbnERLXULZsX_prop_s",
          "name_FUGsEWgJtiLxWUEadSE_prop_s",
          "name_babUPIRWxOJTyQqt_prop_s",
          "name_zqaORMkAJlhSf_prop_s",
          "name_CeRKgIekQl_prop_s",
          "name_sHuCaTJIqfPYqpDILZe_prop_s",
          "name_wMsJtSzGDCJ_prop_s",
          "name_NprXcFInRsRGK_prop_s",
          "name_kruVqZBPAizaB_prop_s",
          "name_OJaYkRoxWwARAGa_prop_s",
          "name_fQeYEMbbBnnmbwS_prop_s",
          "name_jHwrTEPSNe_prop_s",
          "name_tGtgZLRbdYQHqFyI_prop_s",
          "name_bYUODaraQABQMuiVwa_prop_s",
          "name_LsdkDDyTgtLnQv_prop_s",
          "name_WmBvbHCQqNznHXDM_prop_s",
          "name_yCpJZfnNvJt_prop_s",
          "name_nxEaZdhiNOaCgHXu_prop_s",
          "name_YlsRbOaHrwrjw_prop_s",
          "name_wEzIAxJlGY_prop_s",
          "name_wgtEQdJDFUZMRCtKuvN_prop_s",
          "name_NlrMACPMAY_prop_s",
          "name_lyJIPhQYMXgUIOe_prop_s",
          "name_XDMUiHILIfVcRVS_prop_s",
          "name_CReyJWfRLOR_prop_s",
          "name_AySGHgndHRfNrHYs_prop_s",
          "name_vMKLAoTfxxBNIVC_prop_s",
          "name_UiEpdEsyrJWBVZN_prop_s",
          "name_ZDESHNBkigMNhIdqjqB_prop_s",
          "name_MeDLRbvcZrLgrXD_prop_s",
          "name_wtkpdHkreDpFK_prop_s",
          "name_fdKDEadJGWkIhpT_prop_s",
          "name_ozeAMJPgTwwzrTmu_prop_s",
          "name_CNivtYVLtjVlr_prop_s",
          "name_yglTIePAOb_prop_s",
          "name_UTRKTVkvhpJKEE_prop_s",
          "name_OmHylNTQXDRUKEC_prop_s",
          "name_JiZnnChtUFMUrGi_prop_s",
          "name_WoCxWZkHoaQu_prop_s",
          "name_AnNVbPPNuzjqFnL_prop_s",
          "name_kLXLsnBnOoySgS_prop_s",
          "name_UhbzdIMuOFGDaNiXEv_prop_s",
          "name_eWOWltJaJILIzCH_prop_s",
          "name_AIMKZYfLAHIs_prop_s",
          "name_pDzYoeEDPjsvqJ_prop_s",
          "name_eOACrLtTfxoyRlU_prop_s",
          "name_WauBOgBeapqDugJyyp_prop_s",
          "name_uwzXxeCxlcsKrNwpPkm_prop_s",
          "name_zZYjhAOxmRWjICXyd_prop_s",
          "name_jyeCWKaQnlrYHkzwSH_prop_s",
          "name_SesSMUttyVjUJaGKX_prop_s",
          "name_HBOChmtthCl_prop_s",
          "name_CxlLbdpOOfXwL_prop_s",
          "name_MiFBPgcnSSYFJdyju_prop_s",
          "name_rKEAVEpJXKWbRYM_prop_s",
          "name_xLQKEwIRCsGTqWzRf_prop_s"
      });
}
