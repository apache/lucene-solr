package org.apache.solr.spelling;
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.SpellCheckComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class SpellCheckCollatorTest extends SolrTestCaseJ4 {
	@BeforeClass
	public static void beforeClass() throws Exception {
		initCore("solrconfig.xml", "schema.xml");
		assertNull(h.validateUpdate(adoc("id", "0", "lowerfilt", "faith hope and love")));
		assertNull(h.validateUpdate(adoc("id", "1", "lowerfilt", "faith hope and loaves")));
		assertNull(h.validateUpdate(adoc("id", "2", "lowerfilt", "fat hops and loaves")));
		assertNull(h.validateUpdate(adoc("id", "3", "lowerfilt", "faith of homer")));
		assertNull(h.validateUpdate(adoc("id", "4", "lowerfilt", "fat of homer")));
		assertNull(h.validateUpdate(adoc("id", "5", "lowerfilt1", "peace")));
		assertNull(h.validateUpdate(adoc("id", "6", "lowerfilt", "hyphenated word")));
		assertNull(h.validateUpdate(commit()));
	}

	@Test
	public void testCollationWithHypens() throws Exception
	{
	  SolrCore core = h.getCore();
    SearchComponent speller = core.getSearchComponent("spellcheck");
    assertTrue("speller is null and it shouldn't be", speller != null);
    
    ModifiableSolrParams params = new ModifiableSolrParams();   
    params.add(SpellCheckComponent.COMPONENT_NAME, "true");
    params.add(SpellCheckComponent.SPELLCHECK_BUILD, "true");
    params.add(SpellCheckComponent.SPELLCHECK_COUNT, "10");   
    params.add(SpellCheckComponent.SPELLCHECK_COLLATE, "true");
    
    params.add(CommonParams.Q, "lowerfilt:(hypenated-wotd)");
    {
      SolrRequestHandler handler = core.getRequestHandler("spellCheckCompRH");
      SolrQueryResponse rsp = new SolrQueryResponse();
      rsp.add("responseHeader", new SimpleOrderedMap());
      SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
      handler.handleRequest(req, rsp);
      req.close();
      NamedList values = rsp.getValues();
      NamedList spellCheck = (NamedList) values.get("spellcheck");
      NamedList suggestions = (NamedList) spellCheck.get("suggestions");
      List<String> collations = suggestions.getAll("collation");
      assertTrue(collations.size()==1); 
      String collation = collations.iterator().next();      
      assertTrue("Incorrect collation: " + collation,"lowerfilt:(hyphenated-word)".equals(collation));
    }

    params.remove(CommonParams.Q);
    params.add("defType", "dismax");
    params.add("qf", "lowerfilt");
    params.add(CommonParams.Q, "hypenated-wotd");
    {
      SolrRequestHandler handler = core.getRequestHandler("spellCheckCompRH");
      SolrQueryResponse rsp = new SolrQueryResponse();
      rsp.add("responseHeader", new SimpleOrderedMap());
      SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
      handler.handleRequest(req, rsp);
      req.close();
      NamedList values = rsp.getValues();
      NamedList spellCheck = (NamedList) values.get("spellcheck");
      NamedList suggestions = (NamedList) spellCheck.get("suggestions");
      List<String> collations = suggestions.getAll("collation");
      assertTrue(collations.size()==1);
      String collation = collations.iterator().next();
      assertTrue("Incorrect collation: " + collation,"hyphenated-word".equals(collation));
    }

  }

	@Test
	public void testCollateWithFilter() throws Exception
	{
		SolrCore core = h.getCore();
		SearchComponent speller = core.getSearchComponent("spellcheck");
		assertTrue("speller is null and it shouldn't be", speller != null);
		
		ModifiableSolrParams params = new ModifiableSolrParams();		
		params.add(SpellCheckComponent.COMPONENT_NAME, "true");
		params.add(SpellCheckComponent.SPELLCHECK_BUILD, "true");
		params.add(SpellCheckComponent.SPELLCHECK_COUNT, "10");		
		params.add(SpellCheckComponent.SPELLCHECK_COLLATE, "true");
		params.add(SpellCheckComponent.SPELLCHECK_MAX_COLLATION_TRIES, "10");
		params.add(SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "10");
		params.add(CommonParams.Q, "lowerfilt:(+fauth +home +loane)");
		params.add(CommonParams.FQ, "NOT(id:1)");
		
		//Because a FilterQuery is applied which removes doc id#1 from possible hits, we would
		//not want the collations to return us "lowerfilt:(+faith +hope +loaves)" as this only matches doc id#1.
		SolrRequestHandler handler = core.getRequestHandler("spellCheckCompRH");
		SolrQueryResponse rsp = new SolrQueryResponse();
		rsp.add("responseHeader", new SimpleOrderedMap());
		SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
		handler.handleRequest(req, rsp);
		req.close();
		NamedList values = rsp.getValues();
		NamedList spellCheck = (NamedList) values.get("spellcheck");
		NamedList suggestions = (NamedList) spellCheck.get("suggestions");
		List<String> collations = suggestions.getAll("collation");
		assertTrue(collations.size() > 0);
		for(String collation : collations) {
			assertTrue(!collation.equals("lowerfilt:(+faith +hope +loaves)"));	
		}
	}
	
	@Test
	public void testCollateWithMultipleRequestHandlers() throws Exception
	{
		SolrCore core = h.getCore();
		SearchComponent speller = core.getSearchComponent("spellcheck");
		assertTrue("speller is null and it shouldn't be", speller != null);
		
		ModifiableSolrParams params = new ModifiableSolrParams();		
		params.add(SpellCheckComponent.COMPONENT_NAME, "true");
		params.add(SpellCheckComponent.SPELLCHECK_DICT, "multipleFields");
		params.add(SpellCheckComponent.SPELLCHECK_BUILD, "true");
		params.add(SpellCheckComponent.SPELLCHECK_COUNT, "10");		
		params.add(SpellCheckComponent.SPELLCHECK_COLLATE, "true");
		params.add(SpellCheckComponent.SPELLCHECK_MAX_COLLATION_TRIES, "1");
		params.add(SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "1");
		params.add(CommonParams.Q, "peac");	
		
		//SpellCheckCompRH has no "qf" defined.  It will not find "peace" from "peac" despite it being in the dictionary
		//because requrying against this Request Handler results in 0 hits.	
		SolrRequestHandler handler = core.getRequestHandler("spellCheckCompRH");
		SolrQueryResponse rsp = new SolrQueryResponse();
		rsp.add("responseHeader", new SimpleOrderedMap());
		SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
		handler.handleRequest(req, rsp);
		req.close();
		NamedList values = rsp.getValues();
		NamedList spellCheck = (NamedList) values.get("spellcheck");
		NamedList suggestions = (NamedList) spellCheck.get("suggestions");
		String singleCollation = (String) suggestions.get("collation");
		assertNull(singleCollation);
		
		//SpellCheckCompRH1 has "lowerfilt1" defined in the "qf" param.  It will find "peace" from "peac" because
		//requrying field "lowerfilt1" returns the hit.
		params.remove(SpellCheckComponent.SPELLCHECK_BUILD);
		handler = core.getRequestHandler("spellCheckCompRH1");
		rsp = new SolrQueryResponse();
		rsp.add("responseHeader", new SimpleOrderedMap());
		req = new LocalSolrQueryRequest(core, params);
		handler.handleRequest(req, rsp);
		req.close();
		values = rsp.getValues();
		spellCheck = (NamedList) values.get("spellcheck");
		suggestions = (NamedList) spellCheck.get("suggestions");
		singleCollation = (String) suggestions.get("collation");
		assertEquals(singleCollation, "peace");		
	}

	@Test
	public void testExtendedCollate() throws Exception {
		SolrCore core = h.getCore();
		SearchComponent speller = core.getSearchComponent("spellcheck");
		assertTrue("speller is null and it shouldn't be", speller != null);

		ModifiableSolrParams params = new ModifiableSolrParams();
		params.add(CommonParams.QT, "spellCheckCompRH");
		params.add(CommonParams.Q, "lowerfilt:(+fauth +home +loane)");
		params.add(SpellCheckComponent.SPELLCHECK_EXTENDED_RESULTS, "true");
		params.add(SpellCheckComponent.COMPONENT_NAME, "true");
		params.add(SpellCheckComponent.SPELLCHECK_BUILD, "true");
		params.add(SpellCheckComponent.SPELLCHECK_COUNT, "10");
		params.add(SpellCheckComponent.SPELLCHECK_COLLATE, "true");

		// Testing backwards-compatible behavior.
		// Returns 1 collation as a single string.
		// All words are "correct" per the dictionary, but this collation would
		// return no results if tried.
		SolrRequestHandler handler = core.getRequestHandler("spellCheckCompRH");
		SolrQueryResponse rsp = new SolrQueryResponse();
		rsp.add("responseHeader", new SimpleOrderedMap());
		SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
		handler.handleRequest(req, rsp);
		req.close();
		NamedList values = rsp.getValues();
		NamedList spellCheck = (NamedList) values.get("spellcheck");
		NamedList suggestions = (NamedList) spellCheck.get("suggestions");
		String singleCollation = (String) suggestions.get("collation");
		assertEquals("lowerfilt:(+faith +homer +loaves)", singleCollation);

		// Testing backwards-compatible response format but will only return a
		// collation that would return results.
		params.remove(SpellCheckComponent.SPELLCHECK_BUILD);
		params.add(SpellCheckComponent.SPELLCHECK_MAX_COLLATION_TRIES, "5");
		params.add(SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "1");
		handler = core.getRequestHandler("spellCheckCompRH");
		rsp = new SolrQueryResponse();
		rsp.add("responseHeader", new SimpleOrderedMap());
		req = new LocalSolrQueryRequest(core, params);
		handler.handleRequest(req, rsp);
    req.close();
		values = rsp.getValues();
		spellCheck = (NamedList) values.get("spellcheck");
		suggestions = (NamedList) spellCheck.get("suggestions");
		singleCollation = (String) suggestions.get("collation");
		assertEquals("lowerfilt:(+faith +hope +loaves)", singleCollation);

		// Testing returning multiple collations if more than one valid
		// combination exists.
		params.remove(SpellCheckComponent.SPELLCHECK_MAX_COLLATION_TRIES);
		params.remove(SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS);
		params.add(SpellCheckComponent.SPELLCHECK_MAX_COLLATION_TRIES, "10");
		params.add(SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "2");
		handler = core.getRequestHandler("spellCheckCompRH");
		rsp = new SolrQueryResponse();
		rsp.add("responseHeader", new SimpleOrderedMap());
		req = new LocalSolrQueryRequest(core, params);
		handler.handleRequest(req, rsp);
		req.close();
		values = rsp.getValues();
		spellCheck = (NamedList) values.get("spellcheck");
		suggestions = (NamedList) spellCheck.get("suggestions");
		List<String> collations = suggestions.getAll("collation");
		assertTrue(collations.size() == 2);
		for (String multipleCollation : collations) {
			assertTrue(multipleCollation.equals("lowerfilt:(+faith +hope +love)")
					|| multipleCollation.equals("lowerfilt:(+faith +hope +loaves)"));
		}

		// Testing return multiple collations with expanded collation response
		// format.
		params.add(SpellCheckComponent.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true");
		handler = core.getRequestHandler("spellCheckCompRH");
		rsp = new SolrQueryResponse();
		rsp.add("responseHeader", new SimpleOrderedMap());
		req = new LocalSolrQueryRequest(core, params);
		handler.handleRequest(req, rsp);
		req.close();
		values = rsp.getValues();
		spellCheck = (NamedList) values.get("spellcheck");
		suggestions = (NamedList) spellCheck.get("suggestions");
		List<NamedList> expandedCollationList = suggestions.getAll("collation");
		Set<String> usedcollations = new HashSet<String>();
		assertTrue(expandedCollationList.size() == 2);
		for (NamedList expandedCollation : expandedCollationList) {
			String multipleCollation = (String) expandedCollation.get("collationQuery");
			assertTrue(multipleCollation.equals("lowerfilt:(+faith +hope +love)")
					|| multipleCollation.equals("lowerfilt:(+faith +hope +loaves)"));
			assertTrue(!usedcollations.contains(multipleCollation));
			usedcollations.add(multipleCollation);

			int hits = (Integer) expandedCollation.get("hits");
			assertTrue(hits == 1);

			NamedList misspellingsAndCorrections = (NamedList) expandedCollation.get("misspellingsAndCorrections");
			assertTrue(misspellingsAndCorrections.size() == 3);

			String correctionForFauth = (String) misspellingsAndCorrections.get("fauth");
			String correctionForHome = (String) misspellingsAndCorrections.get("home");
			String correctionForLoane = (String) misspellingsAndCorrections.get("loane");
			assertTrue(correctionForFauth.equals("faith"));
			assertTrue(correctionForHome.equals("hope"));
			assertTrue(correctionForLoane.equals("love") || correctionForLoane.equals("loaves"));
		}
	}
	
	@Test
	public void testCollateWithGrouping() throws Exception
	{
		SolrCore core = h.getCore();
		SearchComponent speller = core.getSearchComponent("spellcheck");
		assertTrue("speller is null and it shouldn't be", speller != null);
		
		ModifiableSolrParams params = new ModifiableSolrParams();		
		params.add(SpellCheckComponent.COMPONENT_NAME, "true");
		params.add(SpellCheckComponent.SPELLCHECK_BUILD, "true");
		params.add(SpellCheckComponent.SPELLCHECK_COUNT, "10");		
		params.add(SpellCheckComponent.SPELLCHECK_COLLATE, "true");
		params.add(SpellCheckComponent.SPELLCHECK_MAX_COLLATION_TRIES, "5");
		params.add(SpellCheckComponent.SPELLCHECK_MAX_COLLATIONS, "1");
		params.add(CommonParams.Q, "lowerfilt:(+fauth)");
		params.add(GroupParams.GROUP, "true");
		params.add(GroupParams.GROUP_FIELD, "id");
		
		//Because a FilterQuery is applied which removes doc id#1 from possible hits, we would
		//not want the collations to return us "lowerfilt:(+faith +hope +loaves)" as this only matches doc id#1.
		SolrRequestHandler handler = core.getRequestHandler("spellCheckCompRH");
		SolrQueryResponse rsp = new SolrQueryResponse();
		rsp.add("responseHeader", new SimpleOrderedMap());
		SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
		handler.handleRequest(req, rsp);
		req.close();
		NamedList values = rsp.getValues();
		NamedList spellCheck = (NamedList) values.get("spellcheck");
		NamedList suggestions = (NamedList) spellCheck.get("suggestions");
		List<String> collations = suggestions.getAll("collation");
		assertTrue(collations.size() == 1);
	}
}
