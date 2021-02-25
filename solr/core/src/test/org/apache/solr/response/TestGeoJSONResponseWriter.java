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
package org.apache.solr.response;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.io.SupportedFormats;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestGeoJSONResponseWriter extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final ObjectMapper jsonmapper = new ObjectMapper();
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-spatial.xml");
    createIndex();
  }

  public static void createIndex() {


//    <field name="srpt_geohash" type="srpt_geohash" multiValued="true" />
//    <field name="" type="srpt_quad" multiValued="true" />
//    <field name="" type="srpt_packedquad" multiValued="true" />
//    <field name="" type="stqpt_geohash" multiValued="true" />
    
    // multiple valued field
    assertU(adoc("id","H.A", "srpt_geohash","POINT( 1 2 )"));
    assertU(adoc("id","H.B", "srpt_geohash","POINT( 1 2 )", 
                             "srpt_geohash","POINT( 3 4 )"));
    assertU(adoc("id","H.C", "srpt_geohash","LINESTRING (30 10, 10 30, 40 40)"));

    assertU(adoc("id","Q.A", "srpt_quad","POINT( 1 2 )"));
    assertU(adoc("id","Q.B", "srpt_quad","POINT( 1 2 )", 
                             "srpt_quad","POINT( 3 4 )"));
    assertU(adoc("id","Q.C", "srpt_quad","LINESTRING (30 10, 10 30, 40 40)"));

    assertU(adoc("id","P.A", "srpt_packedquad","POINT( 1 2 )"));
    assertU(adoc("id","P.B", "srpt_packedquad","POINT( 1 2 )", 
                             "srpt_packedquad","POINT( 3 4 )"));
    assertU(adoc("id","P.C", "srpt_packedquad","LINESTRING (30 10, 10 30, 40 40)"));

    
    // single valued field
    assertU(adoc("id","R.A", "srptgeom","POINT( 1 2 )"));

    // non-spatial field
    assertU(adoc("id","S.X", "str_shape","POINT( 1 2 )"));
    assertU(adoc("id","S.A", "str_shape","{\"type\":\"Point\",\"coordinates\":[1,2]}"));
    

    assertU(commit());
  }

  @SuppressWarnings({"unchecked"})
  protected Map<String,Object> readJSON(String json) {
    try {
      return jsonmapper.readValue(json, Map.class);
    }
    catch(Exception ex) {
      log.warn("Unable to read GeoJSON From: {}", json);
      log.warn("Error", ex);
      fail("Unable to parse JSON GeoJSON Response");
    }
    return null; 
  }
  
  @SuppressWarnings({"unchecked"})
  protected Map<String,Object> getFirstFeatureGeometry(Map<String,Object> json)
  {
    Map<String,Object> rsp = (Map<String,Object>)json.get("response");
    assertEquals("FeatureCollection", rsp.get("type"));
    List<Object> vals = (List<Object>)rsp.get("features");
    assertEquals(1, vals.size());
    Map<String,Object> feature = (Map<String,Object>)vals.get(0);
    assertEquals("Feature", feature.get("type"));
    return (Map<String,Object>)feature.get("geometry");
  }

  @Test
  public void testRequestExceptions() throws Exception {
    
    // Make sure we select the field
    try {
      h.query(req(
        "q","*:*", 
        "wt","geojson", 
        "fl","*"));
      fail("should Require a parameter to select the field");
    }
    catch(SolrException ex) {}
    

    // non-spatial fields *must* be stored as JSON
    try {
      h.query(req(
        "q","id:S.X", 
        "wt","geojson", 
        "fl","*",
        "geojson.field", "str_shape"));
      fail("should complain about bad shape config");
    }
    catch(SolrException ex) {}
    
  }

  @Test
  public void testGeoJSONAtRoot() throws Exception {
    
    // Try reading the whole resposne
    String json = h.query(req(
        "q","*:*", 
        "wt","geojson", 
        "rows","2", 
        "fl","*", 
        "geojson.field", "stqpt_geohash",
        "indent","true"));
    
    // Check that we have a normal solr response with 'responseHeader' and 'response'
    Map<String,Object> rsp = readJSON(json);
    assertNotNull(rsp.get("responseHeader"));
    assertNotNull(rsp.get("response"));
    
    json = h.query(req(
        "q","*:*", 
        "wt","geojson", 
        "rows","2", 
        "fl","*", 
        "omitHeader", "true",
        "geojson.field", "stqpt_geohash",
        "indent","true"));
    
    // Check that we have a normal solr response with 'responseHeader' and 'response'
    rsp = readJSON(json);
    assertNull(rsp.get("responseHeader"));
    assertNull(rsp.get("response"));
    assertEquals("FeatureCollection", rsp.get("type"));
    assertNotNull(rsp.get("features"));
  }
  
  @Test
  public void testGeoJSONOutput() throws Exception {
    
    // Try reading the whole resposne
    readJSON(h.query(req(
        "q","*:*", 
        "wt","geojson", 
        "fl","*", 
        "geojson.field", "stqpt_geohash",
        "indent","true")));
    
    // Multivalued Valued Point
    Map<String,Object> json = readJSON(h.query(req(
        "q","id:H.B", 
        "wt","geojson", 
        "fl","*", 
        "geojson.field", "srpt_geohash",
        "indent","true")));
    
    Map<String,Object> geo = getFirstFeatureGeometry(json);
    assertEquals( // NOTE: not actual JSON, it is Map.toString()!
        "{type=GeometryCollection, geometries=["
        + "{type=Point, coordinates=[1, 2]}, "
        + "{type=Point, coordinates=[3, 4]}]}", ""+geo);  
    
    
    // Check the same value encoded on different field types
    String[][] check = new String[][] {
      { "id:H.A", "srpt_geohash" },
      { "id:Q.A", "srpt_quad" },
      { "id:P.A", "srpt_packedquad" },
      { "id:R.A", "srptgeom" },
      { "id:S.A", "str_shape" },
    };
    
    for(String[] args : check) {
      json = readJSON(h.query(req(
          "q",args[0], 
          "wt","geojson", 
          "fl","*", 
          "geojson.field", args[1])));
      
      geo = getFirstFeatureGeometry(json);
      assertEquals( 
        "Error reading point from: "+args[1] + " ("+args[0]+")",
        // NOTE: not actual JSON, it is Map.toString()!
        "{type=Point, coordinates=[1, 2]}", ""+geo);  
    }
  }
  
  @SuppressWarnings({"unchecked"})
  protected Map<String,Object> readFirstDoc(String json)
  {
    @SuppressWarnings({"rawtypes"})
    List docs = (List)((Map)readJSON(json).get("response")).get("docs");
    return (Map)docs.get(0);
  }
  
  public static String normalizeMapToJSON(String val) {
    val = val.replace("\"", ""); // remove quotes
    val = val.replace(':', '=');
    val = val.replace(", ", ",");
    return val;
  }

  @Test
  public void testTransformToAllFormats() throws Exception {
    
    String wkt = "POINT( 1 2 )";
    SupportedFormats fmts = SpatialContext.GEO.getFormats();
    Shape shape = fmts.read(wkt);
    
    String[] check = new String[] {
        "srpt_geohash",
        "srpt_geohash",
        "srpt_quad",
        "srpt_packedquad",
        "srptgeom",
 //       "str_shape",  // NEEDS TO BE A SpatialField!
    };
    
    String[] checkFormats = new String[] {
        "GeoJSON",
        "WKT",
        "POLY"
    };
    
    for(String field : check) {
      // Add a document with the given field
      assertU(adoc("id","test", 
          field, wkt));
      assertU(commit());
      
      
      for(String fmt : checkFormats) {
        String json = h.query(req(
            "q","id:test", 
            "wt","json", 
            "indent", "true",
            "fl","xxx:[geo f="+field+" w="+fmt+"]"
            ));
        
        Map<String,Object> doc = readFirstDoc(json);
        Object v = doc.get("xxx");
        String expect = fmts.getWriter(fmt).toString(shape);
        
        if(!(v instanceof String)) {
          v = normalizeMapToJSON(v.toString());
          expect = normalizeMapToJSON(expect);
        }
        
        assertEquals("Bad result: "+field+"/"+fmt, expect, v.toString());
      }
    }
  }
}
