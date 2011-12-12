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

package org.apache.solr.common.params;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.SolrException;

/**
 */
public class SolrParamTest extends LuceneTestCase 
{  
  public void testGetParams() {
    Map<String,String> pmap = new HashMap<String, String>();
    pmap.put( "str"        , "string"   );
    pmap.put( "bool"       , "true"     );
    pmap.put( "true-0"     , "true"     );
    pmap.put( "true-1"     , "yes"      );
    pmap.put( "true-2"     , "on"       );
    pmap.put( "false-0"    , "false"    );
    pmap.put( "false-1"    , "off"      );
    pmap.put( "false-2"    , "no"       );
    pmap.put( "int"        , "100"      );
    pmap.put( "float"      , "10.6"     );
    pmap.put( "f.fl.str"   , "string"   );
    pmap.put( "f.fl.bool"  , "true"     );
    pmap.put( "f.fl.int"   , "100"      );
    pmap.put( "f.fl.float" , "10.6"     );
    pmap.put( "f.bad.bool" , "notbool"  );
    pmap.put( "f.bad.int"  , "notint"   );
    pmap.put( "f.bad.float", "notfloat" );
    final SolrParams params = new MapSolrParams( pmap );
    
    // Test the string values we put in directly
    assertEquals(  "string"   , params.get( "str"       ) );
    assertEquals(  "true"     , params.get( "bool"      ) );
    assertEquals(  "100"      , params.get( "int"       ) );
    assertEquals(  "10.6"     , params.get( "float"     ) );
    assertEquals(  "string"   , params.get( "f.fl.str"    ) );
    assertEquals(  "true"     , params.get( "f.fl.bool"   ) );
    assertEquals(  "100"      , params.get( "f.fl.int"    ) );
    assertEquals(  "10.6"     , params.get( "f.fl.float"  ) );
    assertEquals(  "notbool"  , params.get( "f.bad.bool"  ) );
    assertEquals(  "notint"   , params.get( "f.bad.int"   ) );
    assertEquals(  "notfloat" , params.get( "f.bad.float" ) );
    
    final String  pstr = "string";
    final Boolean pbool = Boolean.TRUE;
    final Integer pint = new Integer( 100 );
    final Float   pfloat = new Float( 10.6f );
    
    // Make sure they parse ok
    assertEquals( pstr   , params.get(      "str"      ) );
    assertEquals( pbool  , params.getBool(  "bool"     ) );
    assertEquals( pint   , params.getInt(   "int"      ) );
    assertEquals( pfloat , params.getFloat( "float"    ) );
    assertEquals( pbool  , params.getBool(  "f.fl.bool"  ) );
    assertEquals( pint   , params.getInt(   "f.fl.int"   ) );
    assertEquals( pfloat , params.getFloat( "f.fl.float" ) );
    assertEquals( pstr   , params.getFieldParam( "fl", "str"  ) );
    assertEquals( pbool  , params.getFieldBool(  "fl", "bool" ) );
    assertEquals( pint   , params.getFieldInt(   "fl", "int"  ) );
    assertEquals( pfloat , params.getFieldFloat( "fl", "float" ) );
    
    // Test field defaulting (fall through to non-field-specific value)
    assertEquals( pint   , params.getFieldInt( "fff",  "int"      ) );
    
    // test boolean parsing
    for( int i=0; i<3; i++ ) {
      // Must use Boolean rather than boolean reference value to prevent
      // auto-unboxing ambiguity
      assertEquals( Boolean.TRUE,  params.getBool( "true-"+i  ) );
      assertEquals( Boolean.FALSE, params.getBool( "false-"+i ) );
    }
    
    // Malformed params: These should throw a 400
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { params.getInt(   "f.bad.int" ); } } ) );
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { params.getBool(  "f.bad.bool" ); } } ) );
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { params.getFloat( "f.bad.float" ); } } ) );
    
    // Ask for params that arent there
    assertNull( params.get( "asagdsaga" ) );
    assertNull( params.getBool( "asagdsaga" ) );
    assertNull( params.getInt( "asagdsaga" ) );
    assertNull( params.getFloat( "asagdsaga" ) );
    
    // Get things with defaults
    assertEquals( pstr                  , params.get(          "xxx", pstr   ) );
    assertEquals( pbool.booleanValue()  , params.getBool(      "xxx", pbool   ) );
    assertEquals( pint.intValue()       , params.getInt(       "xxx", pint   ) );
    assertEquals( pfloat.floatValue()   , params.getFloat(     "xxx", pfloat  ), 0.1);
    assertEquals( pbool.booleanValue()  , params.getFieldBool( "xxx", "bool", pbool ) );
    assertEquals( pint.intValue()       , params.getFieldInt(  "xxx", "int", pint  ) );
    assertEquals( pfloat.floatValue()   , params.getFieldFloat("xxx", "float", pfloat  ), 0.1);
    assertEquals( pstr                  , params.getFieldParam("xxx", "str", pstr  ) );

    // Required params testing uses decorator
    final SolrParams required = params.required();
    
    // Required params which are present should test same as above
    assertEquals( pstr   , required.get(      "str"      ) );
    assertEquals( pbool  , required.getBool(  "bool"     ) );
    assertEquals( pint   , required.getInt(   "int"      ) );
    assertEquals( pfloat , required.getFloat( "float"    ) );
    
    // field value present
    assertEquals( pbool  , required.getFieldBool(  "fl", "bool" ) );
    // field defaulting (fall through to non-field-specific value)
    assertEquals( pstr   , required.getFieldParams("fakefield", "str")[0] );
    assertEquals( pstr   , required.getFieldParam( "fakefield", "str"   ) );
    assertEquals( pbool  , required.getFieldBool(  "fakefield", "bool"  ) );
    assertEquals( pint   , required.getFieldInt(   "fakefield", "int"   ) );
    assertEquals( pfloat , required.getFieldFloat( "fakefield", "float" ) );
    
    // Required params which are missing: These should throw a 400
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { required.get( "aaaa" ); } } ) );
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { required.getInt(   "f.bad.int" ); } } ) );
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { required.getBool(  "f.bad.bool" ); } } ) );
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { required.getFloat( "f.bad.float" ); } } ) );
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { required.getInt(   "aaa" ); } } ) );
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { required.getBool(  "aaa" ); } } ) );
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { required.getFloat( "aaa" ); } } ) );
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { params.getFieldBool(  "bad", "bool" ); } } ) );
    assertEquals( 400, getReturnCode( new Runnable() { public void run() { params.getFieldInt(   "bad", "int"  ); } } ) );

    // Fields with default use their parent value:
    assertEquals(
        params.get(   "aaaa", "str" ), 
        required.get( "aaaa", "str" ) );
    assertEquals(
        params.getInt(   "f.bad.nnnn", pint ), 
        required.getInt( "f.bad.nnnn", pint ) );
    
    // Check default SolrParams
    Map<String,String> dmap = new HashMap<String, String>();
    // these are not defined in params
    dmap.put( "dstr"               , "default"   );
    dmap.put( "dint"               , "123"       );
    // these are defined in params
    dmap.put( "int"                , "456"       );
    SolrParams defaults = SolrParams.wrapDefaults(params, new MapSolrParams(dmap));
  
    // in params, not in default
    assertEquals( pstr                  , defaults.get( "str"      ) );
    // in default, not in params
    assertEquals( "default"             , defaults.get( "dstr"      ) );
    assertEquals( new Integer(123)      , defaults.getInt(  "dint"     ) );
    // in params, overriding defaults
    assertEquals( pint                  , defaults.getInt(   "int"      ) );
    // in neither params nor defaults
    assertNull( defaults.get( "asagdsaga" ) );
  }
  
  public static int getReturnCode( Runnable runnable )
  {
    try {
      runnable.run();
    }
    catch( SolrException sx ) {
      return sx.code();
    }
    catch( Exception ex ) {
      ex.printStackTrace();
      return 500;
    }
    return 200;
  }
}
