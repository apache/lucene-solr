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

package org.apache.solr.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import junit.framework.TestCase;

/**
 */
public class SolrDocumentTest extends TestCase 
{
  public void testSimple() 
  {
    Float fval = new Float( 10.01f );
    Boolean bval = Boolean.TRUE;
    String sval = "12qwaszx";
    
    // Set up a simple document
    SolrDocument doc = new SolrDocument();
    doc.addField( "f", fval );
    doc.addField( "b", bval );
    doc.addField( "s", sval );
    doc.addField( "f", 100 ); // again, but something else

    // make sure we can pull values out of it
    assertEquals( fval, doc.getFirstValue( "f" ) );
    assertEquals( fval, doc.getFieldValues( "f" ).iterator().next() );
    assertEquals( fval, ((Collection<Object>)doc.getFieldValue( "f" )).iterator().next() );
    assertEquals( bval, doc.getFieldValue( "b" ) );
    assertEquals( sval, doc.getFieldValue( "s" ) );
    assertEquals( 2, doc.getFieldValues( "f" ).size() );
    assertNull( doc.getFieldValue( "xxxxx" ) );
    assertNull( doc.getFieldValues( "xxxxx" ) );
    
    List<String> keys = new ArrayList<String>();
    for( String s : doc.getFieldNames() ) {
      keys.add( s );
    }
    Collections.sort( keys );
    assertEquals( 3, keys.size() );
    assertEquals( "[b, f, s]", keys.toString() );
    
    // set field replaced existing values:
    doc.setField( "f", fval );
    assertEquals( 1, doc.getFieldValues( "f" ).size() );
    assertEquals( fval, doc.getFieldValue( "f" ) );
    
    doc.setField( "n", null );
    assertEquals( null, doc.getFieldValue( "n" ) );
    
    // now remove some fields
    assertEquals( true, doc.removeFields( "f" ) );
    assertEquals( false, doc.removeFields( "asdgsadgas" ) );
    assertNull( doc.getFieldValue( "f" ) );
    assertNull( doc.getFieldValues( "f" ) );
  }
    
  public void testUnsupportedStuff()
  {
    SolrDocument doc = new SolrDocument();

    try { doc.getFieldValueMap().clear();               fail( "should be unsupported!" ); } catch( Exception ex ){}
    try { doc.getFieldValueMap().containsValue( null ); fail( "should be unsupported!" ); } catch( Exception ex ){}
    try { doc.getFieldValueMap().entrySet();            fail( "should be unsupported!" ); } catch( Exception ex ){}
    try { doc.getFieldValueMap().putAll( null );        fail( "should be unsupported!" ); } catch( Exception ex ){}
    try { doc.getFieldValueMap().values();              fail( "should be unsupported!" ); } catch( Exception ex ){}

    assertEquals( null, doc.getFieldValueMap().get( "aaa" ) );
    doc.setField( "aaa", "bbb" );
    assertEquals( "bbb", doc.getFieldValueMap().get( "aaa" ) );
    doc.getFieldValueMap().remove( "aaa" );
    assertEquals( null, doc.getFieldValueMap().get( "aaa" ) );
  }
  
  public void testAddCollections()
  {
    List<String> c0 = new ArrayList<String>();
    c0.add( "aaa" );
    c0.add( "aaa" );
    c0.add( "aaa" );
    c0.add( "bbb" );
    c0.add( "ccc" );
    
    SolrDocument doc = new SolrDocument();
    doc.addField( "v", c0 );
    assertEquals( c0.size(), doc.getFieldValues("v").size() );
    
    // Same thing with an array
    Object[] arr = new Object[] { "aaa", "aaa", "aaa", 10, 'b' };
    doc = new SolrDocument();
    doc.addField( "v", c0 );
    assertEquals( arr.length, doc.getFieldValues("v").size() );
  }
  
  public void testOrderedDistinctFields()
  {
    List<String> c0 = new ArrayList<String>();
    c0.add( "aaa" );
    c0.add( "bbb" );
    c0.add( "aaa" );
    c0.add( "aaa" );
    c0.add( "ccc" );
    
    SolrInputDocument doc = new SolrInputDocument();
    doc.setRemoveDuplicateFieldValues( "f1", true );
    doc.setRemoveDuplicateFieldValues( "f2", false );
    doc.addField( "f1", c0, 1.0f );
    doc.addField( "f2", c0, 1.0f );
    assertEquals( 3, doc.getField("f1").getValueCount() );
    assertEquals( 5, doc.getField("f2").getValueCount() );

    assertEquals( "[aaa, bbb, ccc]", doc.getField( "f1" ).getValues().toString() );
    assertEquals( "[aaa, bbb, aaa, aaa, ccc]", doc.getField( "f2" ).getValues().toString() );
  }
 
  public void testDuplicate() 
  {
    Float fval0 = new Float( 10.01f );
    Float fval1 = new Float( 11.01f );
    Float fval2 = new Float( 12.01f );
    
    // Set up a simple document
    SolrInputDocument doc = new SolrInputDocument();
    for( int i=0; i<5; i++ ) {
      doc.addField( "f", fval0, 1.0f );
      doc.addField( "f", fval1, 1.0f );
      doc.addField( "f", fval2, 1.0f );
    }
    assertEquals( (3*5), doc.getField("f").getValueCount() );
    
    try {
      doc.setRemoveDuplicateFieldValues( "f", true );
      fail( "can't change distinct for an existing field" );
    }
    catch( Exception ex ) {}
    
    doc.removeField( "f" );
    doc.setRemoveDuplicateFieldValues( "f", true );
    for( int i=0; i<5; i++ ) {
      doc.addField( "f", fval0, 1.0f );
      doc.addField( "f", fval1, 1.0f );
      doc.addField( "f", fval2, 1.0f );
    }
    assertEquals( (3), doc.getField("f").getValueCount() );
  }
}




