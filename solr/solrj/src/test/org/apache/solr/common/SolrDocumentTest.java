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
package org.apache.solr.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCase;

/**
 */
public class SolrDocumentTest extends SolrTestCase
{
  @SuppressWarnings({"unchecked"})
  public void testSimple()
  {
    Float fval = 10.01f;
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
    
    List<String> keys = new ArrayList<>();
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
    
    try { doc.getFieldValueMap().clear();               fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValueMap().containsValue( null ); fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValueMap().entrySet();            fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValueMap().putAll( null );        fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValueMap().values();              fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValueMap().remove( "key" );       fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValueMap().put( "key", "value" ); fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}

    try { doc.getFieldValuesMap().clear();               fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValuesMap().containsValue( null ); fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValuesMap().entrySet();            fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValuesMap().putAll( null );        fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValuesMap().values();              fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValuesMap().remove( "key" );       fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}
    try { doc.getFieldValueMap().put( "key", Collections.emptyList() ); fail( "should be unsupported!" ); } catch( UnsupportedOperationException ex ){}

    assertEquals( null, doc.getFieldValueMap().get( "aaa" ) );
    doc.setField( "aaa", "bbb" );
    assertEquals( "bbb", doc.getFieldValueMap().get( "aaa" ) );
  }
  
  public void testAddCollections()
  {
    final List<String> c0 = new ArrayList<>();
    c0.add( "aaa" );
    c0.add( "aaa" );
    c0.add( "aaa" );
    c0.add( "bbb" );
    c0.add( "ccc" );
    c0.add( "ddd" );
    
    SolrDocument doc = new SolrDocument();
    doc.addField( "v", c0 );
    assertEquals( c0.size(), doc.getFieldValues("v").size() );
    assertEquals( c0.get(0), doc.getFirstValue( "v" ) );
    
    // Same thing with an array
    Object[] arr = new Object[] { "aaa", "aaa", "aaa", 10, 'b' };
    doc = new SolrDocument();
    doc.addField( "v", arr );
    assertEquals( arr.length, doc.getFieldValues("v").size() );
    // try the same thing with 'setField'
    doc.setField( "v", arr );
    assertEquals( arr.length, doc.getFieldValues("v").size() );
    
    doc.clear();
    assertEquals( 0, doc.getFieldNames().size() );
    
    @SuppressWarnings({"rawtypes"})
    Iterable iter = new Iterable() {
      @Override
      @SuppressWarnings({"rawtypes"})
      public Iterator iterator() {
        return c0.iterator();
      }
    };
    doc.addField( "v", iter );
    assertEquals( c0.size(), doc.getFieldValues("v").size() );
    // do it again to get twice the size...
    doc.addField( "v", iter );
    assertEquals( c0.size()*2, doc.getFieldValues("v").size() );
    
    // An empty list:
    doc.setField( "empty", new ArrayList<String>() );
    assertNull( doc.getFirstValue( "empty" ) );

    // Try the JSTL accessor functions...
    assertFalse( doc.getFieldValueMap().isEmpty() );
    assertFalse( doc.getFieldValuesMap().isEmpty() );
    assertEquals( 2, doc.getFieldValueMap().size() );
    assertEquals( 2, doc.getFieldValuesMap().size() );
    assertTrue( doc.getFieldValueMap().containsKey( "v" ) );
    assertTrue( doc.getFieldValuesMap().containsKey( "v" ) );
    assertTrue( doc.getFieldValueMap().keySet().contains( "v" ) );
    assertTrue( doc.getFieldValuesMap().keySet().contains( "v" ) );
    assertFalse( doc.getFieldValueMap().containsKey( "g" ) );
    assertFalse( doc.getFieldValuesMap().containsKey( "g" ) );
    assertFalse( doc.getFieldValueMap().keySet().contains( "g" ) );
    assertFalse( doc.getFieldValuesMap().keySet().contains( "g" ) );

    // A read-only list shouldn't break addField("v", ...).
    List<String> ro = Collections.unmodifiableList(c0);
    doc = new SolrDocument();
    doc.addField( "v", ro );

    // This should NOT throw an UnsupportedOperationException.
    doc.addField( "v", "asdf" );

    // set field using a collection is documented to be backed by 
    // that collection, so changes should affect it.
    Collection<String> tmp = new ArrayList<>(3);
    tmp.add("one");
    doc.setField( "collection_backed", tmp );
    assertEquals("collection not the same", 
                 tmp, doc.getFieldValues( "collection_backed" ));
    tmp.add("two");
    assertEquals("wrong size", 
                 2, doc.getFieldValues( "collection_backed" ).size());
    assertEquals("collection not the same", 
                 tmp, doc.getFieldValues( "collection_backed" ));
    
  }
   
  public void testDuplicate() 
  {
    Float fval0 = 10.01f;
    Float fval1 = 11.01f;
    Float fval2 = 12.01f;
    
    // Set up a simple document
    SolrInputDocument doc = new SolrInputDocument();
    for( int i=0; i<5; i++ ) {
      doc.addField( "f", fval0 );
      doc.addField( "f", fval1 );
      doc.addField( "f", fval2 );
    }
    assertEquals( (3*5), doc.getField("f").getValueCount() );
  }
  
  public void testMapInterface()
  {
    SolrDocument doc = new SolrDocument();
    assertTrue( doc instanceof Map );
    assertTrue( Map.class.isAssignableFrom( SolrDocument.class ) );
    
    SolrInputDocument indoc = new SolrInputDocument();
    assertTrue( indoc instanceof Map );
    assertTrue( Map.class.isAssignableFrom( indoc.getClass() ) );
  }
}




