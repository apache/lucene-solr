package org.apache.lucene.search.cache;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.FieldCache.*;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;
import org.junit.BeforeClass;

import static org.hamcrest.CoreMatchers.*;

public class TestEntryCreators extends LuceneTestCase {
  protected IndexReader reader;
  private static int NUM_DOCS;
  private Directory directory;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    NUM_DOCS = atLeast(500);
  }

  static class NumberTypeTester {
    String funcName;
    Class<? extends CachedArrayCreator> creator;
    Class<? extends Parser> parser;
    String field;
    Number[] values;
    
    public NumberTypeTester( String f, String func, Class<? extends CachedArrayCreator> creator, Class<? extends Parser> parser ) {
      field = f;
      funcName = func;
      this.creator = creator;
      this.parser = parser;
      values = new Number[NUM_DOCS];
    }
    @Override
    public String toString()
    {
      return field;
    }
  }
  private NumberTypeTester[] typeTests;
  
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer= new RandomIndexWriter(random, directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));

    typeTests = new NumberTypeTester[] {
        new NumberTypeTester( "theRandomByte",   "getBytes",   ByteValuesCreator.class,   ByteParser.class ),
        new NumberTypeTester( "theRandomShort",  "getShorts",  ShortValuesCreator.class,  ShortParser.class  ),
        new NumberTypeTester( "theRandomInt",    "getInts",    IntValuesCreator.class,    IntParser.class  ),
        new NumberTypeTester( "theRandomLong",   "getLongs",   LongValuesCreator.class,   LongParser.class  ),
        new NumberTypeTester( "theRandomFloat",  "getFloats",  FloatValuesCreator.class,  FloatParser.class  ),
        new NumberTypeTester( "theRandomDouble", "getDoubles", DoubleValuesCreator.class, DoubleParser.class  ),
    };
    
    for (int i = 0; i < NUM_DOCS; i++){
      Document doc = new Document();
      
      // Test the valid bits
      for( NumberTypeTester tester : typeTests ) {
        if (random.nextInt(20) != 17 && i > 1) {
          tester.values[i] = 10 + random.nextInt( 20 ); // get some field overlap
          FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
          customType.setTokenized(false);
          doc.add(newField(tester.field, String.valueOf(tester.values[i]), customType));
        }
      }
      writer.addDocument(doc);
    }

    reader = writer.getReader();
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  public void testKeys() throws IOException {
    // Check that the keys are unique for different fields

    EntryKey key_1 = new ByteValuesCreator( "field1", null ).getCacheKey();
    EntryKey key_2 = new ByteValuesCreator( "field2", null ).getCacheKey();
    assertThat("different fields should have a different key", key_1, not(key_2) );

    key_1 = new ByteValuesCreator(  "field1", null ).getCacheKey();
    key_2 = new ShortValuesCreator( "field1", null ).getCacheKey();
    assertThat( "same field different type should have different key", key_1, not( key_2 ) );

    key_1 = new ByteValuesCreator( "ff", null ).getCacheKey();
    key_2 = new ByteValuesCreator( "ff", null ).getCacheKey();
    assertThat( "same args should have same key", key_1, is( key_2 ) );

    key_1 = new ByteValuesCreator( "ff", null, ByteValuesCreator.OPTION_CACHE_BITS ^ ByteValuesCreator.OPTION_CACHE_VALUES ).getCacheKey();
    key_2 = new ByteValuesCreator( "ff", null ).getCacheKey();
    assertThat( "different options should share same key", key_1, is( key_2 ) );

    key_1 = new IntValuesCreator( "ff", FieldCache.DEFAULT_INT_PARSER ).getCacheKey();
    key_2 = new IntValuesCreator( "ff", FieldCache.NUMERIC_UTILS_INT_PARSER ).getCacheKey();
    assertThat( "diferent parser should have same key", key_1, is( key_2 ) );
  }
  
  private CachedArray getWithReflection( FieldCache cache, NumberTypeTester tester, int flags ) throws IOException 
  {
    try {
      Method getXXX = cache.getClass().getMethod( tester.funcName, IndexReader.class, String.class, EntryCreator.class );
      Constructor constructor = tester.creator.getConstructor( String.class, tester.parser, Integer.TYPE );
      CachedArrayCreator creator = (CachedArrayCreator)constructor.newInstance( tester.field, null, flags );
      return (CachedArray) getXXX.invoke(cache, reader, tester.field, creator );
    }
    catch( Exception ex ) {
      throw new RuntimeException( "Reflection failed", ex );
    }
  }

  public void testCachedArrays() throws IOException 
  {
    FieldCache cache = FieldCache.DEFAULT;

    // Check the Different CachedArray Types
    CachedArray last = null;
    CachedArray justbits = null;
    String field;
    
    for( NumberTypeTester tester : typeTests ) {
      justbits = getWithReflection( cache, tester, CachedArrayCreator.OPTION_CACHE_BITS );
      assertNull( "should not get values : "+tester, justbits.getRawArray() );
      assertNotNull( "should get bits : "+tester, justbits.valid );
      last = getWithReflection( cache, tester, CachedArrayCreator.CACHE_VALUES_AND_BITS );
      assertEquals( "should use same cached object : "+tester, justbits, last );
      assertNull( "Validate=false shoudl not regenerate : "+tester, justbits.getRawArray() ); 
      last = getWithReflection( cache, tester, CachedArrayCreator.CACHE_VALUES_AND_BITS_VALIDATE );
      assertEquals( "should use same cached object : "+tester, justbits, last );
      assertNotNull( "Validate=true should add the Array : "+tester, justbits.getRawArray() ); 
      checkCachedArrayValuesAndBits( tester, last );
    }
    
    // Now switch the the parser (for the same type) and expect an error
    cache.purgeAllCaches();
    int flags = CachedArrayCreator.CACHE_VALUES_AND_BITS_VALIDATE;
    field = "theRandomInt";
    last = cache.getInts(reader, field, new IntValuesCreator( field, FieldCache.DEFAULT_INT_PARSER, flags ) );
    checkCachedArrayValuesAndBits( typeTests[2], last );
    try {
      cache.getInts(reader, field, new IntValuesCreator( field, FieldCache.NUMERIC_UTILS_INT_PARSER, flags ) );
      fail( "Should fail if you ask for the same type with a different parser : " + field );
    } catch( Exception ex ) {} // expected

    field = "theRandomLong";
    last = cache.getLongs(reader,   field, new LongValuesCreator( field, FieldCache.DEFAULT_LONG_PARSER, flags ) );
    checkCachedArrayValuesAndBits( typeTests[3], last );
    try {
      cache.getLongs(reader, field, new LongValuesCreator( field, FieldCache.NUMERIC_UTILS_LONG_PARSER, flags ) );
      fail( "Should fail if you ask for the same type with a different parser : " + field );
    } catch( Exception ex ) {} // expected

    field = "theRandomFloat";
    last = cache.getFloats(reader,   field, new FloatValuesCreator( field, FieldCache.DEFAULT_FLOAT_PARSER, flags ) );
    checkCachedArrayValuesAndBits( typeTests[4], last );
    try {
      cache.getFloats(reader, field, new FloatValuesCreator( field, FieldCache.NUMERIC_UTILS_FLOAT_PARSER, flags ) );
      fail( "Should fail if you ask for the same type with a different parser : " + field );
    } catch( Exception ex ) {} // expected

    field = "theRandomDouble";
    last = cache.getDoubles(reader,   field, new DoubleValuesCreator( field, FieldCache.DEFAULT_DOUBLE_PARSER, flags ) );
    checkCachedArrayValuesAndBits( typeTests[5], last );
    try {
      cache.getDoubles(reader, field, new DoubleValuesCreator( field, FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, flags ) );
      fail( "Should fail if you ask for the same type with a different parser : " + field );
    } catch( Exception ex ) {} // expected
  }

  private void checkCachedArrayValuesAndBits( NumberTypeTester tester, CachedArray cachedVals )
  {
//    for( int i=0; i<NUM_DOCS; i++ ) {
//      System.out.println( i + "] "+ tester.values[i] + " :: " + cachedVals.valid.get(i) );
//    }
    
    int numDocs =0;
    Set<Number> distinctTerms = new HashSet<Number>();
    for( int i=0; i<NUM_DOCS; i++ ) {
      Number v = tester.values[i];
      boolean isValid = cachedVals.valid.get(i);
      if( v != null ) {
        numDocs++;
        distinctTerms.add( v );
        assertTrue( "Valid bit should be true ("+i+"="+tester.values[i]+") "+tester, isValid );        
      }
      else {
        assertFalse( "Valid bit should be false ("+i+") "+tester, isValid );        
      }
    }
    assertEquals( "Cached numTerms does not match : "+tester, distinctTerms.size(), cachedVals.numTerms );
    assertEquals( "Cached numDocs does not match : "+tester, numDocs, cachedVals.numDocs );
    assertEquals( "Ordinal should match numDocs : "+tester, numDocs, ((FixedBitSet)cachedVals.valid).cardinality() );
  }

}
