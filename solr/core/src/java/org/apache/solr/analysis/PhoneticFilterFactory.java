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

package org.apache.solr.analysis;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.codec.Encoder;
import org.apache.commons.codec.language.Caverphone;
import org.apache.commons.codec.language.ColognePhonetic;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.phonetic.PhoneticFilter;
import org.apache.solr.common.SolrException;

/**
 * Factory for {@link PhoneticFilter}.
 * 
 * Create tokens based on phonetic encoders
 * 
 * http://jakarta.apache.org/commons/codec/api-release/org/apache/commons/codec/language/package-summary.html
 * 
 * This takes two arguments:
 *  "encoder" required, one of "DoubleMetaphone", "Metaphone", "Soundex", "RefinedSoundex"
 * 
 * "inject" (default=true) add tokens to the stream with the offset=0
 *
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_phonetic" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.PhoneticFilterFactory" encoder="DoubleMetaphone" inject="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * 
 *
 * @see PhoneticFilter
 */
public class PhoneticFilterFactory extends BaseTokenFilterFactory 
{
  public static final String ENCODER = "encoder";
  public static final String INJECT = "inject"; // boolean
  private static final String PACKAGE_CONTAINING_ENCODERS = "org.apache.commons.codec.language.";
  
  private static final Map<String, Class<? extends Encoder>> registry = new HashMap<String, Class<? extends Encoder>>()
  {{
    put( "DoubleMetaphone".toUpperCase(Locale.ENGLISH), DoubleMetaphone.class );
    put( "Metaphone".toUpperCase(Locale.ENGLISH),       Metaphone.class );
    put( "Soundex".toUpperCase(Locale.ENGLISH),         Soundex.class );
    put( "RefinedSoundex".toUpperCase(Locale.ENGLISH),  RefinedSoundex.class );
    put( "Caverphone".toUpperCase(Locale.ENGLISH),      Caverphone.class );
    put( "ColognePhonetic".toUpperCase(Locale.ENGLISH), ColognePhonetic.class );
  }};
  private static final Lock lock = new ReentrantLock();
  
  protected boolean inject = true;
  protected String name = null;
  protected Encoder encoder = null;

  @Override
  public void init(Map<String,String> args) {
    super.init( args );

    inject = getBoolean(INJECT, true);
    
    String name = args.get( ENCODER );
    if( name == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Missing required parameter: "+ENCODER
          +" ["+registry.keySet()+"]" );
    }
    Class<? extends Encoder> clazz = registry.get(name.toUpperCase(Locale.ENGLISH));
    if( clazz == null ) {
      lock.lock();
      try {
        clazz = resolveEncoder(name);
      } finally {
        lock.unlock();
      }
    }
    
    try {
      encoder = clazz.newInstance();
      
      // Try to set the maxCodeLength
      String v = args.get( "maxCodeLength" );
      if( v != null ) {
        Method setter = encoder.getClass().getMethod( "setMaxCodeLen", int.class );
        setter.invoke( encoder, Integer.parseInt( v ) );
      }
    } 
    catch (Exception e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Error initializing: "+name + "/"+clazz, e);
    }
  }
  
  private Class<? extends Encoder> resolveEncoder(String name) {
    Class<? extends Encoder> clazz = null;
    try {
      clazz = lookupEncoder(PACKAGE_CONTAINING_ENCODERS+name);
    } catch (ClassNotFoundException e) {
      try {
        clazz = lookupEncoder(name);
      } catch (ClassNotFoundException cnfe) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Unknown encoder: "+name +" ["+registry.keySet()+"]" );
      }
    }
    catch (ClassCastException e) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Not an encoder: "+name +" ["+registry.keySet()+"]" );
    }
    return clazz;
  }
  
  private Class<? extends Encoder> lookupEncoder(String name)
      throws ClassNotFoundException {
    Class<? extends Encoder> clazz = Class.forName(name).asSubclass(Encoder.class);
    registry.put( name.toUpperCase(Locale.ENGLISH), clazz );
    return clazz;
  }

  public PhoneticFilter create(TokenStream input) {
    return new PhoneticFilter(input,encoder,inject);
  }
}
