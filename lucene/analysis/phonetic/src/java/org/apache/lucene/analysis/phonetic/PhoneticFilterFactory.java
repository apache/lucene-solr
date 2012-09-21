package org.apache.lucene.analysis.phonetic;

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

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.codec.Encoder;
import org.apache.commons.codec.language.*;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory; // javadocs
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link PhoneticFilter}.
 * 
 * Create tokens based on phonetic encoders from <a href="
 * http://commons.apache.org/codec/api-release/org/apache/commons/codec/language/package-summary.html
 * ">Apache Commons Codec</a>.
 * <p>
 * This takes one required argument, "encoder", and the rest are optional:
 * <dl>
 *  <dt>encoder<dd> required, one of "DoubleMetaphone", "Metaphone", "Soundex", "RefinedSoundex", "Caverphone" (v2.0),
 *  or "ColognePhonetic" (case insensitive). If encoder isn't one of these, it'll be resolved as a class name either by
 *  itself if it already contains a '.' or otherwise as in the same package as these others.
 *  <dt>inject<dd> (default=true) add tokens to the stream with the offset=0
 *  <dt>maxCodeLength<dd>The maximum length of the phonetic codes, as defined by the encoder. If an encoder doesn't
 *  support this then specifying this is an error.
 * </dl>
 *
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_phonetic" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.PhoneticFilterFactory" encoder="DoubleMetaphone" inject="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * 
 * @see PhoneticFilter
 */
public class PhoneticFilterFactory extends TokenFilterFactory
  implements ResourceLoaderAware
{
  /** parameter name: either a short name or a full class name */
  public static final String ENCODER = "encoder";
  /** parameter name: true if encoded tokens should be added as synonyms */
  public static final String INJECT = "inject"; // boolean
  /** parameter name: restricts the length of the phonetic code */
  public static final String MAX_CODE_LENGTH = "maxCodeLength";
  private static final String PACKAGE_CONTAINING_ENCODERS = "org.apache.commons.codec.language.";

  //Effectively constants; uppercase keys
  private static final Map<String, Class<? extends Encoder>> registry = new HashMap<String, Class<? extends Encoder>>(6);

  static {
    registry.put("DoubleMetaphone".toUpperCase(Locale.ROOT), DoubleMetaphone.class);
    registry.put("Metaphone".toUpperCase(Locale.ROOT), Metaphone.class);
    registry.put("Soundex".toUpperCase(Locale.ROOT), Soundex.class);
    registry.put("RefinedSoundex".toUpperCase(Locale.ROOT), RefinedSoundex.class);
    registry.put("Caverphone".toUpperCase(Locale.ROOT), Caverphone2.class);
    registry.put("ColognePhonetic".toUpperCase(Locale.ROOT), ColognePhonetic.class);
  }

  boolean inject = true; //accessed by the test
  private String name = null;
  private Class<? extends Encoder> clazz = null;
  private Method setMaxCodeLenMethod = null;
  private Integer maxCodeLength = null;
  
  /** Sole constructor. See {@link AbstractAnalysisFactory} for initialization lifecycle. */
  public PhoneticFilterFactory() {}

  @Override
  public void inform(ResourceLoader loader) throws IOException {

    inject = getBoolean(INJECT, true);

    String name = args.get( ENCODER );
    if( name == null ) {
      throw new IllegalArgumentException("Missing required parameter: " + ENCODER
          + " [" + registry.keySet() + "]");
    }
    clazz = registry.get(name.toUpperCase(Locale.ROOT));
    if( clazz == null ) {
      clazz = resolveEncoder(name, loader);
    }

    String v = args.get(MAX_CODE_LENGTH);
    if (v != null) {
      maxCodeLength = Integer.valueOf(v);
      try {
        setMaxCodeLenMethod = clazz.getMethod("setMaxCodeLen", int.class);
      } catch (Exception e) {
        throw new IllegalArgumentException("Encoder " + name + " / " + clazz + " does not support " + MAX_CODE_LENGTH, e);
      }
    }

    getEncoder();//trigger initialization for potential problems to be thrown now
  }

  private Class<? extends Encoder> resolveEncoder(String name, ResourceLoader loader) {
    String lookupName = name;
    if (name.indexOf('.') == -1) {
      lookupName = PACKAGE_CONTAINING_ENCODERS + name;
    }
    try {
      return loader.newInstance(lookupName, Encoder.class).getClass();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Error loading encoder '" + name + "': must be full class name or one of " + registry.keySet(), e);
    }
  }

  /** Must be thread-safe. */
  protected Encoder getEncoder() {
    // Unfortunately, Commons-Codec doesn't offer any thread-safe guarantees so we must play it safe and instantiate
    // every time.  A simple benchmark showed this as negligible.
    try {
      Encoder encoder = clazz.newInstance();
      // Try to set the maxCodeLength
      if(maxCodeLength != null && setMaxCodeLenMethod != null) {
        setMaxCodeLenMethod.invoke(encoder, maxCodeLength);
      }
      return encoder;
    } catch (Exception e) {
      final Throwable t = (e instanceof InvocationTargetException) ? e.getCause() : e;
      throw new IllegalArgumentException("Error initializing encoder: " + name + " / " + clazz, t);
    }
  }

  public PhoneticFilter create(TokenStream input) {
    return new PhoneticFilter(input, getEncoder(), inject);
  }

}
