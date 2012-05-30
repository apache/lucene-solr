package org.apache.lucene.util;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.appending.AppendingCodec;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.index.RandomCodec;
import org.apache.lucene.search.RandomSimilarityProvider;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import com.carrotsearch.randomizedtesting.RandomizedContext;

import static org.apache.lucene.util.LuceneTestCase.*;
import static org.apache.lucene.util.LuceneTestCase.INFOSTREAM;
import static org.apache.lucene.util.LuceneTestCase.TEST_CODEC;
import static org.apache.lucene.util.LuceneTestCase.VERBOSE;



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

/**
 * Setup and restore suite-level environment (fine grained junk that 
 * doesn't fit anywhere else).
 */
final class TestRuleSetupAndRestoreClassEnv extends AbstractBeforeAfterRule {
  /**
   * Restore these system property values.
   */
  private HashMap<String, String> restoreProperties = new HashMap<String,String>();

  private Codec savedCodec;
  private Locale savedLocale;
  private TimeZone savedTimeZone;
  private InfoStream savedInfoStream;

  Locale locale;
  TimeZone timeZone;
  Similarity similarity;

  /**
   * @see SuppressCodecs
   */
  HashSet<String> avoidCodecs;


  @Override
  protected void before() throws Exception {
    // enable this by default, for IDE consistency with ant tests (as its the default from ant)
    // TODO: really should be in solr base classes, but some extend LTC directly.
    // we do this in beforeClass, because some tests currently disable it
    restoreProperties.put("solr.directoryFactory", System.getProperty("solr.directoryFactory"));
    if (System.getProperty("solr.directoryFactory") == null) {
      System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockDirectoryFactory");
    }
    
    // enable the Lucene 3.x PreflexRW codec explicitly, to work around bugs in IBM J9 / Harmony ServiceLoader:
    try {
      final java.lang.reflect.Field spiLoaderField = Codec.class.getDeclaredField("loader");
      spiLoaderField.setAccessible(true);
      final Object spiLoader = spiLoaderField.get(null);
      final java.lang.reflect.Field modifiableServicesField = NamedSPILoader.class.getDeclaredField("modifiableServices");
      modifiableServicesField.setAccessible(true);
      @SuppressWarnings({"unchecked","rawtypes"}) final Map<String,Codec> serviceMap =
        (Map) modifiableServicesField.get(spiLoader);
      /* note: re-enable this if we make a Lucene4x impersonator 
      if (!(Codec.forName("Lucene3x") instanceof PreFlexRWCodec)) {
        if (Constants.JAVA_VENDOR.startsWith("IBM")) {
          // definitely a buggy version
          System.err.println("ERROR: Your VM's java.util.ServiceLoader implementation is buggy"+
            " and does not respect classpath order, please report this to the vendor.");
        } else {
          // could just be a classpath issue
          System.err.println("ERROR: fix your classpath to have tests-framework.jar before lucene-core.jar!"+
              " If you have already done this, then your VM's java.util.ServiceLoader implementation is buggy"+
              " and does not respect classpath order, please report this to the vendor.");
        }
        serviceMap.put("Lucene3x", new PreFlexRWCodec());
      } */
    } catch (Exception e) {
      throw new RuntimeException("Cannot access internals of Codec and NamedSPILoader classes", e);
    }
    
    // if verbose: print some debugging stuff about which codecs are loaded
    if (VERBOSE) {
      Set<String> codecs = Codec.availableCodecs();
      for (String codec : codecs) {
        System.out.println("Loaded codec: '" + codec + "': " + Codec.forName(codec).getClass().getName());
      }
      
      Set<String> postingsFormats = PostingsFormat.availablePostingsFormats();
      for (String postingsFormat : postingsFormats) {
        System.out.println("Loaded postingsFormat: '" + postingsFormat + "': " + PostingsFormat.forName(postingsFormat).getClass().getName());
      }
    }

    savedInfoStream = InfoStream.getDefault();
    final Random random = RandomizedContext.current().getRandom();
    final boolean v = random.nextBoolean();
    if (INFOSTREAM) {
      InfoStream.setDefault(new PrintStreamInfoStream(System.out) {
          @Override
          public void message(String component, String message) {
            final String name;
            if (Thread.currentThread().getName().startsWith("TEST-")) {
              // The name of the main thread is way too
              // long when looking at IW verbose output...:
              name = "main";
            } else {
              name = Thread.currentThread().getName();
            }
            stream.println(component + " " + messageID + " [" + new Date() + "; " + name + "]: " + message);    
          }
        });
    } else if (v) {
      InfoStream.setDefault(new NullInfoStream());
    }

    Class<?> targetClass = RandomizedContext.current().getTargetClass();
    avoidCodecs = new HashSet<String>();
    if (targetClass.isAnnotationPresent(SuppressCodecs.class)) {
      SuppressCodecs a = targetClass.getAnnotation(SuppressCodecs.class);
      avoidCodecs.addAll(Arrays.asList(a.value()));
      System.err.println("NOTE: Suppressing codecs " + Arrays.toString(a.value()) 
          + " for " + targetClass.getSimpleName() + ".");
    }
    
    PREFLEX_IMPERSONATION_IS_ACTIVE = false;
    savedCodec = Codec.getDefault();
    final Codec codec;
    int randomVal = random.nextInt(10);
    /* note: re-enable this if we make a 4.x impersonator
     * if ("Lucene3x".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal < 2 && !shouldAvoidCodec("Lucene3x"))) { // preflex-only setup
      codec = Codec.forName("Lucene3x");
      assert (codec instanceof PreFlexRWCodec) : "fix your classpath to have tests-framework.jar before lucene-core.jar";
      PREFLEX_IMPERSONATION_IS_ACTIVE = true;
    } else */if ("SimpleText".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal == 9 && !shouldAvoidCodec("SimpleText"))) {
      codec = new SimpleTextCodec();
    } else if ("Appending".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal == 8 && !shouldAvoidCodec("Appending"))) {
      codec = new AppendingCodec();
    } else if (!"random".equals(TEST_CODEC)) {
      codec = Codec.forName(TEST_CODEC);
    } else if ("random".equals(TEST_POSTINGSFORMAT)) {
      codec = new RandomCodec(random, avoidCodecs);
    } else {
      codec = new Lucene40Codec() {
        private final PostingsFormat format = PostingsFormat.forName(TEST_POSTINGSFORMAT);
        
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return format;
        }

        @Override
        public String toString() {
          return super.toString() + ": " + format.toString();
        }
      };
    }
    Codec.setDefault(codec);

    // Initialize locale/ timezone.
    String testLocale = System.getProperty("tests.locale", "random");
    String testTimeZone = System.getProperty("tests.timezone", "random");

    // Always pick a random one for consistency (whether tests.locale was specified or not).
    savedLocale = Locale.getDefault();
    Locale randomLocale = randomLocale(random);
    locale = testLocale.equals("random") ? randomLocale : localeForName(testLocale);
    Locale.setDefault(locale);

    // TimeZone.getDefault will set user.timezone to the default timezone of the user's locale.
    // So store the original property value and restore it at end.
    restoreProperties.put("user.timezone", System.getProperty("user.timezone"));
    savedTimeZone = TimeZone.getDefault();
    TimeZone randomTimeZone = randomTimeZone(random());
    timeZone = testTimeZone.equals("random") ? randomTimeZone : TimeZone.getTimeZone(testTimeZone);
    TimeZone.setDefault(timeZone);
    similarity = random().nextBoolean() ? new DefaultSimilarity() : new RandomSimilarityProvider(random());    
  }

  /**
   * After suite cleanup (always invoked).
   */
  @Override
  protected void after() throws Exception {
    for (Map.Entry<String,String> e : restoreProperties.entrySet()) {
      if (e.getValue() == null) {
        System.clearProperty(e.getKey());
      } else {
        System.setProperty(e.getKey(), e.getValue());
      }
    }
    restoreProperties.clear();

    Codec.setDefault(savedCodec);
    InfoStream.setDefault(savedInfoStream);
    Locale.setDefault(savedLocale);
    TimeZone.setDefault(savedTimeZone);

    System.clearProperty("solr.solr.home");
    System.clearProperty("solr.data.dir");
  }

  /**
   * Should a given codec be avoided for the currently executing suite?
   */
  public boolean shouldAvoidCodec(String codec) {
    return !avoidCodecs.isEmpty() && avoidCodecs.contains(codec);
  }
}
