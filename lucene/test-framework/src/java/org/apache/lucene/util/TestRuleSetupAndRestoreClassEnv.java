package org.apache.lucene.util;

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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.appending.AppendingRWCodec;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.codecs.lucene3x.PreFlexRWCodec;
import org.apache.lucene.codecs.cheapbastard.CheapBastardCodec;
import org.apache.lucene.codecs.compressing.CompressingCodec;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.codecs.lucene40.Lucene40RWCodec;
import org.apache.lucene.codecs.lucene40.Lucene40RWPostingsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41Codec;
import org.apache.lucene.codecs.lucene41.Lucene41RWCodec;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.codecs.mockrandom.MockRandomPostingsFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.index.RandomCodec;
import org.apache.lucene.search.RandomSimilarityProvider;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.internal.AssumptionViolatedException;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import static org.apache.lucene.util.LuceneTestCase.*;



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
  Codec codec;

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

    // Restore more Solr properties. 
    restoreProperties.put("solr.solr.home", System.getProperty("solr.solr.home"));
    restoreProperties.put("solr.data.dir", System.getProperty("solr.data.dir"));

    // if verbose: print some debugging stuff about which codecs are loaded.
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
              // long when looking at IW verbose output...
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
    }
    
    PREFLEX_IMPERSONATION_IS_ACTIVE = false;
    savedCodec = Codec.getDefault();
    int randomVal = random.nextInt(10);
    if ("Lucene3x".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) &&
                                          "random".equals(TEST_POSTINGSFORMAT) &&
                                          "random".equals(TEST_DOCVALUESFORMAT) &&
                                          randomVal == 0 &&
                                          !shouldAvoidCodec("Lucene3x"))) { // preflex-only setup
      codec = Codec.forName("Lucene3x");
      assert (codec instanceof PreFlexRWCodec) : "fix your classpath to have tests-framework.jar before lucene-core.jar";
      PREFLEX_IMPERSONATION_IS_ACTIVE = true;
    } else if ("Lucene40".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) &&
                                                 "random".equals(TEST_POSTINGSFORMAT) &&
                                                  randomVal == 2 &&
                                                  !shouldAvoidCodec("Lucene40"))) { // 4.0 setup
      codec = Codec.forName("Lucene40");
      assert codec instanceof Lucene40RWCodec : "fix your classpath to have tests-framework.jar before lucene-core.jar";
      assert (PostingsFormat.forName("Lucene40") instanceof Lucene40RWPostingsFormat) : "fix your classpath to have tests-framework.jar before lucene-core.jar";
    } else if ("Lucene41".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) &&
                                                 "random".equals(TEST_POSTINGSFORMAT) &&
                                                 "random".equals(TEST_DOCVALUESFORMAT) &&
                                                 randomVal == 1 &&
                                                 !shouldAvoidCodec("Lucene41"))) { 
      codec = Codec.forName("Lucene41");
      assert codec instanceof Lucene41RWCodec : "fix your classpath to have tests-framework.jar before lucene-core.jar";
    } else if (("random".equals(TEST_POSTINGSFORMAT) == false) || ("random".equals(TEST_DOCVALUESFORMAT) == false)) {
      // the user wired postings or DV: this is messy
      // refactor into RandomCodec....
      
      final PostingsFormat format;
      if ("random".equals(TEST_POSTINGSFORMAT)) {
        format = PostingsFormat.forName("Lucene41");
      } else {
        format = PostingsFormat.forName(TEST_POSTINGSFORMAT);
      }
      
      final DocValuesFormat dvFormat;
      if ("random".equals(TEST_DOCVALUESFORMAT)) {
        // pick one from SPI
        String formats[] = DocValuesFormat.availableDocValuesFormats().toArray(new String[0]);
        dvFormat = DocValuesFormat.forName(formats[random.nextInt(formats.length)]);
      } else {
        dvFormat = DocValuesFormat.forName(TEST_DOCVALUESFORMAT);
      }
      
      codec = new Lucene42Codec() {       
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return format;
        }

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          return dvFormat;
        }

        @Override
        public String toString() {
          return super.toString() + ": " + format.toString() + ", " + dvFormat.toString();
        }
      };
    } else if ("SimpleText".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal == 9 && LuceneTestCase.rarely(random) && !shouldAvoidCodec("SimpleText"))) {
      codec = new SimpleTextCodec();
    } else if ("Appending".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal == 8 && !shouldAvoidCodec("Appending"))) {
      codec = new AppendingRWCodec();
    } else if ("CheapBastard".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal == 8 && !shouldAvoidCodec("CheapBastard") && !shouldAvoidCodec("Lucene41"))) {
      // we also avoid this codec if Lucene41 is avoided, since thats the postings format it uses.
      codec = new CheapBastardCodec();
    } else if ("Asserting".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal == 6 && !shouldAvoidCodec("Asserting"))) {
      codec = new AssertingCodec();
    } else if ("Compressing".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal == 5 && !shouldAvoidCodec("Compressing"))) {
      codec = CompressingCodec.randomInstance(random);
    } else if (!"random".equals(TEST_CODEC)) {
      codec = Codec.forName(TEST_CODEC);
    } else if ("random".equals(TEST_POSTINGSFORMAT)) {
      codec = new RandomCodec(random, avoidCodecs);
    } else {
      assert false;
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

    // Check codec restrictions once at class level.
    try {
      checkCodecRestrictions(codec);
    } catch (AssumptionViolatedException e) {
      System.err.println("NOTE: " + e.getMessage() + " Suppressed codecs: " + 
          Arrays.toString(avoidCodecs.toArray()));
      throw e;
    }
  }

  /**
   * Check codec restrictions.
   * 
   * @throws AssumptionViolatedException if the class does not work with a given codec.
   */
  private void checkCodecRestrictions(Codec codec) {
    assumeFalse("Class not allowed to use codec: " + codec.getName() + ".",
        shouldAvoidCodec(codec.getName()));

    if (codec instanceof RandomCodec && !avoidCodecs.isEmpty()) {
      for (String name : ((RandomCodec)codec).formatNames) {
        assumeFalse("Class not allowed to use postings format: " + name + ".",
            shouldAvoidCodec(name));
      }
    }

    PostingsFormat pf = codec.postingsFormat();
    assumeFalse("Class not allowed to use postings format: " + pf.getName() + ".",
        shouldAvoidCodec(pf.getName()));

    assumeFalse("Class not allowed to use postings format: " + LuceneTestCase.TEST_POSTINGSFORMAT + ".", 
        shouldAvoidCodec(LuceneTestCase.TEST_POSTINGSFORMAT));
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
    if (savedLocale != null) Locale.setDefault(savedLocale);
    if (savedTimeZone != null) TimeZone.setDefault(savedTimeZone);
  }

  /**
   * Should a given codec be avoided for the currently executing suite?
   */
  private boolean shouldAvoidCodec(String codec) {
    return !avoidCodecs.isEmpty() && avoidCodecs.contains(codec);
  }
}
