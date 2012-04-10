package org.apache.lucene.analysis.core;

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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.CharReader;
import org.apache.lucene.analysis.CharStream;
import org.apache.lucene.analysis.EmptyTokenizer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.apache.lucene.analysis.ValidatingTokenFilter;
import org.apache.lucene.analysis.charfilter.CharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.compound.DictionaryCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.TestCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.apache.lucene.analysis.hunspell.HunspellDictionary;
import org.apache.lucene.analysis.hunspell.HunspellDictionaryTest;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter;
import org.apache.lucene.analysis.miscellaneous.TrimFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer;
import org.apache.lucene.analysis.payloads.IdentityEncoder;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.apache.lucene.analysis.position.PositionFilter;
import org.apache.lucene.analysis.snowball.TestSnowball;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.util.CharArrayMap;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.AttributeSource.AttributeFactory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.Rethrow;
import org.apache.lucene.util.Version;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.tartarus.snowball.SnowballProgram;
import org.xml.sax.InputSource;

/** tests random analysis chains */
public class TestRandomChains extends BaseTokenStreamTestCase {

  static List<Constructor<? extends Tokenizer>> tokenizers;
  static List<Constructor<? extends TokenFilter>> tokenfilters;
  static List<Constructor<? extends CharStream>> charfilters;

  // TODO: fix those and remove
  private static final Set<Class<?>> brokenComponents = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());
  static {
    // nocommit can we promote some of these to be only
    // offsets offenders?
    Collections.<Class<?>>addAll(brokenComponents,
      // TODO: fix basetokenstreamtestcase not to trip because this one has no CharTermAtt
      EmptyTokenizer.class,
      // doesn't actual reset itself!
      CachingTokenFilter.class,
      // doesn't consume whole stream!
      LimitTokenCountFilter.class,
      // Not broken: we forcefully add this, so we shouldn't
      // also randomly pick it:
      ValidatingTokenFilter.class,
      // NOTE: these by themselves won't cause any 'basic assertions' to fail.
      // but see https://issues.apache.org/jira/browse/LUCENE-3920, if any 
      // tokenfilter that combines words (e.g. shingles) comes after them,
      // this will create bogus offsets because their 'offsets go backwards',
      // causing shingle or whatever to make a single token with a 
      // startOffset thats > its endOffset
      // (see LUCENE-3738 for a list of other offenders here)
      // broken!
      NGramTokenizer.class,
      // broken!
      NGramTokenFilter.class,
      // broken!
      EdgeNGramTokenizer.class,
      // broken!
      EdgeNGramTokenFilter.class
    );
  }

  // TODO: also fix these and remove (maybe):
  // Classes that don't produce consistent graph offsets:
  private static final Set<Class<?>> brokenOffsetsComponents = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());
  static {
    Collections.<Class<?>>addAll(brokenOffsetsComponents,
      WordDelimiterFilter.class,
      TrimFilter.class,
      ReversePathHierarchyTokenizer.class,
      PathHierarchyTokenizer.class,
      HyphenationCompoundWordTokenFilter.class,
      DictionaryCompoundWordTokenFilter.class,
      // nocommit: corrumpts graphs (offset consistency check):
      PositionFilter.class,
      // nocommit it seems to mess up offsets!?
      WikipediaTokenizer.class
    );
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    List<Class<?>> analysisClasses = new ArrayList<Class<?>>();
    getClassesForPackage("org.apache.lucene.analysis", analysisClasses);
    tokenizers = new ArrayList<Constructor<? extends Tokenizer>>();
    tokenfilters = new ArrayList<Constructor<? extends TokenFilter>>();
    charfilters = new ArrayList<Constructor<? extends CharStream>>();
    for (final Class<?> c : analysisClasses) {
      final int modifiers = c.getModifiers();
      if (
        // don't waste time with abstract classes or deprecated known-buggy ones
        Modifier.isAbstract(modifiers) || !Modifier.isPublic(modifiers)
        || c.isSynthetic() || c.isAnonymousClass() || c.isMemberClass() || c.isInterface()
        || brokenComponents.contains(c)
        || c.isAnnotationPresent(Deprecated.class)
        || !(Tokenizer.class.isAssignableFrom(c) || TokenFilter.class.isAssignableFrom(c) || CharStream.class.isAssignableFrom(c))
      ) {
        continue;
      }
      
      for (final Constructor<?> ctor : c.getConstructors()) {
        // don't test synthetic or deprecated ctors, they likely have known bugs:
        if (ctor.isSynthetic() || ctor.isAnnotationPresent(Deprecated.class)) {
          continue;
        }
        if (Tokenizer.class.isAssignableFrom(c)) {
          assertTrue(ctor.toGenericString() + " has unsupported parameter types",
            allowedTokenizerArgs.containsAll(Arrays.asList(ctor.getParameterTypes())));
          tokenizers.add(castConstructor(Tokenizer.class, ctor));
        } else if (TokenFilter.class.isAssignableFrom(c)) {
          assertTrue(ctor.toGenericString() + " has unsupported parameter types",
            allowedTokenFilterArgs.containsAll(Arrays.asList(ctor.getParameterTypes())));
          tokenfilters.add(castConstructor(TokenFilter.class, ctor));
        } else if (CharStream.class.isAssignableFrom(c)) {
          assertTrue(ctor.toGenericString() + " has unsupported parameter types",
            allowedCharFilterArgs.containsAll(Arrays.asList(ctor.getParameterTypes())));
          charfilters.add(castConstructor(CharStream.class, ctor));
        } else {
          fail("Cannot get here");
        }
      }
    }
    
    final Comparator<Constructor<?>> ctorComp = new Comparator<Constructor<?>>() {
      @Override
      public int compare(Constructor<?> arg0, Constructor<?> arg1) {
        return arg0.toGenericString().compareTo(arg1.toGenericString());
      }
    };
    Collections.sort(tokenizers, ctorComp);
    Collections.sort(tokenfilters, ctorComp);
    Collections.sort(charfilters, ctorComp);
    if (VERBOSE) {
      System.out.println("tokenizers = " + tokenizers);
      System.out.println("tokenfilters = " + tokenfilters);
      System.out.println("charfilters = " + charfilters);
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    tokenizers = null;
    tokenfilters = null;
    charfilters = null;
  }
  
  /** Hack to work around the stupidness of Oracle's strict Java backwards compatibility.
   * {@code Class<T>#getConstructors()} should return unmodifiable {@code List<Constructor<T>>} not array! */
  @SuppressWarnings("unchecked") 
  private static <T> Constructor<T> castConstructor(Class<T> instanceClazz, Constructor<?> ctor) {
    return (Constructor<T>) ctor;
  }
  private static void getClassesForPackage(String pckgname, List<Class<?>> classes) throws Exception {
    final ClassLoader cld = TestRandomChains.class.getClassLoader();
    final String path = pckgname.replace('.', '/');
    final Enumeration<URL> resources = cld.getResources(path);
    while (resources.hasMoreElements()) {
      final File directory = new File(resources.nextElement().toURI());
      if (directory.exists()) {
        String[] files = directory.list();
        for (String file : files) {
          if (new File(directory, file).isDirectory()) {
            // recurse
            String subPackage = pckgname + "." + file;
            getClassesForPackage(subPackage, classes);
          }
          if (file.endsWith(".class")) {
            String clazzName = file.substring(0, file.length() - 6);
            // exclude Test classes that happen to be in these packages.
            // class.ForName'ing some of them can cause trouble.
            if (!clazzName.endsWith("Test") && !clazzName.startsWith("Test")) {
              // Don't run static initializers, as we won't use most of them.
              // Java will do that automatically once accessed/instantiated.
              classes.add(Class.forName(pckgname + '.' + clazzName, false, cld));
            }
          }
        }
      }
    }
  }
  
  private static interface ArgProducer {
    Object create(Random random);
  }
  
  private static final Map<Class<?>,ArgProducer> argProducers = new IdentityHashMap<Class<?>,ArgProducer>() {{
    put(int.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // TODO: could cause huge ram usage to use full int range for some filters
        // (e.g. allocate enormous arrays)
        // return Integer.valueOf(random.nextInt());
        return Integer.valueOf(_TestUtil.nextInt(random, -100, 100));
      }
    });
    put(char.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // nocommit: fix any filters that care to throw IAE instead.
        // return Character.valueOf((char)random.nextInt(65536));
        while(true) {
          char c = (char)random.nextInt(65536);
          if (c < '\uD800' || c > '\uDFFF') {
            return Character.valueOf(c);
          }
        }
      }
    });
    put(float.class, new ArgProducer() {
      @Override public Object create(Random random) {
        return Float.valueOf(random.nextFloat());
      }
    });
    put(boolean.class, new ArgProducer() {
      @Override public Object create(Random random) {
        return Boolean.valueOf(random.nextBoolean());
      }
    });
    put(byte.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // this wraps to negative when casting to byte
        return Byte.valueOf((byte) random.nextInt(256));
      }
    });
    put(byte[].class, new ArgProducer() {
      @Override public Object create(Random random) {
        byte bytes[] = new byte[random.nextInt(256)];
        random.nextBytes(bytes);
        return bytes;
      }
    });
    put(Random.class, new ArgProducer() {
      @Override public Object create(Random random) {
        return new Random(random.nextLong());
      }
    });
    put(Version.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // we expect bugs in emulating old versions
        return TEST_VERSION_CURRENT;
      }
    });
    put(Set.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // TypeTokenFilter
        Set<String> set = new HashSet<String>();
        int num = random.nextInt(5);
        for (int i = 0; i < num; i++) {
          set.add(StandardTokenizer.TOKEN_TYPES[random.nextInt(StandardTokenizer.TOKEN_TYPES.length)]);
        }
        return set;
      }
    });
    put(Collection.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // CapitalizationFilter
        Collection<char[]> col = new ArrayList<char[]>();
        int num = random.nextInt(5);
        for (int i = 0; i < num; i++) {
          col.add(_TestUtil.randomSimpleString(random).toCharArray());
        }
        return col;
      }
    });
    put(CharArraySet.class, new ArgProducer() {
      @Override public Object create(Random random) {
        int num = random.nextInt(10);
        CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, num, random.nextBoolean());
        for (int i = 0; i < num; i++) {
          // TODO: make nastier
          set.add(_TestUtil.randomSimpleString(random));
        }
        return set;
      }
    });
    put(Pattern.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // TODO: don't want to make the exponentially slow ones Dawid documents
        // in TestPatternReplaceFilter, so dont use truly random patterns (for now)
        return Pattern.compile("a");
      }
    });
    put(PayloadEncoder.class, new ArgProducer() {
      @Override public Object create(Random random) {
        return new IdentityEncoder(); // the other encoders will throw exceptions if tokens arent numbers?
      }
    });
    put(HunspellDictionary.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // TODO: make nastier
        InputStream affixStream = HunspellDictionaryTest.class.getResourceAsStream("test.aff");
        InputStream dictStream = HunspellDictionaryTest.class.getResourceAsStream("test.dic");
        try {
         return new HunspellDictionary(affixStream, dictStream, TEST_VERSION_CURRENT);
        } catch (Exception ex) {
          Rethrow.rethrow(ex);
          return null; // unreachable code
        }
      }
    });
    put(EdgeNGramTokenizer.Side.class, new ArgProducer() {
      @Override public Object create(Random random) {
        return random.nextBoolean() 
            ? EdgeNGramTokenizer.Side.FRONT 
            : EdgeNGramTokenizer.Side.BACK;
      }
    });
    put(EdgeNGramTokenFilter.Side.class, new ArgProducer() {
      @Override public Object create(Random random) {
        return random.nextBoolean() 
            ? EdgeNGramTokenFilter.Side.FRONT 
            : EdgeNGramTokenFilter.Side.BACK;
      }
    });
    put(HyphenationTree.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // TODO: make nastier
        try {
          InputSource is = new InputSource(TestCompoundWordTokenFilter.class.getResource("da_UTF8.xml").toExternalForm());
          HyphenationTree hyphenator = HyphenationCompoundWordTokenFilter.getHyphenationTree(is);
          return hyphenator;
        } catch (Exception ex) {
          Rethrow.rethrow(ex);
          return null; // unreachable code
        }
      }
    });
    put(SnowballProgram.class, new ArgProducer() {
      @Override public Object create(Random random) {
        try {
          String lang = TestSnowball.SNOWBALL_LANGS[random.nextInt(TestSnowball.SNOWBALL_LANGS.length)];
          Class<? extends SnowballProgram> clazz = Class.forName("org.tartarus.snowball.ext." + lang + "Stemmer").asSubclass(SnowballProgram.class);
          return clazz.newInstance();
        } catch (Exception ex) {
          Rethrow.rethrow(ex);
          return null; // unreachable code
        }
      }
    });
    put(String.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // TODO: make nastier
        if (random.nextBoolean()) {
          // a token type
          return StandardTokenizer.TOKEN_TYPES[random.nextInt(StandardTokenizer.TOKEN_TYPES.length)];
        } else {
          return _TestUtil.randomSimpleString(random);
        }
      }
    });
    put(NormalizeCharMap.class, new ArgProducer() {
      @Override public Object create(Random random) {
        NormalizeCharMap map = new NormalizeCharMap();
        // we can't add duplicate keys, or NormalizeCharMap gets angry
        Set<String> keys = new HashSet<String>();
        int num = random.nextInt(5);
        //System.out.println("NormalizeCharMap=");
        for (int i = 0; i < num; i++) {
          String key = _TestUtil.randomSimpleString(random);
          if (!keys.contains(key)) {
            String value = _TestUtil.randomSimpleString(random);
            map.add(key, value);
            keys.add(key);
            //System.out.println("mapping: '" + key + "' => '" + value + "'");
          }
        }
        return map;
      }
    });
    put(CharacterRunAutomaton.class, new ArgProducer() {
      @Override public Object create(Random random) {
        // TODO: could probably use a purely random automaton
        switch(random.nextInt(5)) {
          case 0: return MockTokenizer.KEYWORD;
          case 1: return MockTokenizer.SIMPLE;
          case 2: return MockTokenizer.WHITESPACE;
          case 3: return MockTokenFilter.EMPTY_STOPSET;
          default: return MockTokenFilter.ENGLISH_STOPSET;
        }
      }
    });
    put(CharArrayMap.class, new ArgProducer() {
      @Override public Object create(Random random) {
        int num = random.nextInt(10);
        CharArrayMap<String> map = new CharArrayMap<String>(TEST_VERSION_CURRENT, num, random.nextBoolean());
        for (int i = 0; i < num; i++) {
          // TODO: make nastier
          map.put(_TestUtil.randomSimpleString(random), _TestUtil.randomSimpleString(random));
        }
        return map;
      }
    });
    put(SynonymMap.class, new ArgProducer() {
      @Override public Object create(Random random) {
        SynonymMap.Builder b = new SynonymMap.Builder(random.nextBoolean());
        final int numEntries = atLeast(10);
        for (int j = 0; j < numEntries; j++) {
          addSyn(b, randomNonEmptyString(random), randomNonEmptyString(random), random.nextBoolean());
        }
        try {
          return b.build();
        } catch (Exception ex) {
          Rethrow.rethrow(ex);
          return null; // unreachable code
        }
      }
      
      private void addSyn(SynonymMap.Builder b, String input, String output, boolean keepOrig) {
        b.add(new CharsRef(input.replaceAll(" +", "\u0000")),
              new CharsRef(output.replaceAll(" +", "\u0000")),
              keepOrig);
      }
      
      private String randomNonEmptyString(Random random) {
        while(true) {
          final String s = _TestUtil.randomUnicodeString(random).trim();
          if (s.length() != 0 && s.indexOf('\u0000') == -1) {
            return s;
          }
        }
      }    
    });
  }};
  
  static final Set<Class<?>> allowedTokenizerArgs, allowedTokenFilterArgs, allowedCharFilterArgs;
  static {
    allowedTokenizerArgs = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());
    allowedTokenizerArgs.addAll(argProducers.keySet());
    allowedTokenizerArgs.add(Reader.class);
    allowedTokenizerArgs.add(AttributeFactory.class);
    allowedTokenizerArgs.add(AttributeSource.class);
    
    allowedTokenFilterArgs = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());
    allowedTokenFilterArgs.addAll(argProducers.keySet());
    allowedTokenFilterArgs.add(TokenStream.class);
    // TODO: fix this one, thats broken:
    allowedTokenFilterArgs.add(CommonGramsFilter.class);
    
    allowedCharFilterArgs = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());
    allowedCharFilterArgs.addAll(argProducers.keySet());
    allowedCharFilterArgs.add(Reader.class);
    allowedCharFilterArgs.add(CharStream.class);
  }
  
  @SuppressWarnings("unchecked")
  static <T> T newRandomArg(Random random, Class<T> paramType) {
    final ArgProducer producer = argProducers.get(paramType);
    assertNotNull("No producer for arguments of type " + paramType.getName() + " found", producer);
    return (T) producer.create(random);
  }
  
  static Object[] newTokenizerArgs(Random random, Reader reader, Class<?>[] paramTypes) {
    Object[] args = new Object[paramTypes.length];
    for (int i = 0; i < args.length; i++) {
      Class<?> paramType = paramTypes[i];
      if (paramType == Reader.class) {
        args[i] = reader;
      } else if (paramType == AttributeFactory.class) {
        // TODO: maybe the collator one...???
        args[i] = AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY;
      } else if (paramType == AttributeSource.class) {
        // nocommit: args[i] = new AttributeSource();
        // this is currently too scary to deal with!
        args[i] = null; // force IAE
      } else {
        args[i] = newRandomArg(random, paramType);
      }
    }
    return args;
  }
  
  static Object[] newCharFilterArgs(Random random, Reader reader, Class<?>[] paramTypes) {
    Object[] args = new Object[paramTypes.length];
    for (int i = 0; i < args.length; i++) {
      Class<?> paramType = paramTypes[i];
      if (paramType == Reader.class) {
        args[i] = reader;
      } else if (paramType == CharStream.class) {
        args[i] = CharReader.get(reader);
      } else {
        args[i] = newRandomArg(random, paramType);
      }
    }
    return args;
  }
  
  static Object[] newFilterArgs(Random random, TokenStream stream, Class<?>[] paramTypes) {
    Object[] args = new Object[paramTypes.length];
    for (int i = 0; i < args.length; i++) {
      Class<?> paramType = paramTypes[i];
      if (paramType == TokenStream.class) {
        args[i] = stream;
      } else if (paramType == CommonGramsFilter.class) {
        // TODO: fix this one, thats broken: CommonGramsQueryFilter takes this one explicitly
        args[i] = new CommonGramsFilter(TEST_VERSION_CURRENT, stream, newRandomArg(random, CharArraySet.class));
      } else {
        args[i] = newRandomArg(random, paramType);
      }
    }
    return args;
  }

  static class MockRandomAnalyzer extends Analyzer {
    final long seed;
    
    MockRandomAnalyzer(long seed) {
      this.seed = seed;
    }

    public boolean offsetsAreCorrect() {
      // nocommit: can we not do the full chain here!?
      Random random = new Random(seed);
      TokenizerSpec tokenizerSpec = newTokenizer(random, new StringReader(""));
      TokenFilterSpec filterSpec = newFilterChain(random, tokenizerSpec.tokenizer, tokenizerSpec.offsetsAreCorrect);
      return filterSpec.offsetsAreCorrect;
    }
    
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Random random = new Random(seed);
      TokenizerSpec tokenizerSpec = newTokenizer(random, reader);
      TokenFilterSpec filterSpec = newFilterChain(random, tokenizerSpec.tokenizer, tokenizerSpec.offsetsAreCorrect);
      return new TokenStreamComponents(tokenizerSpec.tokenizer, filterSpec.stream);
    }

    @Override
    protected Reader initReader(Reader reader) {
      Random random = new Random(seed);
      CharFilterSpec charfilterspec = newCharFilterChain(random, reader);
      return charfilterspec.reader;
    }

    @Override
    public String toString() {
      Random random = new Random(seed);
      StringBuilder sb = new StringBuilder();
      CharFilterSpec charFilterSpec = newCharFilterChain(random, new StringReader(""));
      sb.append("\ncharfilters=");
      sb.append(charFilterSpec.toString);
      // intentional: initReader gets its own separate random
      random = new Random(seed);
      TokenizerSpec tokenizerSpec = newTokenizer(random, charFilterSpec.reader);
      sb.append("\n");
      sb.append("tokenizer=");
      sb.append(tokenizerSpec.toString);
      TokenFilterSpec tokenFilterSpec = newFilterChain(random, tokenizerSpec.tokenizer, tokenizerSpec.offsetsAreCorrect);
      sb.append("\n");
      sb.append("filters=");
      sb.append(tokenFilterSpec.toString);
      sb.append("\n");
      sb.append("offsetsAreCorrect=" + tokenFilterSpec.offsetsAreCorrect);
      return sb.toString();
    }
    
    private <T> T createComponent(Constructor<T> ctor, Object[] args, StringBuilder descr) {
      try {
        final T instance = ctor.newInstance(args);
        /*
        if (descr.length() > 0) {
          descr.append(",");
        }
        */
        descr.append("\n  ");
        descr.append(ctor.getDeclaringClass().getName());
        String params = Arrays.toString(args);
        params = params.substring(1, params.length()-1);
        descr.append("(").append(params).append(")");
        return instance;
      } catch (InvocationTargetException ite) {
        final Throwable cause = ite.getCause();
        if (cause instanceof IllegalArgumentException ||
            cause instanceof UnsupportedOperationException) {
          // thats ok, ignore
          if (VERBOSE) {
            System.err.println("Ignoring IAE/UOE from ctor:");
            cause.printStackTrace(System.err);
          }
        } else {
          Rethrow.rethrow(cause);
        }
      } catch (IllegalAccessException iae) {
        Rethrow.rethrow(iae);
      } catch (InstantiationException ie) {
        Rethrow.rethrow(ie);
      }
      return null; // no success
    }
    
    // create a new random tokenizer from classpath
    private TokenizerSpec newTokenizer(Random random, Reader reader) {
      TokenizerSpec spec = new TokenizerSpec();
      while (spec.tokenizer == null) {
        final Constructor<? extends Tokenizer> ctor = tokenizers.get(random.nextInt(tokenizers.size()));
        final StringBuilder descr = new StringBuilder();
        final CheckThatYouDidntReadAnythingReaderWrapper wrapper = new CheckThatYouDidntReadAnythingReaderWrapper(reader);
        final Object args[] = newTokenizerArgs(random, wrapper, ctor.getParameterTypes());
        spec.tokenizer = createComponent(ctor, args, descr);
        if (brokenOffsetsComponents.contains(ctor.getDeclaringClass())) {
          spec.offsetsAreCorrect = false;
        }
        if (spec.tokenizer == null) {
          assertFalse(ctor.getDeclaringClass().getName() + " has read something in ctor but failed with UOE/IAE", wrapper.readSomething);
        }
        spec.toString = descr.toString();
      }
      return spec;
    }
    
    private CharFilterSpec newCharFilterChain(Random random, Reader reader) {
      CharFilterSpec spec = new CharFilterSpec();
      spec.reader = reader;
      StringBuilder descr = new StringBuilder();
      int numFilters = random.nextInt(3);
      for (int i = 0; i < numFilters; i++) {
        while (true) {
          final Constructor<? extends CharStream> ctor = charfilters.get(random.nextInt(charfilters.size()));
          final Object args[] = newCharFilterArgs(random, spec.reader, ctor.getParameterTypes());
          reader = createComponent(ctor, args, descr);
          if (reader != null) {
            spec.reader = reader;
            break;
          }
        }
      }
      spec.toString = descr.toString();
      return spec;
    }
    
    private TokenFilterSpec newFilterChain(Random random, Tokenizer tokenizer, boolean offsetsAreCorrect) {
      TokenFilterSpec spec = new TokenFilterSpec();
      spec.offsetsAreCorrect = offsetsAreCorrect;
      spec.stream = tokenizer;
      StringBuilder descr = new StringBuilder();
      int numFilters = random.nextInt(5);
      for (int i = 0; i < numFilters; i++) {

        // Insert ValidatingTF after each stage so we can
        // catch problems right after the TF that "caused"
        // them:
        spec.stream = new ValidatingTokenFilter(spec.stream, "stage " + i, spec.offsetsAreCorrect);

        while (true) {
          final Constructor<? extends TokenFilter> ctor = tokenfilters.get(random.nextInt(tokenfilters.size()));
          final Object args[] = newFilterArgs(random, spec.stream, ctor.getParameterTypes());
          final TokenFilter flt = createComponent(ctor, args, descr);
          if (flt != null) {
            if (brokenOffsetsComponents.contains(ctor.getDeclaringClass())) {
              spec.offsetsAreCorrect = false;
            }
            spec.stream = flt;
            break;
          }
        }
      }

      // Insert ValidatingTF after each stage so we can
      // catch problems right after the TF that "caused"
      // them:
      spec.stream = new ValidatingTokenFilter(spec.stream, "last stage", spec.offsetsAreCorrect);

      spec.toString = descr.toString();
      return spec;
    }
  }
  
  static final class CheckThatYouDidntReadAnythingReaderWrapper extends CharFilter {
    boolean readSomething = false;
    
    CheckThatYouDidntReadAnythingReaderWrapper(Reader in) {
      super(CharReader.get(in));
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      readSomething = true;
      return super.read(cbuf, off, len);
    }

    @Override
    public int read() throws IOException {
      readSomething = true;
      return super.read();
    }

    @Override
    public int read(CharBuffer target) throws IOException {
      readSomething = true;
      return super.read(target);
    }

    @Override
    public int read(char[] cbuf) throws IOException {
      readSomething = true;
      return super.read(cbuf);
    }

    @Override
    public long skip(long n) throws IOException {
      readSomething = true;
      return super.skip(n);
    }
  }
  
  static class TokenizerSpec {
    Tokenizer tokenizer;
    String toString;
    boolean offsetsAreCorrect = true;
  }
  
  static class TokenFilterSpec {
    TokenStream stream;
    String toString;
    boolean offsetsAreCorrect = true;
  }
  
  static class CharFilterSpec {
    Reader reader;
    String toString;
  }
  
  public void testRandomChains() throws Throwable {
    int numIterations = atLeast(20);
    for (int i = 0; i < numIterations; i++) {
      MockRandomAnalyzer a = new MockRandomAnalyzer(random.nextLong());
      // nocommit: wrap the uncaught handler with our own that prints the analyzer
      if (true || VERBOSE) {
        System.out.println("Creating random analyzer:" + a);
      }
      try {
        checkRandomData(random, a, 1000, 20, false,
                        false /* We already validate our own offsets... */);
      } catch (Throwable e) {
        System.err.println("Exception from random analyzer: " + a);
        throw e;
      }
    }
  }
}
