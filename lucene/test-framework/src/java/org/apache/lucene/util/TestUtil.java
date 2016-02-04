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
package org.apache.lucene.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.codecs.blockterms.LuceneFixedGap;
import org.apache.lucene.codecs.blocktreeords.BlockTreeOrdsPostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.apache.lucene.codecs.lucene54.Lucene54DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.FilteredQuery.FilterStrategy;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Assert;


/**
 * General utility methods for Lucene unit tests. 
 */
public final class TestUtil {
  private TestUtil() {
    //
  }

  /** 
   * Convenience method unzipping zipName into destDir, cleaning up 
   * destDir first.
   * Closes the given InputStream after extracting! 
   */
  public static void unzip(InputStream in, Path destDir) throws IOException {
    in = new BufferedInputStream(in);
    IOUtils.rm(destDir);
    Files.createDirectory(destDir);

    try (ZipInputStream zipInput = new ZipInputStream(in)) {
      ZipEntry entry;
      byte[] buffer = new byte[8192];
      while ((entry = zipInput.getNextEntry()) != null) {
        Path targetFile = destDir.resolve(entry.getName());
        
        // be on the safe side: do not rely on that directories are always extracted
        // before their children (although this makes sense, but is it guaranteed?)
        Files.createDirectories(targetFile.getParent());
        if (!entry.isDirectory()) {
          OutputStream out = Files.newOutputStream(targetFile);
          int len;
          while((len = zipInput.read(buffer)) >= 0) {
            out.write(buffer, 0, len);
          }
          out.close();
        }
        zipInput.closeEntry();
      }
    }
  }
  
  /** 
   * Checks that the provided iterator is well-formed.
   * <ul>
   *   <li>is read-only: does not allow {@code remove}
   *   <li>returns {@code expectedSize} number of elements
   *   <li>does not return null elements, unless {@code allowNull} is true.
   *   <li>throws NoSuchElementException if {@code next} is called
   *       after {@code hasNext} returns false. 
   * </ul>
   */
  public static <T> void checkIterator(Iterator<T> iterator, long expectedSize, boolean allowNull) {
    for (long i = 0; i < expectedSize; i++) {
      boolean hasNext = iterator.hasNext();
      assert hasNext;
      T v = iterator.next();
      assert allowNull || v != null;
      try {
        iterator.remove();
        throw new AssertionError("broken iterator (supports remove): " + iterator);
      } catch (UnsupportedOperationException expected) {
        // ok
      }
    }
    assert !iterator.hasNext();
    try {
      iterator.next();
      throw new AssertionError("broken iterator (allows next() when hasNext==false) " + iterator);
    } catch (NoSuchElementException expected) {
      // ok
    }
  }
  
  /** 
   * Checks that the provided iterator is well-formed.
   * <ul>
   *   <li>is read-only: does not allow {@code remove}
   *   <li>does not return null elements.
   *   <li>throws NoSuchElementException if {@code next} is called
   *       after {@code hasNext} returns false. 
   * </ul>
   */
  public static <T> void checkIterator(Iterator<T> iterator) {
    while (iterator.hasNext()) {
      T v = iterator.next();
      assert v != null;
      try {
        iterator.remove();
        throw new AssertionError("broken iterator (supports remove): " + iterator);
      } catch (UnsupportedOperationException expected) {
        // ok
      }
    }
    try {
      iterator.next();
      throw new AssertionError("broken iterator (allows next() when hasNext==false) " + iterator);
    } catch (NoSuchElementException expected) {
      // ok
    }
  }

  /**
   * Checks that the provided collection is read-only.
   * @see #checkIterator(Iterator)
   */
  public static <T> void checkReadOnly(Collection<T> coll) {
    int size = 0;
    for (Iterator<?> it = coll.iterator(); it.hasNext(); ) {
      it.next();
      size += 1;
    }
    if (size != coll.size()) {
      throw new AssertionError("broken collection, reported size is "
          + coll.size() + " but iterator has " + size + " elements: " + coll);
    }

    if (coll.isEmpty() == false) {
      try {
        coll.remove(coll.iterator().next());
        throw new AssertionError("broken collection (supports remove): " + coll);
      } catch (UnsupportedOperationException e) {
        // ok
      }
    }

    try {
      coll.add(null);
      throw new AssertionError("broken collection (supports add): " + coll);
    } catch (UnsupportedOperationException e) {
      // ok
    }

    try {
      coll.addAll(Collections.<T>singleton(null));
      throw new AssertionError("broken collection (supports addAll): " + coll);
    } catch (UnsupportedOperationException e) {
      // ok
    }

    checkIterator(coll.iterator());
  }

  public static void syncConcurrentMerges(IndexWriter writer) {
    syncConcurrentMerges(writer.getConfig().getMergeScheduler());
  }

  public static void syncConcurrentMerges(MergeScheduler ms) {
    if (ms instanceof ConcurrentMergeScheduler)
      ((ConcurrentMergeScheduler) ms).sync();
  }

  /** This runs the CheckIndex tool on the index in.  If any
   *  issues are hit, a RuntimeException is thrown; else,
   *  true is returned. */
  public static CheckIndex.Status checkIndex(Directory dir) throws IOException {
    return checkIndex(dir, true);
  }

  public static CheckIndex.Status checkIndex(Directory dir, boolean crossCheckTermVectors) throws IOException {
    return checkIndex(dir, crossCheckTermVectors, false);
  }

  /** If failFast is true, then throw the first exception when index corruption is hit, instead of moving on to other fields/segments to
   *  look for any other corruption.  */
  public static CheckIndex.Status checkIndex(Directory dir, boolean crossCheckTermVectors, boolean failFast) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    // TODO: actually use the dir's locking, unless test uses a special method?
    // some tests e.g. exception tests become much more complicated if they have to close the writer
    try (CheckIndex checker = new CheckIndex(dir, NoLockFactory.INSTANCE.obtainLock(dir, "bogus"))) {
      checker.setCrossCheckTermVectors(crossCheckTermVectors);
      checker.setFailFast(failFast);
      checker.setInfoStream(new PrintStream(bos, false, IOUtils.UTF_8), false);
      CheckIndex.Status indexStatus = checker.checkIndex(null);
      
      if (indexStatus == null || indexStatus.clean == false) {
        System.out.println("CheckIndex failed");
        System.out.println(bos.toString(IOUtils.UTF_8));
        throw new RuntimeException("CheckIndex failed");
      } else {
        if (LuceneTestCase.INFOSTREAM) {
          System.out.println(bos.toString(IOUtils.UTF_8));
        }
        return indexStatus;
      }
    }
  }
  
  /** This runs the CheckIndex tool on the Reader.  If any
   *  issues are hit, a RuntimeException is thrown */
  public static void checkReader(IndexReader reader) throws IOException {
    for (LeafReaderContext context : reader.leaves()) {
      checkReader(context.reader(), true);
    }
  }
  
  public static void checkReader(LeafReader reader, boolean crossCheckTermVectors) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    PrintStream infoStream = new PrintStream(bos, false, IOUtils.UTF_8);

    final CodecReader codecReader;
    if (reader instanceof CodecReader) {
      codecReader = (CodecReader) reader;
      reader.checkIntegrity();
    } else {
      codecReader = SlowCodecReaderWrapper.wrap(reader);
    }
    CheckIndex.testLiveDocs(codecReader, infoStream, true);
    CheckIndex.testFieldInfos(codecReader, infoStream, true);
    CheckIndex.testFieldNorms(codecReader, infoStream, true);
    CheckIndex.testPostings(codecReader, infoStream, false, true);
    CheckIndex.testStoredFields(codecReader, infoStream, true);
    CheckIndex.testTermVectors(codecReader, infoStream, false, crossCheckTermVectors, true);
    CheckIndex.testDocValues(codecReader, infoStream, true);
    
    // some checks really against the reader API
    checkReaderSanity(reader);
    
    if (LuceneTestCase.INFOSTREAM) {
      System.out.println(bos.toString(IOUtils.UTF_8));
    }
    
    LeafReader unwrapped = FilterLeafReader.unwrap(reader);
    if (unwrapped instanceof SegmentReader) {
      SegmentReader sr = (SegmentReader) unwrapped;
      long bytesUsed = sr.ramBytesUsed(); 
      if (sr.ramBytesUsed() < 0) {
        throw new IllegalStateException("invalid ramBytesUsed for reader: " + bytesUsed);
      }
      assert Accountables.toString(sr) != null;
    }
  }
  
  // used by TestUtil.checkReader to check some things really unrelated to the index,
  // just looking for bugs in indexreader implementations.
  private static void checkReaderSanity(LeafReader reader) throws IOException {
    for (FieldInfo info : reader.getFieldInfos()) {
      
      // reader shouldn't return normValues if the field does not have them
      if (!info.hasNorms()) {
        if (reader.getNormValues(info.name) != null) {
          throw new RuntimeException("field: " + info.name + " should omit norms but has them!");
        }
      }
      
      // reader shouldn't return docValues if the field does not have them
      // reader shouldn't return multiple docvalues types for the same field.
      switch(info.getDocValuesType()) {
        case NONE:
          if (reader.getBinaryDocValues(info.name) != null ||
              reader.getNumericDocValues(info.name) != null ||
              reader.getSortedDocValues(info.name) != null || 
              reader.getSortedSetDocValues(info.name) != null || 
              reader.getDocsWithField(info.name) != null) {
            throw new RuntimeException("field: " + info.name + " has docvalues but should omit them!");
          }
          break;
        case SORTED:
          if (reader.getBinaryDocValues(info.name) != null ||
              reader.getNumericDocValues(info.name) != null ||
              reader.getSortedNumericDocValues(info.name) != null ||
              reader.getSortedSetDocValues(info.name) != null) {
            throw new RuntimeException(info.name + " returns multiple docvalues types!");
          }
          break;
        case SORTED_NUMERIC:
          if (reader.getBinaryDocValues(info.name) != null ||
              reader.getNumericDocValues(info.name) != null ||
              reader.getSortedSetDocValues(info.name) != null ||
              reader.getSortedDocValues(info.name) != null) {
            throw new RuntimeException(info.name + " returns multiple docvalues types!");
          }
          break;
        case SORTED_SET:
          if (reader.getBinaryDocValues(info.name) != null ||
              reader.getNumericDocValues(info.name) != null ||
              reader.getSortedNumericDocValues(info.name) != null ||
              reader.getSortedDocValues(info.name) != null) {
            throw new RuntimeException(info.name + " returns multiple docvalues types!");
          }
          break;
        case BINARY:
          if (reader.getNumericDocValues(info.name) != null ||
              reader.getSortedDocValues(info.name) != null ||
              reader.getSortedNumericDocValues(info.name) != null ||
              reader.getSortedSetDocValues(info.name) != null) {
            throw new RuntimeException(info.name + " returns multiple docvalues types!");
          }
          break;
        case NUMERIC:
          if (reader.getBinaryDocValues(info.name) != null ||
              reader.getSortedDocValues(info.name) != null ||
              reader.getSortedNumericDocValues(info.name) != null ||
              reader.getSortedSetDocValues(info.name) != null) {
            throw new RuntimeException(info.name + " returns multiple docvalues types!");
          }
          break;
        default:
          throw new AssertionError();
      }
    }
  }

  /** start and end are BOTH inclusive */
  public static int nextInt(Random r, int start, int end) {
    return RandomInts.randomIntBetween(r, start, end);
  }

  /** start and end are BOTH inclusive */
  public static long nextLong(Random r, long start, long end) {
    assert end >= start;
    final BigInteger range = BigInteger.valueOf(end).add(BigInteger.valueOf(1)).subtract(BigInteger.valueOf(start));
    if (range.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) <= 0) {
      return start + r.nextInt(range.intValue());
    } else {
      // probably not evenly distributed when range is large, but OK for tests
      final BigInteger augend = new BigDecimal(range).multiply(new BigDecimal(r.nextDouble())).toBigInteger();
      final long result = BigInteger.valueOf(start).add(augend).longValue();
      assert result >= start;
      assert result <= end;
      return result;
    }
  }

  public static String randomSimpleString(Random r, int maxLength) {
    return randomSimpleString(r, 0, maxLength);
  }
  
  public static String randomSimpleString(Random r, int minLength, int maxLength) {
    final int end = nextInt(r, minLength, maxLength);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      buffer[i] = (char) TestUtil.nextInt(r, 'a', 'z');
    }
    return new String(buffer, 0, end);
  }

  public static String randomSimpleStringRange(Random r, char minChar, char maxChar, int maxLength) {
    final int end = nextInt(r, 0, maxLength);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      buffer[i] = (char) TestUtil.nextInt(r, minChar, maxChar);
    }
    return new String(buffer, 0, end);
  }

  public static String randomSimpleString(Random r) {
    return randomSimpleString(r, 0, 10);
  }

  /** Returns random string, including full unicode range. */
  public static String randomUnicodeString(Random r) {
    return randomUnicodeString(r, 20);
  }

  /**
   * Returns a random string up to a certain length.
   */
  public static String randomUnicodeString(Random r, int maxLength) {
    final int end = nextInt(r, 0, maxLength);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    randomFixedLengthUnicodeString(r, buffer, 0, buffer.length);
    return new String(buffer, 0, end);
  }

  /**
   * Fills provided char[] with valid random unicode code
   * unit sequence.
   */
  public static void randomFixedLengthUnicodeString(Random random, char[] chars, int offset, int length) {
    int i = offset;
    final int end = offset + length;
    while(i < end) {
      final int t = random.nextInt(5);
      if (0 == t && i < length - 1) {
        // Make a surrogate pair
        // High surrogate
        chars[i++] = (char) nextInt(random, 0xd800, 0xdbff);
        // Low surrogate
        chars[i++] = (char) nextInt(random, 0xdc00, 0xdfff);
      } else if (t <= 1) {
        chars[i++] = (char) random.nextInt(0x80);
      } else if (2 == t) {
        chars[i++] = (char) nextInt(random, 0x80, 0x7ff);
      } else if (3 == t) {
        chars[i++] = (char) nextInt(random, 0x800, 0xd7ff);
      } else if (4 == t) {
        chars[i++] = (char) nextInt(random, 0xe000, 0xffff);
      }
    }
  }
  
  /**
   * Returns a String thats "regexpish" (contains lots of operators typically found in regular expressions)
   * If you call this enough times, you might get a valid regex!
   */
  public static String randomRegexpishString(Random r) {
    return randomRegexpishString(r, 20);
  }

  /**
   * Maximum recursion bound for '+' and '*' replacements in
   * {@link #randomRegexpishString(Random, int)}.
   */
  private final static int maxRecursionBound = 5;

  /**
   * Operators for {@link #randomRegexpishString(Random, int)}.
   */
  private final static List<String> ops = Arrays.asList(
      ".", "?", 
      "{0," + maxRecursionBound + "}",  // bounded replacement for '*'
      "{1," + maxRecursionBound + "}",  // bounded replacement for '+'
      "(",
      ")",
      "-",
      "[",
      "]",
      "|"
  );

  /**
   * Returns a String thats "regexpish" (contains lots of operators typically found in regular expressions)
   * If you call this enough times, you might get a valid regex!
   * 
   * <P>Note: to avoid practically endless backtracking patterns we replace asterisk and plus
   * operators with bounded repetitions. See LUCENE-4111 for more info.
   * 
   * @param maxLength A hint about maximum length of the regexpish string. It may be exceeded by a few characters.
   */
  public static String randomRegexpishString(Random r, int maxLength) {
    final StringBuilder regexp = new StringBuilder(maxLength);
    for (int i = nextInt(r, 0, maxLength); i > 0; i--) {
      if (r.nextBoolean()) {
        regexp.append((char) RandomInts.randomIntBetween(r, 'a', 'z'));
      } else {
        regexp.append(RandomPicks.randomFrom(r, ops));
      }
    }
    return regexp.toString();
  }

  private static final String[] HTML_CHAR_ENTITIES = {
      "AElig", "Aacute", "Acirc", "Agrave", "Alpha", "AMP", "Aring", "Atilde",
      "Auml", "Beta", "COPY", "Ccedil", "Chi", "Dagger", "Delta", "ETH",
      "Eacute", "Ecirc", "Egrave", "Epsilon", "Eta", "Euml", "Gamma", "GT",
      "Iacute", "Icirc", "Igrave", "Iota", "Iuml", "Kappa", "Lambda", "LT",
      "Mu", "Ntilde", "Nu", "OElig", "Oacute", "Ocirc", "Ograve", "Omega",
      "Omicron", "Oslash", "Otilde", "Ouml", "Phi", "Pi", "Prime", "Psi",
      "QUOT", "REG", "Rho", "Scaron", "Sigma", "THORN", "Tau", "Theta",
      "Uacute", "Ucirc", "Ugrave", "Upsilon", "Uuml", "Xi", "Yacute", "Yuml",
      "Zeta", "aacute", "acirc", "acute", "aelig", "agrave", "alefsym",
      "alpha", "amp", "and", "ang", "apos", "aring", "asymp", "atilde",
      "auml", "bdquo", "beta", "brvbar", "bull", "cap", "ccedil", "cedil",
      "cent", "chi", "circ", "clubs", "cong", "copy", "crarr", "cup",
      "curren", "dArr", "dagger", "darr", "deg", "delta", "diams", "divide",
      "eacute", "ecirc", "egrave", "empty", "emsp", "ensp", "epsilon",
      "equiv", "eta", "eth", "euml", "euro", "exist", "fnof", "forall",
      "frac12", "frac14", "frac34", "frasl", "gamma", "ge", "gt", "hArr",
      "harr", "hearts", "hellip", "iacute", "icirc", "iexcl", "igrave",
      "image", "infin", "int", "iota", "iquest", "isin", "iuml", "kappa",
      "lArr", "lambda", "lang", "laquo", "larr", "lceil", "ldquo", "le",
      "lfloor", "lowast", "loz", "lrm", "lsaquo", "lsquo", "lt", "macr",
      "mdash", "micro", "middot", "minus", "mu", "nabla", "nbsp", "ndash",
      "ne", "ni", "not", "notin", "nsub", "ntilde", "nu", "oacute", "ocirc",
      "oelig", "ograve", "oline", "omega", "omicron", "oplus", "or", "ordf",
      "ordm", "oslash", "otilde", "otimes", "ouml", "para", "part", "permil",
      "perp", "phi", "pi", "piv", "plusmn", "pound", "prime", "prod", "prop",
      "psi", "quot", "rArr", "radic", "rang", "raquo", "rarr", "rceil",
      "rdquo", "real", "reg", "rfloor", "rho", "rlm", "rsaquo", "rsquo",
      "sbquo", "scaron", "sdot", "sect", "shy", "sigma", "sigmaf", "sim",
      "spades", "sub", "sube", "sum", "sup", "sup1", "sup2", "sup3", "supe",
      "szlig", "tau", "there4", "theta", "thetasym", "thinsp", "thorn",
      "tilde", "times", "trade", "uArr", "uacute", "uarr", "ucirc", "ugrave",
      "uml", "upsih", "upsilon", "uuml", "weierp", "xi", "yacute", "yen",
      "yuml", "zeta", "zwj", "zwnj"
  };
  
  public static String randomHtmlishString(Random random, int numElements) {
    final int end = nextInt(random, 0, numElements);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < end; i++) {
      int val = random.nextInt(25);
      switch(val) {
        case 0: sb.append("<p>"); break;
        case 1: {
          sb.append("<");
          sb.append("    ".substring(nextInt(random, 0, 4)));
          sb.append(randomSimpleString(random));
          for (int j = 0 ; j < nextInt(random, 0, 10) ; ++j) {
            sb.append(' ');
            sb.append(randomSimpleString(random));
            sb.append(" ".substring(nextInt(random, 0, 1)));
            sb.append('=');
            sb.append(" ".substring(nextInt(random, 0, 1)));
            sb.append("\"".substring(nextInt(random, 0, 1)));
            sb.append(randomSimpleString(random));
            sb.append("\"".substring(nextInt(random, 0, 1)));
          }
          sb.append("    ".substring(nextInt(random, 0, 4)));
          sb.append("/".substring(nextInt(random, 0, 1)));
          sb.append(">".substring(nextInt(random, 0, 1)));
          break;
        }
        case 2: {
          sb.append("</");
          sb.append("    ".substring(nextInt(random, 0, 4)));
          sb.append(randomSimpleString(random));
          sb.append("    ".substring(nextInt(random, 0, 4)));
          sb.append(">".substring(nextInt(random, 0, 1)));
          break;
        }
        case 3: sb.append(">"); break;
        case 4: sb.append("</p>"); break;
        case 5: sb.append("<!--"); break;
        case 6: sb.append("<!--#"); break;
        case 7: sb.append("<script><!-- f('"); break;
        case 8: sb.append("</script>"); break;
        case 9: sb.append("<?"); break;
        case 10: sb.append("?>"); break;
        case 11: sb.append("\""); break;
        case 12: sb.append("\\\""); break;
        case 13: sb.append("'"); break;
        case 14: sb.append("\\'"); break;
        case 15: sb.append("-->"); break;
        case 16: {
          sb.append("&");
          switch(nextInt(random, 0, 2)) {
            case 0: sb.append(randomSimpleString(random)); break;
            case 1: sb.append(HTML_CHAR_ENTITIES[random.nextInt(HTML_CHAR_ENTITIES.length)]); break;
          }
          sb.append(";".substring(nextInt(random, 0, 1)));
          break;
        }
        case 17: {
          sb.append("&#");
          if (0 == nextInt(random, 0, 1)) {
            sb.append(nextInt(random, 0, Integer.MAX_VALUE - 1));
            sb.append(";".substring(nextInt(random, 0, 1)));
          }
          break;
        } 
        case 18: {
          sb.append("&#x");
          if (0 == nextInt(random, 0, 1)) {
            sb.append(Integer.toString(nextInt(random, 0, Integer.MAX_VALUE - 1), 16));
            sb.append(";".substring(nextInt(random, 0, 1)));
          }
          break;
        }
          
        case 19: sb.append(";"); break;
        case 20: sb.append(nextInt(random, 0, Integer.MAX_VALUE - 1)); break;
        case 21: sb.append("\n"); break;
        case 22: sb.append("          ".substring(nextInt(random, 0, 10))); break;
        case 23: {
          sb.append("<");
          if (0 == nextInt(random, 0, 3)) {
            sb.append("          ".substring(nextInt(random, 1, 10)));
          }
          if (0 == nextInt(random, 0, 1)) {
            sb.append("/");
            if (0 == nextInt(random, 0, 3)) {
              sb.append("          ".substring(nextInt(random, 1, 10)));
            }
          }
          switch (nextInt(random, 0, 3)) {
            case 0: sb.append(randomlyRecaseCodePoints(random, "script")); break;
            case 1: sb.append(randomlyRecaseCodePoints(random, "style")); break;
            case 2: sb.append(randomlyRecaseCodePoints(random, "br")); break;
            // default: append nothing
          }
          sb.append(">".substring(nextInt(random, 0, 1)));
          break;
        }
        default: sb.append(randomSimpleString(random));
      }
    }
    return sb.toString();
  }

  /**
   * Randomly upcases, downcases, or leaves intact each code point in the given string
   */
  public static String randomlyRecaseCodePoints(Random random, String str) {
    StringBuilder builder = new StringBuilder();
    int pos = 0;
    while (pos < str.length()) {
      int codePoint = str.codePointAt(pos);
      pos += Character.charCount(codePoint);
      switch (nextInt(random, 0, 2)) {
        case 0: builder.appendCodePoint(Character.toUpperCase(codePoint)); break;
        case 1: builder.appendCodePoint(Character.toLowerCase(codePoint)); break;
        case 2: builder.appendCodePoint(codePoint); // leave intact
      }
    }
    return builder.toString();
  }

  private static final int[] blockStarts = {
    0x0000, 0x0080, 0x0100, 0x0180, 0x0250, 0x02B0, 0x0300, 0x0370, 0x0400, 
    0x0500, 0x0530, 0x0590, 0x0600, 0x0700, 0x0750, 0x0780, 0x07C0, 0x0800, 
    0x0900, 0x0980, 0x0A00, 0x0A80, 0x0B00, 0x0B80, 0x0C00, 0x0C80, 0x0D00, 
    0x0D80, 0x0E00, 0x0E80, 0x0F00, 0x1000, 0x10A0, 0x1100, 0x1200, 0x1380, 
    0x13A0, 0x1400, 0x1680, 0x16A0, 0x1700, 0x1720, 0x1740, 0x1760, 0x1780, 
    0x1800, 0x18B0, 0x1900, 0x1950, 0x1980, 0x19E0, 0x1A00, 0x1A20, 0x1B00, 
    0x1B80, 0x1C00, 0x1C50, 0x1CD0, 0x1D00, 0x1D80, 0x1DC0, 0x1E00, 0x1F00, 
    0x2000, 0x2070, 0x20A0, 0x20D0, 0x2100, 0x2150, 0x2190, 0x2200, 0x2300, 
    0x2400, 0x2440, 0x2460, 0x2500, 0x2580, 0x25A0, 0x2600, 0x2700, 0x27C0, 
    0x27F0, 0x2800, 0x2900, 0x2980, 0x2A00, 0x2B00, 0x2C00, 0x2C60, 0x2C80, 
    0x2D00, 0x2D30, 0x2D80, 0x2DE0, 0x2E00, 0x2E80, 0x2F00, 0x2FF0, 0x3000, 
    0x3040, 0x30A0, 0x3100, 0x3130, 0x3190, 0x31A0, 0x31C0, 0x31F0, 0x3200, 
    0x3300, 0x3400, 0x4DC0, 0x4E00, 0xA000, 0xA490, 0xA4D0, 0xA500, 0xA640, 
    0xA6A0, 0xA700, 0xA720, 0xA800, 0xA830, 0xA840, 0xA880, 0xA8E0, 0xA900, 
    0xA930, 0xA960, 0xA980, 0xAA00, 0xAA60, 0xAA80, 0xABC0, 0xAC00, 0xD7B0, 
    0xE000, 0xF900, 0xFB00, 0xFB50, 0xFE00, 0xFE10, 
    0xFE20, 0xFE30, 0xFE50, 0xFE70, 0xFF00, 0xFFF0, 
    0x10000, 0x10080, 0x10100, 0x10140, 0x10190, 0x101D0, 0x10280, 0x102A0, 
    0x10300, 0x10330, 0x10380, 0x103A0, 0x10400, 0x10450, 0x10480, 0x10800, 
    0x10840, 0x10900, 0x10920, 0x10A00, 0x10A60, 0x10B00, 0x10B40, 0x10B60, 
    0x10C00, 0x10E60, 0x11080, 0x12000, 0x12400, 0x13000, 0x1D000, 0x1D100, 
    0x1D200, 0x1D300, 0x1D360, 0x1D400, 0x1F000, 0x1F030, 0x1F100, 0x1F200, 
    0x20000, 0x2A700, 0x2F800, 0xE0000, 0xE0100, 0xF0000, 0x100000
  };
  
  private static final int[] blockEnds = {
    0x007F, 0x00FF, 0x017F, 0x024F, 0x02AF, 0x02FF, 0x036F, 0x03FF, 0x04FF, 
    0x052F, 0x058F, 0x05FF, 0x06FF, 0x074F, 0x077F, 0x07BF, 0x07FF, 0x083F, 
    0x097F, 0x09FF, 0x0A7F, 0x0AFF, 0x0B7F, 0x0BFF, 0x0C7F, 0x0CFF, 0x0D7F, 
    0x0DFF, 0x0E7F, 0x0EFF, 0x0FFF, 0x109F, 0x10FF, 0x11FF, 0x137F, 0x139F, 
    0x13FF, 0x167F, 0x169F, 0x16FF, 0x171F, 0x173F, 0x175F, 0x177F, 0x17FF, 
    0x18AF, 0x18FF, 0x194F, 0x197F, 0x19DF, 0x19FF, 0x1A1F, 0x1AAF, 0x1B7F, 
    0x1BBF, 0x1C4F, 0x1C7F, 0x1CFF, 0x1D7F, 0x1DBF, 0x1DFF, 0x1EFF, 0x1FFF, 
    0x206F, 0x209F, 0x20CF, 0x20FF, 0x214F, 0x218F, 0x21FF, 0x22FF, 0x23FF, 
    0x243F, 0x245F, 0x24FF, 0x257F, 0x259F, 0x25FF, 0x26FF, 0x27BF, 0x27EF, 
    0x27FF, 0x28FF, 0x297F, 0x29FF, 0x2AFF, 0x2BFF, 0x2C5F, 0x2C7F, 0x2CFF, 
    0x2D2F, 0x2D7F, 0x2DDF, 0x2DFF, 0x2E7F, 0x2EFF, 0x2FDF, 0x2FFF, 0x303F, 
    0x309F, 0x30FF, 0x312F, 0x318F, 0x319F, 0x31BF, 0x31EF, 0x31FF, 0x32FF, 
    0x33FF, 0x4DBF, 0x4DFF, 0x9FFF, 0xA48F, 0xA4CF, 0xA4FF, 0xA63F, 0xA69F, 
    0xA6FF, 0xA71F, 0xA7FF, 0xA82F, 0xA83F, 0xA87F, 0xA8DF, 0xA8FF, 0xA92F, 
    0xA95F, 0xA97F, 0xA9DF, 0xAA5F, 0xAA7F, 0xAADF, 0xABFF, 0xD7AF, 0xD7FF, 
    0xF8FF, 0xFAFF, 0xFB4F, 0xFDFF, 0xFE0F, 0xFE1F, 
    0xFE2F, 0xFE4F, 0xFE6F, 0xFEFF, 0xFFEF, 0xFFFF, 
    0x1007F, 0x100FF, 0x1013F, 0x1018F, 0x101CF, 0x101FF, 0x1029F, 0x102DF, 
    0x1032F, 0x1034F, 0x1039F, 0x103DF, 0x1044F, 0x1047F, 0x104AF, 0x1083F, 
    0x1085F, 0x1091F, 0x1093F, 0x10A5F, 0x10A7F, 0x10B3F, 0x10B5F, 0x10B7F, 
    0x10C4F, 0x10E7F, 0x110CF, 0x123FF, 0x1247F, 0x1342F, 0x1D0FF, 0x1D1FF, 
    0x1D24F, 0x1D35F, 0x1D37F, 0x1D7FF, 0x1F02F, 0x1F09F, 0x1F1FF, 0x1F2FF, 
    0x2A6DF, 0x2B73F, 0x2FA1F, 0xE007F, 0xE01EF, 0xFFFFF, 0x10FFFF
  };
  
  /** Returns random string of length between 0-20 codepoints, all codepoints within the same unicode block. */
  public static String randomRealisticUnicodeString(Random r) {
    return randomRealisticUnicodeString(r, 20);
  }
  
  /** Returns random string of length up to maxLength codepoints , all codepoints within the same unicode block. */
  public static String randomRealisticUnicodeString(Random r, int maxLength) {
    return randomRealisticUnicodeString(r, 0, maxLength);
  }

  /** Returns random string of length between min and max codepoints, all codepoints within the same unicode block. */
  public static String randomRealisticUnicodeString(Random r, int minLength, int maxLength) {
    final int end = nextInt(r, minLength, maxLength);
    final int block = r.nextInt(blockStarts.length);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < end; i++)
      sb.appendCodePoint(nextInt(r, blockStarts[block], blockEnds[block]));
    return sb.toString();
  }
  
  /** Returns random string, with a given UTF-8 byte length*/
  public static String randomFixedByteLengthUnicodeString(Random r, int length) {
    
    final char[] buffer = new char[length*3];
    int bytes = length;
    int i = 0;
    for (; i < buffer.length && bytes != 0; i++) {
      int t;
      if (bytes >= 4) {
        t = r.nextInt(5);
      } else if (bytes >= 3) {
        t = r.nextInt(4);
      } else if (bytes >= 2) {
        t = r.nextInt(2);
      } else {
        t = 0;
      }
      if (t == 0) {
        buffer[i] = (char) r.nextInt(0x80);
        bytes--;
      } else if (1 == t) {
        buffer[i] = (char) nextInt(r, 0x80, 0x7ff);
        bytes -= 2;
      } else if (2 == t) {
        buffer[i] = (char) nextInt(r, 0x800, 0xd7ff);
        bytes -= 3;
      } else if (3 == t) {
        buffer[i] = (char) nextInt(r, 0xe000, 0xffff);
        bytes -= 3;
      } else if (4 == t) {
        // Make a surrogate pair
        // High surrogate
        buffer[i++] = (char) nextInt(r, 0xd800, 0xdbff);
        // Low surrogate
        buffer[i] = (char) nextInt(r, 0xdc00, 0xdfff);
        bytes -= 4;
      }

    }
    return new String(buffer, 0, i);
  }

  /** Returns a random binary term. */
  public static BytesRef randomBinaryTerm(Random r) {
    int length = r.nextInt(15);
    BytesRef b = new BytesRef(length);
    r.nextBytes(b.bytes);
    b.length = length;
    return b;
  }
  
  /** Return a Codec that can read any of the
   *  default codecs and formats, but always writes in the specified
   *  format. */
  public static Codec alwaysPostingsFormat(final PostingsFormat format) {
    // TODO: we really need for postings impls etc to announce themselves
    // (and maybe their params, too) to infostream on flush and merge.
    // otherwise in a real debugging situation we won't know whats going on!
    if (LuceneTestCase.VERBOSE) {
      System.out.println("forcing postings format to:" + format);
    }
    return new AssertingCodec() {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        return format;
      }
    };
  }
  
  /** Return a Codec that can read any of the
   *  default codecs and formats, but always writes in the specified
   *  format. */
  public static Codec alwaysDocValuesFormat(final DocValuesFormat format) {
    // TODO: we really need for docvalues impls etc to announce themselves
    // (and maybe their params, too) to infostream on flush and merge.
    // otherwise in a real debugging situation we won't know whats going on!
    if (LuceneTestCase.VERBOSE) {
      System.out.println("TestUtil: forcing docvalues format to:" + format);
    }
    return new AssertingCodec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return format;
      }
    };
  }
  
  /** 
   * Returns the actual default codec (e.g. LuceneMNCodec) for this version of Lucene.
   * This may be different than {@link Codec#getDefault()} because that is randomized. 
   */
  public static Codec getDefaultCodec() {
    return new Lucene54Codec();
  }
  
  /** 
   * Returns the actual default postings format (e.g. LuceneMNPostingsFormat for this version of Lucene.
   */
  public static PostingsFormat getDefaultPostingsFormat() {
    return new Lucene50PostingsFormat();
  }
  
  /** 
   * Returns the actual default postings format (e.g. LuceneMNPostingsFormat for this version of Lucene.
   * @lucene.internal this may disappear at any time
   */
  public static PostingsFormat getDefaultPostingsFormat(int minItemsPerBlock, int maxItemsPerBlock) {
    return new Lucene50PostingsFormat(minItemsPerBlock, maxItemsPerBlock);
  }
  
  /** Returns a random postings format that supports term ordinals */
  public static PostingsFormat getPostingsFormatWithOrds(Random r) {
    switch (r.nextInt(2)) {
      case 0: return new LuceneFixedGap();
      case 1: return new BlockTreeOrdsPostingsFormat();
      // TODO: these don't actually support ords!
      //case 2: return new FSTOrdPostingsFormat();
      default: throw new AssertionError();
    }
  }
  
  /** 
   * Returns the actual default docvalues format (e.g. LuceneMNDocValuesFormat for this version of Lucene.
   */
  public static DocValuesFormat getDefaultDocValuesFormat() {
    return new Lucene54DocValuesFormat();
  }

  // TODO: generalize all 'test-checks-for-crazy-codecs' to
  // annotations (LUCENE-3489)
  public static String getPostingsFormat(String field) {
    return getPostingsFormat(Codec.getDefault(), field);
  }
  
  public static String getPostingsFormat(Codec codec, String field) {
    PostingsFormat p = codec.postingsFormat();
    if (p instanceof PerFieldPostingsFormat) {
      return ((PerFieldPostingsFormat)p).getPostingsFormatForField(field).getName();
    } else {
      return p.getName();
    }
  }

  public static String getDocValuesFormat(String field) {
    return getDocValuesFormat(Codec.getDefault(), field);
  }
  
  public static String getDocValuesFormat(Codec codec, String field) {
    DocValuesFormat f = codec.docValuesFormat();
    if (f instanceof PerFieldDocValuesFormat) {
      return ((PerFieldDocValuesFormat) f).getDocValuesFormatForField(field).getName();
    } else {
      return f.getName();
    }
  }

  // TODO: remove this, push this test to Lucene40/Lucene42 codec tests
  public static boolean fieldSupportsHugeBinaryDocValues(String field) {
    String dvFormat = getDocValuesFormat(field);
    if (dvFormat.equals("Lucene40") || dvFormat.equals("Lucene42") || dvFormat.equals("Memory")) {
      return false;
    }
    return true;
  }

  public static boolean anyFilesExceptWriteLock(Directory dir) throws IOException {
    String[] files = dir.listAll();
    if (files.length > 1 || (files.length == 1 && !files[0].equals("write.lock"))) {
      return true;
    } else {
      return false;
    }
  }
  
  public static void addIndexesSlowly(IndexWriter writer, DirectoryReader... readers) throws IOException {
    List<CodecReader> leaves = new ArrayList<>();
    for (DirectoryReader reader : readers) {
      for (LeafReaderContext context : reader.leaves()) {
        leaves.add(SlowCodecReaderWrapper.wrap(context.reader()));
      }
    }
    writer.addIndexes(leaves.toArray(new CodecReader[leaves.size()]));
  }

  /** just tries to configure things to keep the open file
   * count lowish */
  public static void reduceOpenFiles(IndexWriter w) {
    // keep number of open files lowish
    MergePolicy mp = w.getConfig().getMergePolicy();
    if (mp instanceof LogMergePolicy) {
      LogMergePolicy lmp = (LogMergePolicy) mp;
      lmp.setMergeFactor(Math.min(5, lmp.getMergeFactor()));
      lmp.setNoCFSRatio(1.0);
    } else if (mp instanceof TieredMergePolicy) {
      TieredMergePolicy tmp = (TieredMergePolicy) mp;
      tmp.setMaxMergeAtOnce(Math.min(5, tmp.getMaxMergeAtOnce()));
      tmp.setSegmentsPerTier(Math.min(5, tmp.getSegmentsPerTier()));
      tmp.setNoCFSRatio(1.0);
    }
    MergeScheduler ms = w.getConfig().getMergeScheduler();
    if (ms instanceof ConcurrentMergeScheduler) {
      // wtf... shouldnt it be even lower since it's 1 by default?!?!
      ((ConcurrentMergeScheduler) ms).setMaxMergesAndThreads(3, 2);
    }
  }

  /** Checks some basic behaviour of an AttributeImpl
   * @param reflectedValues contains a map with "AttributeClass#key" as values
   */
  public static <T> void assertAttributeReflection(final AttributeImpl att, Map<String,T> reflectedValues) {
    final Map<String,Object> map = new HashMap<>();
    att.reflectWith(new AttributeReflector() {
      @Override
      public void reflect(Class<? extends Attribute> attClass, String key, Object value) {
        map.put(attClass.getName() + '#' + key, value);
      }
    });
    Assert.assertEquals("Reflection does not produce same map", reflectedValues, map);
  }

  public static void assertEquals(TopDocs expected, TopDocs actual) {
    Assert.assertEquals("wrong total hits", expected.totalHits, actual.totalHits);
    Assert.assertEquals("wrong maxScore", expected.getMaxScore(), actual.getMaxScore(), 0.0);
    Assert.assertEquals("wrong hit count", expected.scoreDocs.length, actual.scoreDocs.length);
    for(int hitIDX=0;hitIDX<expected.scoreDocs.length;hitIDX++) {
      final ScoreDoc expectedSD = expected.scoreDocs[hitIDX];
      final ScoreDoc actualSD = actual.scoreDocs[hitIDX];
      Assert.assertEquals("wrong hit docID", expectedSD.doc, actualSD.doc);
      Assert.assertEquals("wrong hit score", expectedSD.score, actualSD.score, 0.0);
      if (expectedSD instanceof FieldDoc) {
        Assert.assertTrue(actualSD instanceof FieldDoc);
        Assert.assertArrayEquals("wrong sort field values",
                            ((FieldDoc) expectedSD).fields,
                            ((FieldDoc) actualSD).fields);
      } else {
        Assert.assertFalse(actualSD instanceof FieldDoc);
      }
    }
  }

  // NOTE: this is likely buggy, and cannot clone fields
  // with tokenStreamValues, etc.  Use at your own risk!!

  // TODO: is there a pre-existing way to do this!!!
  public static Document cloneDocument(Document doc1) {
    final Document doc2 = new Document();
    for(IndexableField f : doc1.getFields()) {
      final Field field1 = (Field) f;
      final Field field2;
      final DocValuesType dvType = field1.fieldType().docValuesType();
      final NumericType numType = field1.fieldType().numericType();
      if (dvType != DocValuesType.NONE) {
        switch(dvType) {
          case NUMERIC:
            field2 = new NumericDocValuesField(field1.name(), field1.numericValue().longValue());
            break;
          case BINARY:
            field2 = new BinaryDocValuesField(field1.name(), field1.binaryValue());
            break;
          case SORTED:
            field2 = new SortedDocValuesField(field1.name(), field1.binaryValue());
            break;
          default:
            throw new IllegalStateException("unknown Type: " + dvType);
        }
      } else if (numType != null) {
        switch (numType) {
          case INT:
            field2 = new IntField(field1.name(), field1.numericValue().intValue(), field1.fieldType());
            break;
          case FLOAT:
            field2 = new FloatField(field1.name(), field1.numericValue().intValue(), field1.fieldType());
            break;
          case LONG:
            field2 = new LongField(field1.name(), field1.numericValue().intValue(), field1.fieldType());
            break;
          case DOUBLE:
            field2 = new DoubleField(field1.name(), field1.numericValue().intValue(), field1.fieldType());
            break;
          default:
            throw new IllegalStateException("unknown Type: " + numType);
        }
      } else {
        field2 = new Field(field1.name(), field1.stringValue(), field1.fieldType());
      }
      doc2.add(field2);
    }

    return doc2;
  }

  // Returns a DocsEnum, but randomly sometimes uses a
  // DocsAndFreqsEnum, DocsAndPositionsEnum.  Returns null
  // if field/term doesn't exist:
  public static PostingsEnum docs(Random random, IndexReader r, String field, BytesRef term, PostingsEnum reuse, int flags) throws IOException {
    final Terms terms = MultiFields.getTerms(r, field);
    if (terms == null) {
      return null;
    }
    final TermsEnum termsEnum = terms.iterator();
    if (!termsEnum.seekExact(term)) {
      return null;
    }
    return docs(random, termsEnum, reuse, flags);
  }

  // Returns a PostingsEnum with random features available
  public static PostingsEnum docs(Random random, TermsEnum termsEnum, PostingsEnum reuse, int flags) throws IOException {
    // TODO: simplify this method? it would be easier to randomly either use the flags passed, or do the random selection,
    // FREQS should be part fo the random selection instead of outside on its own?
    if (random.nextBoolean()) {
      if (random.nextBoolean()) {
        final int posFlags;
        switch (random.nextInt(4)) {
          case 0: posFlags = PostingsEnum.POSITIONS; break;
          case 1: posFlags = PostingsEnum.OFFSETS; break;
          case 2: posFlags = PostingsEnum.PAYLOADS; break;
          default: posFlags = PostingsEnum.ALL; break;
        }
        return termsEnum.postings(null, posFlags);
      }
      flags |= PostingsEnum.FREQS;
    }
    return termsEnum.postings(reuse, flags);
  }
  
  public static CharSequence stringToCharSequence(String string, Random random) {
    return bytesToCharSequence(new BytesRef(string), random);
  }
  
  public static CharSequence bytesToCharSequence(BytesRef ref, Random random) {
    switch(random.nextInt(5)) {
    case 4:
      final char[] chars = new char[ref.length];
      final int len = UnicodeUtil.UTF8toUTF16(ref.bytes, ref.offset, ref.length, chars);
      return new CharsRef(chars, 0, len);
    case 3:
      return CharBuffer.wrap(ref.utf8ToString());
    default:
      return ref.utf8ToString();
    }
  }

  /**
   * Shutdown {@link ExecutorService} and wait for its.
   */
  public static void shutdownExecutorService(ExecutorService ex) {
    if (ex != null) {
      try {
        ex.shutdown();
        ex.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // Just report it on the syserr.
        System.err.println("Could not properly close executor service.");
        e.printStackTrace(System.err);
      }
    }
  }

  /**
   * Returns a valid (compiling) Pattern instance with random stuff inside. Be careful
   * when applying random patterns to longer strings as certain types of patterns
   * may explode into exponential times in backtracking implementations (such as Java's).
   */
  public static Pattern randomPattern(Random random) {
    final String nonBmpString = "AB\uD840\uDC00C";
    while (true) {
      try {
        Pattern p = Pattern.compile(TestUtil.randomRegexpishString(random));
        String replacement = null;
        // ignore bugs in Sun's regex impl
        try {
          replacement = p.matcher(nonBmpString).replaceAll("_");
        } catch (StringIndexOutOfBoundsException jdkBug) {
          System.out.println("WARNING: your jdk is buggy!");
          System.out.println("Pattern.compile(\"" + p.pattern() + 
              "\").matcher(\"AB\\uD840\\uDC00C\").replaceAll(\"_\"); should not throw IndexOutOfBounds!");
        }
        // Make sure the result of applying the pattern to a string with extended
        // unicode characters is a valid utf16 string. See LUCENE-4078 for discussion.
        if (replacement != null && UnicodeUtil.validUTF16String(replacement)) {
          return p;
        }
      } catch (PatternSyntaxException ignored) {
        // Loop trying until we hit something that compiles.
      }
    }
  }
    
  
  public static final FilterStrategy randomFilterStrategy(final Random random) {
    switch(random.nextInt(6)) {
      case 5:
      case 4:
        return new FilteredQuery.RandomAccessFilterStrategy() {
          @Override
          protected boolean useRandomAccess(Bits bits, long filterCost) {
            return LuceneTestCase.random().nextBoolean();
          }
        };
      case 3:
        return FilteredQuery.RANDOM_ACCESS_FILTER_STRATEGY;
      case 2:
        return FilteredQuery.LEAP_FROG_FILTER_FIRST_STRATEGY;
      case 1:
        return FilteredQuery.LEAP_FROG_QUERY_FIRST_STRATEGY;
      case 0: 
        return FilteredQuery.QUERY_FIRST_FILTER_STRATEGY;
      default:
        return FilteredQuery.RANDOM_ACCESS_FILTER_STRATEGY;
    }
  }

  public static String randomAnalysisString(Random random, int maxLength, boolean simple) {
    assert maxLength >= 0;

    // sometimes just a purely random string
    if (random.nextInt(31) == 0) {
      return randomSubString(random, random.nextInt(maxLength), simple);
    }

    // otherwise, try to make it more realistic with 'words' since most tests use MockTokenizer
    // first decide how big the string will really be: 0..n
    maxLength = random.nextInt(maxLength);
    int avgWordLength = TestUtil.nextInt(random, 3, 8);
    StringBuilder sb = new StringBuilder();
    while (sb.length() < maxLength) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      int wordLength = -1;
      while (wordLength < 0) {
        wordLength = (int) (random.nextGaussian() * 3 + avgWordLength);
      }
      wordLength = Math.min(wordLength, maxLength - sb.length());
      sb.append(randomSubString(random, wordLength, simple));
    }
    return sb.toString();
  }

  public static String randomSubString(Random random, int wordLength, boolean simple) {
    if (wordLength == 0) {
      return "";
    }

    int evilness = TestUtil.nextInt(random, 0, 20);

    StringBuilder sb = new StringBuilder();
    while (sb.length() < wordLength) {
      if (simple) {
        sb.append(random.nextBoolean() ? TestUtil.randomSimpleString(random, wordLength) : TestUtil.randomHtmlishString(random, wordLength));
      } else {
        if (evilness < 10) {
          sb.append(TestUtil.randomSimpleString(random, wordLength));
        } else if (evilness < 15) {
          assert sb.length() == 0; // we should always get wordLength back!
          sb.append(TestUtil.randomRealisticUnicodeString(random, wordLength, wordLength));
        } else if (evilness == 16) {
          sb.append(TestUtil.randomHtmlishString(random, wordLength));
        } else if (evilness == 17) {
          // gives a lot of punctuation
          sb.append(TestUtil.randomRegexpishString(random, wordLength));
        } else {
          sb.append(TestUtil.randomUnicodeString(random, wordLength));
        }
      }
    }
    if (sb.length() > wordLength) {
      sb.setLength(wordLength);
      if (Character.isHighSurrogate(sb.charAt(wordLength-1))) {
        sb.setLength(wordLength-1);
      }
    }

    if (random.nextInt(17) == 0) {
      // mix up case
      String mixedUp = TestUtil.randomlyRecaseCodePoints(random, sb.toString());
      assert mixedUp.length() == sb.length();
      return mixedUp;
    } else {
      return sb.toString();
    }
  }

  /** For debugging: tries to include br.utf8ToString(), but if that
   *  fails (because it's not valid utf8, which is fine!), just
   *  use ordinary toString. */
  public static String bytesRefToString(BytesRef br) {
    if (br == null) {
      return "(null)";
    } else {
      try {
        return br.utf8ToString() + " " + br.toString();
      } catch (AssertionError | IllegalArgumentException t) {
        // If BytesRef isn't actually UTF8, or it's eg a
        // prefix of UTF8 that ends mid-unicode-char, we
        // fallback to hex:
        return br.toString();
      }
    }
  }
  
  /** Returns a copy of directory, entirely in RAM */
  public static RAMDirectory ramCopyOf(Directory dir) throws IOException {
    RAMDirectory ram = new RAMDirectory();
    for (String file : dir.listAll()) {
      if (file.startsWith(IndexFileNames.SEGMENTS) || IndexFileNames.CODEC_FILE_PATTERN.matcher(file).matches()) {
        ram.copyFrom(dir, file, file, IOContext.DEFAULT);
      }
    }
    return ram;
  }
}
