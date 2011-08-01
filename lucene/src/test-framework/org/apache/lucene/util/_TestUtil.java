package org.apache.lucene.util;

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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.junit.Assert;

public class _TestUtil {

  /** Returns temp dir, based on String arg in its name;
   *  does not create the directory. */
  public static File getTempDir(String desc) {
    try {
      File f = createTempFile(desc, "tmp", LuceneTestCase.TEMP_DIR);
      f.delete();
      LuceneTestCase.registerTempFile(f);
      return f;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deletes a directory and everything underneath it.
   */
  public static void rmDir(File dir) throws IOException {
    if (dir.exists()) {
      for (File f : dir.listFiles()) {
        if (f.isDirectory()) {
          rmDir(f);
        } else {
          if (!f.delete()) {
            throw new IOException("could not delete " + f);
          }
        }
      }
      if (!dir.delete()) {
        throw new IOException("could not delete " + dir);
      }
    }
  }

  /** 
   * Convenience method: Unzip zipName + ".zip" under destDir, removing destDir first 
   */
  public static void unzip(File zipName, File destDir) throws IOException {
    
    ZipFile zipFile = new ZipFile(zipName);
    
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    
    rmDir(destDir);
    
    destDir.mkdir();
    LuceneTestCase.registerTempFile(destDir);
    
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      
      InputStream in = zipFile.getInputStream(entry);
      File targetFile = new File(destDir, entry.getName());
      if (entry.isDirectory()) {
        // allow unzipping with directory structure
        targetFile.mkdirs();
      } else {
        if (targetFile.getParentFile()!=null) {
          // be on the safe side: do not rely on that directories are always extracted
          // before their children (although this makes sense, but is it guaranteed?)
          targetFile.getParentFile().mkdirs();   
        }
        OutputStream out = new BufferedOutputStream(new FileOutputStream(targetFile));
        
        byte[] buffer = new byte[8192];
        int len;
        while((len = in.read(buffer)) >= 0) {
          out.write(buffer, 0, len);
        }
        
        in.close();
        out.close();
      }
    }
    
    zipFile.close();
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
    return checkIndex(dir, CodecProvider.getDefault());
  }
  
  /** This runs the CheckIndex tool on the index in.  If any
   *  issues are hit, a RuntimeException is thrown; else,
   *  true is returned. */
  public static CheckIndex.Status checkIndex(Directory dir, CodecProvider codecs) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    CheckIndex checker = new CheckIndex(dir);
    checker.setInfoStream(new PrintStream(bos));
    CheckIndex.Status indexStatus = checker.checkIndex(null, codecs);
    if (indexStatus == null || indexStatus.clean == false) {
      System.out.println("CheckIndex failed");
      System.out.println(bos.toString());
      throw new RuntimeException("CheckIndex failed");
    } else {
      return indexStatus;
    }
  }

  /** start and end are BOTH inclusive */
  public static int nextInt(Random r, int start, int end) {
    return start + r.nextInt(end-start+1);
  }

  public static String randomSimpleString(Random r) {
    final int end = r.nextInt(10);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      buffer[i] = (char) _TestUtil.nextInt(r, 97, 102);
    }
    return new String(buffer, 0, end);
  }

  /** Returns random string, including full unicode range. */
  public static String randomUnicodeString(Random r) {
    return randomUnicodeString(r, 20);
  }

  /**
   * Returns a random string up to a certain length.
   */
  public static String randomUnicodeString(Random r, int maxLength) {
    final int end = r.nextInt(maxLength);
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
    return randomRealisticUnicodeString(r, 0, 20);
  }

  /** Returns random string of length between min and max codepoints, all codepoints within the same unicode block. */
  public static String randomRealisticUnicodeString(Random r, int minLength, int maxLength) {
    final int end = minLength + r.nextInt(maxLength);
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

  public static CodecProvider alwaysCodec(final Codec c) {
    CodecProvider p = new CodecProvider() {
      @Override
      public Codec lookup(String name) {
        // can't do this until we fix PreFlexRW to not
        //impersonate PreFlex:
        if (name.equals(c.name)) {
          return c;
        } else {
          return CodecProvider.getDefault().lookup(name);
        }
      }
    };
    p.setDefaultFieldCodec(c.name);
    return p;
  }

  /** Return a CodecProvider that can read any of the
   *  default codecs, but always writes in the specified
   *  codec. */
  public static CodecProvider alwaysCodec(final String codec) {
    return alwaysCodec(CodecProvider.getDefault().lookup(codec));
  }

  public static boolean anyFilesExceptWriteLock(Directory dir) throws IOException {
    String[] files = dir.listAll();
    if (files.length > 1 || (files.length == 1 && !files[0].equals("write.lock"))) {
      return true;
    } else {
      return false;
    }
  }

  /** just tries to configure things to keep the open file
   * count lowish */
  public static void reduceOpenFiles(IndexWriter w) {
    // keep number of open files lowish
    MergePolicy mp = w.getConfig().getMergePolicy();
    if (mp instanceof LogMergePolicy) {
      LogMergePolicy lmp = (LogMergePolicy) mp;
      lmp.setMergeFactor(Math.min(5, lmp.getMergeFactor()));
    } else if (mp instanceof TieredMergePolicy) {
      TieredMergePolicy tmp = (TieredMergePolicy) mp;
      tmp.setMaxMergeAtOnce(Math.min(5, tmp.getMaxMergeAtOnce()));
      tmp.setSegmentsPerTier(Math.min(5, tmp.getSegmentsPerTier()));
    }
    MergeScheduler ms = w.getConfig().getMergeScheduler();
    if (ms instanceof ConcurrentMergeScheduler) {
      ((ConcurrentMergeScheduler) ms).setMaxThreadCount(2);
      ((ConcurrentMergeScheduler) ms).setMaxMergeCount(3);
    }
  }

  /** Checks some basic behaviour of an AttributeImpl
   * @param reflectedValues contains a map with "AttributeClass#key" as values
   */
  public static <T> void assertAttributeReflection(final AttributeImpl att, Map<String,T> reflectedValues) {
    final Map<String,Object> map = new HashMap<String,Object>();
    att.reflectWith(new AttributeReflector() {
      public void reflect(Class<? extends Attribute> attClass, String key, Object value) {
        map.put(attClass.getName() + '#' + key, value);
      }
    });
    Assert.assertEquals("Reflection does not produce same map", reflectedValues, map);
  }

  public static void keepFullyDeletedSegments(IndexWriter w) {
    try {
      // Carefully invoke what is a package-private (test
      // only, internal) method on IndexWriter:
      Method m = IndexWriter.class.getDeclaredMethod("keepFullyDeletedSegments");
      m.setAccessible(true);
      m.invoke(w);
    } catch (Exception e) {
      // Should not happen?
      throw new RuntimeException(e);
    }
  }
  
  /** Adds field info for a Document. */
  public static void add(Document doc, FieldInfos fieldInfos) {
    List<Fieldable> fields = doc.getFields();
    for (Fieldable field : fields) {
      fieldInfos.addOrUpdate(field.name(), field.isIndexed(), field.isTermVectorStored(), field.isStorePositionWithTermVector(),
              field.isStoreOffsetWithTermVector(), field.getOmitNorms(), false, field.getIndexOptions(), field.docValuesType());
    }
  }
  
  /** 
   * insecure, fast version of File.createTempFile
   * uses Random instead of SecureRandom.
   */
  public static File createTempFile(String prefix, String suffix, File directory)
      throws IOException {
    // Force a prefix null check first
    if (prefix.length() < 3) {
      throw new IllegalArgumentException("prefix must be 3");
    }
    String newSuffix = suffix == null ? ".tmp" : suffix;
    File result;
    do {
      result = genTempFile(prefix, newSuffix, directory);
    } while (!result.createNewFile());
    return result;
  }

  /* Temp file counter */
  private static int counter = 0;

  /* identify for differnt VM processes */
  private static int counterBase = 0;

  private static class TempFileLocker {};
  private static TempFileLocker tempFileLocker = new TempFileLocker();

  private static File genTempFile(String prefix, String suffix, File directory) {
    int identify = 0;

    synchronized (tempFileLocker) {
      if (counter == 0) {
        int newInt = new Random().nextInt();
        counter = ((newInt / 65535) & 0xFFFF) + 0x2710;
        counterBase = counter;
      }
      identify = counter++;
    }

    StringBuilder newName = new StringBuilder();
    newName.append(prefix);
    newName.append(counterBase);
    newName.append(identify);
    newName.append(suffix);
    return new File(directory, newName.toString());
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
        Assert.assertEquals("wrong sort field values",
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
    for(Fieldable f : doc1.getFields()) {
      Field field1 = (Field) f;
      
      Field field2 = new Field(field1.name(),
                               field1.stringValue(),
                               field1.isStored() ? Field.Store.YES : Field.Store.NO,
                               field1.isIndexed() ? (field1.isTokenized() ? Field.Index.ANALYZED : Field.Index.NOT_ANALYZED) : Field.Index.NO);
      field2.setOmitNorms(field1.getOmitNorms());
      field2.setIndexOptions(field1.getIndexOptions());
      doc2.add(field2);
    }

    return doc2;
  }
}
