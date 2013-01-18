package org.egothor.stemmer;

/*
 Egothor Software License version 1.00
 Copyright (C) 1997-2004 Leo Galambos.
 Copyright (C) 2002-2004 "Egothor developers"
 on behalf of the Egothor Project.
 All rights reserved.

 This  software  is  copyrighted  by  the "Egothor developers". If this
 license applies to a single file or document, the "Egothor developers"
 are the people or entities mentioned as copyright holders in that file
 or  document.  If  this  license  applies  to the Egothor project as a
 whole,  the  copyright holders are the people or entities mentioned in
 the  file CREDITS. This file can be found in the same location as this
 license in the distribution.

 Redistribution  and  use  in  source and binary forms, with or without
 modification, are permitted provided that the following conditions are
 met:
 1. Redistributions  of  source  code  must retain the above copyright
 notice, the list of contributors, this list of conditions, and the
 following disclaimer.
 2. Redistributions  in binary form must reproduce the above copyright
 notice, the list of contributors, this list of conditions, and the
 disclaimer  that  follows  these  conditions  in the documentation
 and/or other materials provided with the distribution.
 3. The name "Egothor" must not be used to endorse or promote products
 derived  from  this software without prior written permission. For
 written permission, please contact Leo.G@seznam.cz
 4. Products  derived  from this software may not be called "Egothor",
 nor  may  "Egothor"  appear  in  their name, without prior written
 permission from Leo.G@seznam.cz.

 In addition, we request that you include in the end-user documentation
 provided  with  the  redistribution  and/or  in the software itself an
 acknowledgement equivalent to the following:
 "This product includes software developed by the Egothor Project.
 http://egothor.sf.net/"

 THIS  SOFTWARE  IS  PROVIDED  ``AS  IS''  AND ANY EXPRESSED OR IMPLIED
 WARRANTIES,  INCLUDING,  BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 MERCHANTABILITY  AND  FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 IN  NO  EVENT  SHALL THE EGOTHOR PROJECT OR ITS CONTRIBUTORS BE LIABLE
 FOR   ANY   DIRECT,   INDIRECT,  INCIDENTAL,  SPECIAL,  EXEMPLARY,  OR
 CONSEQUENTIAL  DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 SUBSTITUTE  GOODS  OR  SERVICES;  LOSS  OF  USE,  DATA, OR PROFITS; OR
 BUSINESS  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 WHETHER  IN  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
 IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 This  software  consists  of  voluntary  contributions  made  by  many
 individuals  on  behalf  of  the  Egothor  Project  and was originally
 created by Leo Galambos (Leo.G@seznam.cz).
 */

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestCompile extends LuceneTestCase {
  
  public void testCompile() throws Exception {
    File dir = _TestUtil.getTempDir("testCompile");
    dir.mkdirs();
    InputStream input = getClass().getResourceAsStream("testRules.txt");
    File output = new File(dir, "testRules.txt");
    copy(input, output);
    input.close();
    String path = output.getAbsolutePath();
    Compile.main(new String[] {"test", path});
    String compiled = path + ".out";
    Trie trie = loadTrie(compiled);
    assertTrie(trie, path, true, true);
    assertTrie(trie, path, false, true);
    new File(compiled).delete();
  }
  
  public void testCompileBackwards() throws Exception {
    File dir = _TestUtil.getTempDir("testCompile");
    dir.mkdirs();
    InputStream input = getClass().getResourceAsStream("testRules.txt");
    File output = new File(dir, "testRules.txt");
    copy(input, output);
    input.close();
    String path = output.getAbsolutePath();
    Compile.main(new String[] {"-test", path});
    String compiled = path + ".out";
    Trie trie = loadTrie(compiled);
    assertTrie(trie, path, true, true);
    assertTrie(trie, path, false, true);
    new File(compiled).delete();
  }
  
  public void testCompileMulti() throws Exception {
    File dir = _TestUtil.getTempDir("testCompile");
    dir.mkdirs();
    InputStream input = getClass().getResourceAsStream("testRules.txt");
    File output = new File(dir, "testRules.txt");
    copy(input, output);
    input.close();
    String path = output.getAbsolutePath();
    Compile.main(new String[] {"Mtest", path});
    String compiled = path + ".out";
    Trie trie = loadTrie(compiled);
    assertTrie(trie, path, true, true);
    assertTrie(trie, path, false, true);
    new File(compiled).delete();
  }
  
  static Trie loadTrie(String path) throws IOException {
    Trie trie;
    DataInputStream is = new DataInputStream(new BufferedInputStream(
        new FileInputStream(path)));
    String method = is.readUTF().toUpperCase(Locale.ROOT);
    if (method.indexOf('M') < 0) {
      trie = new Trie(is);
    } else {
      trie = new MultiTrie(is);
    }
    is.close();
    return trie;
  }
  
  private static void assertTrie(Trie trie, String file, boolean usefull,
      boolean storeorig) throws Exception {
    LineNumberReader in = new LineNumberReader(new BufferedReader(
        new InputStreamReader(new FileInputStream(file), IOUtils.CHARSET_UTF_8)));
    
    for (String line = in.readLine(); line != null; line = in.readLine()) {
      try {
        line = line.toLowerCase(Locale.ROOT);
        StringTokenizer st = new StringTokenizer(line);
        String stem = st.nextToken();
        if (storeorig) {
          CharSequence cmd = (usefull) ? trie.getFully(stem) : trie
              .getLastOnPath(stem);
          StringBuilder stm = new StringBuilder(stem);
          Diff.apply(stm, cmd);
          assertEquals(stem.toLowerCase(Locale.ROOT), stm.toString().toLowerCase(Locale.ROOT));
        }
        while (st.hasMoreTokens()) {
          String token = st.nextToken();
          if (token.equals(stem)) {
            continue;
          }
          CharSequence cmd = (usefull) ? trie.getFully(token) : trie
              .getLastOnPath(token);
          StringBuilder stm = new StringBuilder(token);
          Diff.apply(stm, cmd);
          assertEquals(stem.toLowerCase(Locale.ROOT), stm.toString().toLowerCase(Locale.ROOT));
        }
      } catch (java.util.NoSuchElementException x) {
        // no base token (stem) on a line
      }
    }
    
    in.close();
  }
  
  private static void copy(InputStream input, File output) throws IOException {
    FileOutputStream os = new FileOutputStream(output);
    try {
      byte buffer[] = new byte[1024];
      int len;
      while ((len = input.read(buffer)) > 0) {
        os.write(buffer, 0, len);
      }
    } finally {
      os.close();
    }
  }
}
