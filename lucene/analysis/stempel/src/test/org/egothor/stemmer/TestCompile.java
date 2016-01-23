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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.lucene.util.LuceneTestCase;

public class TestCompile extends LuceneTestCase {
  
  public void testCompile() throws Exception {
    Path dir = createTempDir("testCompile");
    Path output = dir.resolve("testRules.txt");
    try (InputStream input = getClass().getResourceAsStream("testRules.txt")) {
      Files.copy(input, output);
    }
    String path = output.toAbsolutePath().toString();
    Compile.main(new String[] {"test", path});
    Path compiled = dir.resolve("testRules.txt.out");
    Trie trie = loadTrie(compiled);
    assertTrie(trie, output, true, true);
    assertTrie(trie, output, false, true);
  }
  
  public void testCompileBackwards() throws Exception {
    Path dir = createTempDir("testCompile");
    Path output = dir.resolve("testRules.txt");
    try (InputStream input = getClass().getResourceAsStream("testRules.txt")) {
      Files.copy(input, output);
    }
    String path = output.toAbsolutePath().toString();
    Compile.main(new String[] {"-test", path});
    Path compiled = dir.resolve("testRules.txt.out");
    Trie trie = loadTrie(compiled);
    assertTrie(trie, output, true, true);
    assertTrie(trie, output, false, true);
  }
  
  public void testCompileMulti() throws Exception {
    Path dir = createTempDir("testCompile");
    Path output = dir.resolve("testRules.txt");
    try (InputStream input = getClass().getResourceAsStream("testRules.txt")) {
      Files.copy(input, output);
    }
    String path = output.toAbsolutePath().toString();
    Compile.main(new String[] {"Mtest", path});
    Path compiled = dir.resolve("testRules.txt.out");
    Trie trie = loadTrie(compiled);
    assertTrie(trie, output, true, true);
    assertTrie(trie, output, false, true);
  }
  
  static Trie loadTrie(Path path) throws IOException {
    Trie trie;
    DataInputStream is = new DataInputStream(new BufferedInputStream(
        Files.newInputStream(path)));
    String method = is.readUTF().toUpperCase(Locale.ROOT);
    if (method.indexOf('M') < 0) {
      trie = new Trie(is);
    } else {
      trie = new MultiTrie(is);
    }
    is.close();
    return trie;
  }
  
  private static void assertTrie(Trie trie, Path file, boolean usefull,
      boolean storeorig) throws Exception {
    LineNumberReader in = new LineNumberReader(Files.newBufferedReader(file, StandardCharsets.UTF_8));
    
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
}
