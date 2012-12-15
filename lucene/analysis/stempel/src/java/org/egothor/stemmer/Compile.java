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
package org.egothor.stemmer;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * The Compile class is used to compile a stemmer table.
 */
public class Compile {
  
  static boolean backward;
  static boolean multi;
  static Trie trie;

  /** no instantiation */
  private Compile() {}

  /**
   * Entry point to the Compile application.
   * <p>
   * This program takes any number of arguments: the first is the name of the
   * desired stemming algorithm to use (a list is available in the package
   * description) , all of the rest should be the path or paths to a file or
   * files containing a stemmer table to compile.
   * 
   * @param args the command line arguments
   */
  public static void main(java.lang.String[] args) throws Exception {
    if (args.length < 1) {
      return;
    }
    
    args[0].toUpperCase(Locale.ROOT);
    
    backward = args[0].charAt(0) == '-';
    int qq = (backward) ? 1 : 0;
    boolean storeorig = false;
    
    if (args[0].charAt(qq) == '0') {
      storeorig = true;
      qq++;
    }
    
    multi = args[0].charAt(qq) == 'M';
    if (multi) {
      qq++;
    }
    
    String charset = System.getProperty("egothor.stemmer.charset", "UTF-8");
    
    char optimizer[] = new char[args[0].length() - qq];
    for (int i = 0; i < optimizer.length; i++) {
      optimizer[i] = args[0].charAt(qq + i);
    }
    
    for (int i = 1; i < args.length; i++) {
      LineNumberReader in;
      // System.out.println("[" + args[i] + "]");
      Diff diff = new Diff();
      int stems = 0;
      int words = 0;
      
      allocTrie();
      
      System.out.println(args[i]);
      in = new LineNumberReader(new BufferedReader(new InputStreamReader(
          new FileInputStream(args[i]), charset)));
      for (String line = in.readLine(); line != null; line = in.readLine()) {
        try {
          line = line.toLowerCase(Locale.ROOT);
          StringTokenizer st = new StringTokenizer(line);
          String stem = st.nextToken();
          if (storeorig) {
            trie.add(stem, "-a");
            words++;
          }
          while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (token.equals(stem) == false) {
              trie.add(token, diff.exec(token, stem));
              words++;
            }
          }
        } catch (java.util.NoSuchElementException x) {
          // no base token (stem) on a line
        }
      }
      in.close();
      
      Optimizer o = new Optimizer();
      Optimizer2 o2 = new Optimizer2();
      Lift l = new Lift(true);
      Lift e = new Lift(false);
      Gener g = new Gener();
      
      for (int j = 0; j < optimizer.length; j++) {
        String prefix;
        switch (optimizer[j]) {
          case 'G':
            trie = trie.reduce(g);
            prefix = "G: ";
            break;
          case 'L':
            trie = trie.reduce(l);
            prefix = "L: ";
            break;
          case 'E':
            trie = trie.reduce(e);
            prefix = "E: ";
            break;
          case '2':
            trie = trie.reduce(o2);
            prefix = "2: ";
            break;
          case '1':
            trie = trie.reduce(o);
            prefix = "1: ";
            break;
          default:
            continue;
        }
        trie.printInfo(System.out, prefix + " ");
      }
      
      DataOutputStream os = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(args[i] + ".out")));
      os.writeUTF(args[0]);
      trie.store(os);
      os.close();
    }
  }
  
  static void allocTrie() {
    if (multi) {
      trie = new MultiTrie2(!backward);
    } else {
      trie = new Trie(!backward);
    }
  }
}
