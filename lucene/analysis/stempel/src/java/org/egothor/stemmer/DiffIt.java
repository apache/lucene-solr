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

import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.lucene.util.SuppressForbidden;

/**
 * The DiffIt class is a means generate patch commands from an already prepared
 * stemmer table.
 */
public class DiffIt {
  
  /** no instantiation */
  private DiffIt() {}
  
  static int get(int i, String s) {
    try {
      return Integer.parseInt(s.substring(i, i + 1));
    } catch (Throwable x) {
      return 1;
    }
  }
  
  /**
   * Entry point to the DiffIt application.
   * <p>
   * This application takes one argument, the path to a file containing a
   * stemmer table. The program reads the file and generates the patch commands
   * for the stems.
   * 
   * @param args the path to a file containing a stemmer table
   */
  @SuppressForbidden(reason = "System.out required: command line tool")
  public static void main(java.lang.String[] args) throws Exception {
    
    int ins = get(0, args[0]);
    int del = get(1, args[0]);
    int rep = get(2, args[0]);
    int nop = get(3, args[0]);
    
    for (int i = 1; i < args.length; i++) {
      LineNumberReader in;
      // System.out.println("[" + args[i] + "]");
      Diff diff = new Diff(ins, del, rep, nop);
      String charset = System.getProperty("egothor.stemmer.charset", "UTF-8");
      in = new LineNumberReader(Files.newBufferedReader(Paths.get(args[i]), Charset.forName(charset)));
      for (String line = in.readLine(); line != null; line = in.readLine()) {
        try {
          line = line.toLowerCase(Locale.ROOT);
          StringTokenizer st = new StringTokenizer(line);
          String stem = st.nextToken();
          System.out.println(stem + " -a");
          while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (token.equals(stem) == false) {
              System.out.println(stem + " " + diff.exec(token, stem));
            }
          }
        } catch (java.util.NoSuchElementException x) {
          // no base token (stem) on a line
        }
      }
    }
  }
}
