/*

Copyright (c) 2001, Dr Martin Porter
Copyright (c) 2002, Richard Boulton
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
    * this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
    * notice, this list of conditions and the following disclaimer in the
    * documentation and/or other materials provided with the distribution.
    * Neither the name of the copyright holders nor the names of its contributors
    * may be used to endorse or promote products derived from this software
    * without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */

package org.tartarus.snowball;

import java.lang.reflect.Method;

/**
 * This is the rev 502 of the Snowball SVN trunk,
 * but modified:
 * made abstract and introduced abstract method stem to avoid expensive reflection in filter class.
 * refactored StringBuffers to StringBuilder
 * uses char[] as buffer instead of StringBuffer/StringBuilder
 * eq_s,eq_s_b,insert,replace_s take CharSequence like eq_v and eq_v_b
 * reflection calls (Lovins, etc) use EMPTY_ARGS/EMPTY_PARAMS
 */
public class Among {
  private static final Class<?>[] EMPTY_PARAMS = new Class[0];

  public Among(String s, int substring_i, int result,
               String methodname, SnowballProgram methodobject) {
    this.s_size = s.length();
    this.s = s.toCharArray();
    this.substring_i = substring_i;
    this.result = result;
    this.methodobject = methodobject;
    if (methodname.length() == 0) {
      this.method = null;
    } else {
      try {
        this.method = methodobject.getClass().
            getDeclaredMethod(methodname, EMPTY_PARAMS);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }
  }

    public final int s_size; /* search string */
    public final char[] s; /* search string */
    public final int substring_i; /* index to longest matching substring */
    public final int result;      /* result of the lookup */
    public final Method method; /* method to use if substring matches */
    public final SnowballProgram methodobject; /* object to invoke method on */
   
};
