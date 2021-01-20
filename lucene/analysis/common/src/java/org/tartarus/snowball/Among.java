/*
Copyright (c) 2001, Dr Martin Porter
Copyright (c) 2004,2005, Richard Boulton
Copyright (c) 2013, Yoshiki Shibukawa
Copyright (c) 2006,2007,2009,2010,2011,2014-2019, Olly Betts
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

  1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
  2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
  3. Neither the name of the Snowball project nor the names of its contributors
   may be used to endorse or promote products derived from this software
   without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package org.tartarus.snowball;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Locale;

/**
 * Internal class used by Snowball stemmers
 */
public class Among {
  public Among (String s, int substring_i, int result) {
    this.s = s.toCharArray();
    this.substring_i = substring_i;
    this.result = result;
    this.method = null;
  }

  public Among (String s, int substring_i, int result, String methodname,
          MethodHandles.Lookup methodobject) {
    this.s = s.toCharArray();
    this.substring_i = substring_i;
    this.result = result;
    final Class<? extends SnowballProgram> clazz = methodobject.lookupClass().asSubclass(SnowballProgram.class);
    if (methodname.length() > 0) {
      try {
        this.method = methodobject.findVirtual(clazz, methodname, MethodType.methodType(boolean.class))
          .asType(MethodType.methodType(boolean.class, SnowballProgram.class));
      } catch (NoSuchMethodException | IllegalAccessException e) {
        throw new RuntimeException(String.format(Locale.ENGLISH,
          "Snowball program '%s' is broken, cannot access method: boolean %s()",
          clazz.getSimpleName(), methodname
        ), e);
      }
    } else {
      this.method = null;
    }
  }

  final char[] s; /* search string */
  final int substring_i; /* index to longest matching substring */
  final int result; /* result of the lookup */

  // Make sure this is not accessible outside package for Java security reasons!
  final MethodHandle method; /* method to use if substring matches */
};
