package org.apache.lucene.util;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001, 2004 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */


public class English {

  public static String intToEnglish(int i) {
    StringBuffer result = new StringBuffer();
    intToEnglish(i, result);
    return result.toString();
  }

  public static void intToEnglish(int i, StringBuffer result) {
    if (i == 0) {
      result.append("zero");
      return;
    }
    if (i < 0) {
      result.append("minus ");
      i = -i;
    }
    if (i >= 1000000000) {			  // billions
      intToEnglish(i/1000000000, result);
      result.append("billion, ");
      i = i%1000000000;
    }
    if (i >= 1000000) {				  // millions
      intToEnglish(i/1000000, result);
      result.append("million, ");
      i = i%1000000;
    }
    if (i >= 1000) {				  // thousands
      intToEnglish(i/1000, result);
      result.append("thousand, ");
      i = i%1000;
    }
    if (i >= 100) {				  // hundreds
      intToEnglish(i/100, result);
      result.append("hundred ");
      i = i%100;
    }
    if (i >= 20) {
      switch (i/10) {
      case 9 : result.append("ninety"); break;
      case 8 : result.append("eighty"); break;
      case 7 : result.append("seventy"); break;
      case 6 : result.append("sixty"); break;
      case 5 : result.append("fifty"); break;
      case 4 : result.append("forty"); break;
      case 3 : result.append("thirty"); break;
      case 2 : result.append("twenty"); break;
      }
      i = i%10;
      if (i == 0)
        result.append(" ");
      else 
        result.append("-");
    }
    switch (i) {
    case 19 : result.append("nineteen "); break;
    case 18 : result.append("eighteen "); break;
    case 17 : result.append("seventeen "); break;
    case 16 : result.append("sixteen "); break;
    case 15 : result.append("fifteen "); break;
    case 14 : result.append("fourteen "); break;
    case 13 : result.append("thirteen "); break;
    case 12 : result.append("twelve "); break;
    case 11 : result.append("eleven "); break;
    case 10 : result.append("ten "); break;
    case 9 : result.append("nine "); break;
    case 8 : result.append("eight "); break;
    case 7 : result.append("seven "); break;
    case 6 : result.append("six "); break;
    case 5 : result.append("five "); break;
    case 4 : result.append("four "); break;
    case 3 : result.append("three "); break;
    case 2 : result.append("two "); break;
    case 1 : result.append("one "); break;
    case 0 : result.append(""); break;
    }
  }

  public static void main(String[] args) {
    System.out.println(intToEnglish(Integer.parseInt(args[0])));
  }

}
