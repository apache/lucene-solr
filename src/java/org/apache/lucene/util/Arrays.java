package org.apache.lucene.util;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
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

// copied from jdk 1.2b3 sources, so that we can use it in java 1.1

/**
 * This class contains various methods for manipulating arrays (such as
 * sorting and searching).  It also contains a static factory that allows
 * arrays to be viewed as Lists.
 *
 * @author  Josh Bloch
 * @version 1.17 03/18/98
 * @since   JDK1.2
 */

public class Arrays {
    /**
     * Sorts the specified array of objects into ascending order, according
     * to the <i>natural comparison method</i> of its elements.  All
     * elements in the array must implement the Comparable interface.
     * Furthermore, all elements in the array must be <i>mutually
     * comparable</i> (that is, e1.compareTo(e2) must not throw a
     * typeMismatchException for any elements e1 and e2 in the array).
     * <p>
     * This sort is guaranteed to be <em>stable</em>:  equal elements will
     * not be reordered as a result of the sort.
     * <p>
     * The sorting algorithm is a modified mergesort (in which the merge is
     * omitted if the highest element in the low sublist is less than the
     * lowest element in the high sublist).  This algorithm offers guaranteed
     * n*log(n) performance, and can approach linear performance on nearly
     * sorted lists.
     * 
     * @param a the array to be sorted.
     * @exception ClassCastException array contains elements that are not
     *		  <i>mutually comparable</i> (for example, Strings and
     *		  Integers).
     * @see Comparable
     */
    public static void sort(String[] a) {
        String aux[] = (String[])a.clone();
        mergeSort(aux, a, 0, a.length);
    }

    private static void mergeSort(String src[], String dest[],
                                  int low, int high) {
	int length = high - low;

	// Insertion sort on smallest arrays
	if (length < 7) {
	    for (int i=low; i<high; i++)
		for (int j=i; j>low && (dest[j-1]).compareTo(dest[j])>0; j--)
		    swap(dest, j, j-1);
	    return;
	}

        // Recursively sort halves of dest into src
        int mid = (low + high)/2;
        mergeSort(dest, src, low, mid);
        mergeSort(dest, src, mid, high);

        // If list is already sorted, just copy from src to dest.  This is an
        // optimization that results in faster sorts for nearly ordered lists.
        if ((src[mid-1]).compareTo(src[mid]) <= 0) {
           System.arraycopy(src, low, dest, low, length);
           return;
        }

        // Merge sorted halves (now in src) into dest
        for(int i = low, p = low, q = mid; i < high; i++) {
            if (q>=high || p<mid && (src[p]).compareTo(src[q])<=0)
                dest[i] = src[p++];
            else
                dest[i] = src[q++];
        }
    }

    /**
     * Swaps x[a] with x[b].
     */
    private static void swap(String x[], int a, int b) {
	String t = x[a];
	x[a] = x[b];
	x[b] = t;
    }
}











