/*
 * $Id$
 * 
 * Copyright 1997 Hewlett-Packard Company
 * 
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 */

package hplb.xml;

import java.util.Hashtable;

/**
 * This class is responsible for maintaining strings as <em>atoms</em>,
 * i.e. if two strings returned by getAtom() are equal in the sense of
 * String.equal() then they are in fact the same Object. This is used to
 * "intern" element and attribute names which can then be compared using
 * the more efficient reference equality, a la "s1==s2".
 * 
 * @author  Anders Kristensen
 */
public final class Atom {
  /** Holds atoms: element names (GIs), and attribute names. */
  private static final Hashtable atoms = new Hashtable();
  
    /**
     * Return an atom corresponding to the argument.
     */
    public static String getAtom(String s) {
        synchronized (atoms) {
            String a = (String) atoms.get(s);
            if (a == null) {
                atoms.put(s, s);
                a = s;
            }
        return a;
        }
    }
}
