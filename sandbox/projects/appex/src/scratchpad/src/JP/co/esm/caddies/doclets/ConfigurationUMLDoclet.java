/*
   Copyright (C) 1999
   Eiwa System Management, Inc.

   Permission to use, copy, modify, distribute and sell this software
   and its documentation for any purpose is hereby granted without fee,
   provided that the above copyright notice appear in all copies and
   that both that copyright notice and this permission notice appear
   in supporting documentation. Eiwa System Management,Inc.
   makes no representations about the suitability of this software for any
   purpose.  It is provided "AS IS" with NO WARRANTY.
*/

package JP.co.esm.caddies.doclets;

import com.sun.tools.doclets.Configuration;
import com.sun.tools.doclets.standard.*;
import com.sun.tools.doclets.*;

import com.sun.javadoc.*;
import java.util.*;
import java.io.*;

/**
  * configuration of UMDoclet
  * 
  * @author Taisuke Fukuno
  */
public class ConfigurationUMLDoclet extends Configuration {
    boolean withstandard = true;
    boolean packagename = true;
    String windowtitle = null;
	String interfacecolor = "fcfb6f";
	String classcolor = "7dfff1";
	String abstractclasscolor = "92ff7d";
	String finalclasscolor = "99adfc";
    List tags = new ArrayList();
    boolean nostereotype = false;
	static final int PIVOT = 0;
	static final int SUB = 1;
	static final int SUPER = 2;
	static final int INNER = 3;
	static final int OUTER = 4;
	int[] detail = new int[] { 2, 2, 2, 0, 2 }; // pivot sub super inner outer
    public static ResourceBundle message = null;
    ConfigurationUMLDoclet() {
    	message = ResourceBundle.getBundle("JP.co.esm.caddies.doclets.resources.UMLDoclet");
	}
	
    public void setSpecificDocletOptions(RootDoc root) throws DocletAbortException {
        String[][] options = root.options();
        for (int oi = 0; oi < options.length; ++oi) {
            String[] os = options[oi];
            String opt = os[0].toLowerCase();
            if (opt.equals("-member")) {
				String s = os[1];
				for (int i = 0; i < 5; i++)
					detail[i] = s.charAt(i) - '0';
            } else if (opt.equals("-nopackage")) {
                packagename = false;
            } else if (opt.equals("-nostereotype")) {
                nostereotype = true;
            } else if (opt.equals("-windowtitle")) {
                windowtitle = os[1];
            } else if (opt.equals("-nostandard")) {
                withstandard = false;
            } else if (opt.equals("-interfacecolor")) {
				interfacecolor = os[1];
            } else if (opt.equals("-classcolor")) {
				classcolor = os[1];
            } else if (opt.equals("-abstractclasscolor")) {
				abstractclasscolor = os[1];
            } else if (opt.equals("-finalclasscolor")) {
				finalclasscolor = os[1];
		    } else if (opt.equals("-tag")) {
	        	tags.add(os[1]);
            }
        }
	}
    public int specificDocletOptionLength(String option) {
		if (option.equals("-nopackage") || option.equals("-nostandard") || option.equals("-nostereotype"))
			return 1;
		else if (option.equals("-member") ||
				 option.equals("-windowtitle") ||
				 option.equals("-interfacecolor") ||
				 option.equals("-classcolor") ||
				 option.equals("-abstractclasscolor") ||
				 option.equals("-finalclasscolor") ||
				 option.equals("-tag"))
			return 2;
		else if (option.equals("-help")) {
			System.err.println(message.getString("doclet.usage"));
			return -1;
		} else
			return Standard.optionLength(option);
	}

    public int optionLength(String option) {
		if (option.equals("-nopackage") || option.equals("-nostandard") || option.equals("-nostereotype"))
			return 1;
		else if (option.equals("-member") ||
				 option.equals("-windowtitle") ||
				 option.equals("-interfacecolor") ||
				 option.equals("-classcolor") ||
				 option.equals("-abstractclasscolor") ||
				 option.equals("-finalclasscolor") ||
				 option.equals("-tag"))
			return 2;
		else if (option.equals("-help")) {
			System.err.println(message.getString("doclet.usage"));
			return -1;
		} else
			return Standard.optionLength(option);
	}
	
    public boolean specificDocletValidOptions(String[][] options, DocErrorReporter reporter) {
		boolean standard = true;
		for (int i = 0; i < options.length; i++) {
			if (options[i][0].equals("-nostandard"))
				standard = false;
			else if (options[i][0].equals("-member")) {
				String s = options[i][1];
				if (s.length() != 5) {
					reporter.printError(message.getString("doclet.illegal_member"));
					return false;
				}
				for (int j = 0; j < 5; j++) {
					char c = s.charAt(j);
					if (c < '0' || c > '3') {
						reporter.printError(message.getString("doclet.illegal_member"));
						return false;
					}
				}
			}
		}
        if (standard) {
			boolean res = false;
			try {
				res = Standard.validOptions(options, reporter);
			} catch (IOException e) {
				reporter.printError(e.getMessage());
			}
			return res;
		}
		return true;
	}
}
