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

import com.sun.javadoc.*;
import com.sun.tools.javadoc.*;
import com.sun.tools.doclets.*;
import com.sun.tools.doclets.standard.*;
import java.util.*;
import java.io.*;

/**
  * This is Doclet generates UML class figure HTML
  * 
  * @author Taisuke Fukuno
  * @author Nicola Ken Barozzi nicolaken@apache.org
  */
public class UMLDoclet {
	static final String PREFIX = "uml-";
	
    static ConfigurationUMLDoclet configuration;
    static ConfigurationUMLDoclet configuration() {
		if (configuration == null)
			configuration = new ConfigurationUMLDoclet();
		return configuration;
	}
    public static int optionLength(String option) {
        return configuration().optionLength(option);
    }
    public static boolean validOptions(String options[][], DocErrorReporter reporter) throws IOException {
        return configuration().specificDocletValidOptions(options, reporter);
    }
    public static boolean start(RootDoc root) throws IOException {
        try {
            configuration().setOptions(root);
            new UMLDoclet().startGeneration(root);
	        return true;
        } catch (DocletAbortException exc) {
        }
        return false;
    }
	public void startGeneration(RootDoc root) throws DocletAbortException {
		ClassDoc[] clss = root.classes();
		if (clss.length == 0) {
            root.printNotice(configuration().message.getString("doclet.No_Public_Classes_To_Document"));
            return;
        }
        
        try {
	        String dest = configuration().destdirname;
	        if (dest.length() > 0)
	        	dest += File.separatorChar;
	        String imgpath = dest + "images" + File.separator;
	        makeContainer(imgpath);
	        writeToFile(EXT_IMAGE, imgpath + "ext.gif");
	        writeToFile(INNER_IMAGE, imgpath + "inner.gif");
	        writeToFile(OUTER_IMAGE, imgpath + "outer.gif");
	        
			for (int i = 0; i < clss.length; i++) {
				writeClass(clss[i], clss);
			}

		} catch (IOException e) {
			System.out.println(e);//throw new DocletAbortException(e.getMessage());
		}
	}
	void writeClass(ClassDoc thiscls, ClassDoc[] clss) throws IOException {
		// inners
		ClassDoc[] ins = thiscls.innerClasses();
		List inners = new ArrayList();
		for (int i = 0; i < ins.length; i++)
			inners.add(ins[i]);
		
		// supers
		List supers = new ArrayList();
		ClassDoc supercls = thiscls.superclass();
		if (supercls != null)
			supers.add(supercls);
		ClassDoc[] ifaces = thiscls.interfaces();
		for (int i = 0; i < ifaces.length; i++)
			supers.add(ifaces[i]);
		
		// subs
		List subs = new ArrayList();
		if (thiscls.isInterface()) {
			for (int j = 0; j < clss.length; j++) {
				ClassDoc[] ifa = clss[j].interfaces();
				for (int k = 0; k < ifa.length; k++) {
					if (ifa[k] == thiscls)
						subs.add(clss[j]);
				}
			}
		}
		for (int j = 0; j < clss.length; j++)
			if (clss[j].superclass() == thiscls)
				subs.add(clss[j]);
		
		// outers
		ClassDoc outer = null;
	FIND_OUTER:
		for (int j = 0; j < clss.length; j++) {
			ClassDoc[] in = clss[j].innerClasses();
			for (int k = 0; k < in.length; k++) {
				if (in[k] == thiscls) {
					outer = clss[j];
					break FIND_OUTER;
				}
			}
		}
		
		// write html
        String destdir = configuration().destdirname.length() > 0 ? configuration().destdirname + File.separatorChar : "";
        String pkg = thiscls.containingPackage().name();
        String dstpath = "";
        if (pkg.length() > 0) {
	        destdir += pkg.replace('.', File.separatorChar) + File.separatorChar;
	        StringBuffer sb = new StringBuffer("../");
	        for (int i = 0; i < pkg.length(); i++)
				if (pkg.charAt(i) == '.')
					sb.append("../");
			dstpath = sb.toString();
	    }
	    String imgpath = "<img src=" + dstpath + "images/";
        String fn = destdir + PREFIX + thiscls.name() + ".html";
		makeContainer(fn);
		OutputStream os = new FileOutputStream(fn);
        String docencoding = configuration().docencoding;
        OutputStreamWriter oswriter;
        if (docencoding == null)
            oswriter = new OutputStreamWriter(os);
        else
            oswriter = new OutputStreamWriter(os, docencoding);
        PrintWriter out = new PrintWriter(oswriter);
        out.println("<html><head><title>");
        if (configuration().windowtitle != null) {
			out.print(configuration().windowtitle + " : ");
		}
        out.print(thiscls.name());
        out.println("</title></head><body bgcolor=\"#ffffff\" text=\"#000000\" vlink=\"#330000\" alink=\"#330044\" link=\"#0000aa\">");
		
		out.println("<h2 class=\"thisclassname\">" + thiscls.name() + "</h2>");
		
		out.println("<table width=100% cellspacing=0 cellpadding=0>");
		
		// inners
		if (!inners.isEmpty()) {
			out.println("<tr><td width=32%></td><td width=12></td><td width=32% valign=bottom>");
			writeClasses(out, inners, configuration().INNER, dstpath);
			out.println("</td><td width=12></td><td width=32%></td></tr><tr><td width=32%></td><td width=12>");
			out.println("</td><td width=32% align=center valign=middle>");
			out.println(imgpath + "inner.gif>");
			out.println("</td><td width=12></td><td width=32%></td></tr>");
		}
		
		// supers
		out.println("<tr><td width=32% valign=top>");
		if (!supers.isEmpty())
			writeClasses(out, supers, configuration().SUPER, dstpath);
		out.println("</td><td width=12 align=center valign=top>");
		if (!supers.isEmpty())
			out.println(imgpath + "ext.gif>");
		
		// this
		out.println("</td><td width=32% valign=top align=center>");
		writeClass(out, thiscls, configuration().PIVOT, dstpath, true);
		
		// outer
		if (outer != null) {
			out.println(imgpath + "outer.gif>");
			writeClass(out, outer, configuration().OUTER, dstpath, false);
		}
		out.println("</td><td width=\"12\" align=\"center\" valign=\"top\">");

		// subs
		if (!subs.isEmpty())
			out.println(imgpath + "ext.gif");
		out.println("</td>");
		out.println("<td width=32% valign=top>");
		if (!subs.isEmpty())
			writeClasses(out, subs, configuration().SUB, dstpath);
		out.println("</td></tr>");
		
		out.println("</table></body></html>");
		out.close();
	}
	void writeClasses(PrintWriter out, List clss, int type, String dstpath) {
		if (clss.size() > 1) {
			out.print("<table border=\"1\"  width=100% cellpadding=0 cellspacing=3>");
			for (Iterator it = clss.iterator(); it.hasNext();) {
				ClassDoc c = (ClassDoc)it.next();
				if (c.isInterface())
					writeClassCore(out, c, type, dstpath, false);
			}
			for (Iterator it = clss.iterator(); it.hasNext();) {
				ClassDoc c = (ClassDoc)it.next();
				if (!c.isInterface())
					writeClassCore(out, c, type, dstpath, false);
			}
			out.println("</table>");
		} else
			writeClass(out, (ClassDoc)clss.get(0), type, dstpath, false);
	}
	void writeClass(PrintWriter out, ClassDoc cls, int type, String dstpath, boolean thiscls) {
		out.print("<table border=0 width=100% cellpadding=0 cellspacing=0>");
		writeClassCore(out, cls, type, dstpath, thiscls);
		out.println("</table>");
	}
	void writeClassCore(PrintWriter out, ClassDoc cls, int type, String dstpath, boolean thiscls) { // <tr>...</tr>
		out.print("<tr><td bgcolor=#");
		if (cls.isInterface())
			out.print(configuration().interfacecolor);
		else if (cls.isFinal())
			out.print(configuration().finalclasscolor);
		else if (cls.isAbstract())
			out.print(configuration().abstractclasscolor);
		else
			out.print(configuration().classcolor);
		out.println("><table border cellspacing=0 cellpadding=0 width=100%><tr><td>");
		if (configuration().packagename) {
			String pkgs = cls.containingPackage().name();
			if (pkgs.length() > 0)
				out.print(pkgs + ". ");
		}
		writeClassName(out, cls, dstpath, thiscls);
		writeClassTag(out, cls);
		
		int dt = configuration().detail[type];
		if (dt > 0) {
			FieldDoc[] fld = cls.fields();
			if (fld.length > 0) {
				out.println("</td></tr><tr><td>");
				for (int i = 0; i < fld.length; i++)
					writeField(out, fld[i], dt, dstpath);
			}
			ConstructorDoc[] con = cls.constructors();
			boolean sep = false;
			if (con.length > 0) {
				for (int i = 0; i < con.length; i++) {
					Parameter[] param = con[i].parameters();
					if (param.length == 1 && param[0].name().startsWith("this$"))
						continue;
					if (!sep) {
						out.println("</td></tr><tr><td>");
						sep = true;
					}
					writeConstructor(out, con[i], dt, dstpath);
				}
			}
			MethodDoc[] met = cls.methods();
			if (met.length > 0) {
				if (!sep)
					out.println("</td></tr><tr><td>");
				for (int i = 0; i < met.length; i++)
					writeMethod(out, met[i], dt, dstpath);
			}
		}
		out.println("</td></tr></table></td></tr>");
	}
	void writeClassTag(PrintWriter out, ClassDoc cls) {
		if (!configuration().nostereotype) {
			Tag[] stype = cls.tags("stereotype");
			for (int i = 0; i < stype.length; i++) {
				out.print(" &lt;&lt;" + stype[i].text() + "&gt;&gt;");
			}
		}
		Iterator it = configuration().tags.iterator();
		while (it.hasNext()) {
			Tag[] tag = cls.tags((String)it.next());
			for (int i = 0; i < tag.length; i++) {
				out.print(" {" + tag[i].kind().substring(1) + ":" + tag[i].text() + "}");
			}
		}
	}
	char getAccessChar(MemberDoc mem) {
		if (mem.isPublic()) {
			return '+';
		} else if (mem.isProtected()) {
			return '#';
		} else if (mem.isPrivate()) {
			return '-';
		}
		return ' ';
	}
	String getTypeName(Type type, int detail, String dstpath) {
		ClassDoc cls = type.asClassDoc();
		if (cls != null) {
			if (detail == 3) {
				return getLinkedClass(cls.qualifiedName(), cls, dstpath);
			}
			return getLinkedClass(cls.name(), cls, dstpath);
		}
		return type.toString();
	}
	void writeField(PrintWriter out, FieldDoc fld, int detail, String dstpath) {
		StringBuffer sb = new StringBuffer();
		sb.append(getAccessChar(fld));
		sb.append(getLinkedMember(fld.name(), fld, dstpath));
		sb.append(' ');
		sb.append(':');
		sb.append(getTypeName(fld.type(), detail, dstpath));
		writeMember(out, sb, fld);
	}
	void writeConstructor(PrintWriter out, ConstructorDoc con, int detail, String dstpath) {
		StringBuffer sb = new StringBuffer();
		sb.append(getAccessChar(con));
		sb.append(getLinkedMember(con.name(), con, dstpath));
		sb.append('(');
		if (detail >= 2) {
			Parameter[] param = con.parameters();
			for (int i = 0; i < param.length; i++) {
				if (i > 0) {
					sb.append(',');
					sb.append(' ');
				}
				sb.append(getTypeName(param[i].type(), detail, dstpath));
				String paramname = param[i].name();
				if (paramname.length() > 0) {
					sb.append(' ');
					sb.append(paramname);
				}
			}
		}
		sb.append(')');
		writeMember(out, sb, con);
	}
	void writeMethod(PrintWriter out, MethodDoc met, int detail, String dstpath) {
		StringBuffer sb = new StringBuffer();
		sb.append(getAccessChar(met));
		sb.append(getLinkedMember(encode(met.name()), met, dstpath));
		Parameter[] param = met.parameters();
		sb.append('(');
		if (detail >= 2) {
			for (int i = 0; i < param.length; i++) {
				if (i > 0) {
					sb.append(',');
					sb.append(' ');
				}
				sb.append(getTypeName(param[i].type(), detail, dstpath) + " " + param[i].name());
			}
		}
		sb.append(')');
		sb.append(' ');
		sb.append(':');
		sb.append(getTypeName(met.returnType(), detail, dstpath));
		writeMember(out, sb, met);
	}
	boolean isIncluded(ClassDoc cls) {
		ClassDoc c = cls;
		for (;;) {
			if (c.isIncluded())
				return true;
			c = c.containingClass();
			if (c == null)
				return false;
		}
	}
	String getLinkedMember(String name, MemberDoc mem, String dstpath) {
		if (!configuration().withstandard)
			return name;
		ClassDoc cls = mem.containingClass();
		if (!isIncluded(cls))
			return name;
		StringBuffer sb = new StringBuffer(name);
		StringBuffer ahref = new StringBuffer(mem.name());
		if (mem instanceof ExecutableMemberDoc) {
			ExecutableMemberDoc md = (ExecutableMemberDoc)mem;
			ahref.append('(');
			Parameter[] param = md.parameters();
			if (param.length > 0) {
				for (int i = 0; i < param.length - 1; i++) {
					ahref.append(param[i].type().toString());
					ahref.append(',');
				}
				ahref.append(param[0].type().toString());
			}
			ahref.append(')');
		}
		ahref.insert(0, ".html#");
		ahref.insert(0, cls.name());
		ahref.insert(0, getPath(cls.containingPackage()));
		ahref.insert(0, dstpath);
		ahref.insert(0, "<a href=");
		ahref.append(">");
		sb.insert(0, ahref);
		sb.append("</a>");
		return sb.toString();
	}
	void writeMember(PrintWriter out, StringBuffer sb, MemberDoc mem) {
		if (mem.isStatic()) {
			sb.insert(0, "<u>");
			sb.append("</u><br>");
		} else
			sb.append("<br>");
		out.println(sb.toString());
	}
	String getRelativePath(PackageDoc from, PackageDoc to) {
        String pkg = from.name();
        StringBuffer sb = new StringBuffer();
        if (pkg.length() > 0) {
	        sb.append("../");
	        for (int i = 0; i < pkg.length(); i++)
				if (pkg.charAt(i) == '.')
					sb.append("../");
	    }
	    sb.append(getPath(to));
		return sb.toString();
	}
	String getPath(PackageDoc pkgdoc) {
	    String pkg = pkgdoc.name();
	    if (pkg.length() > 0) {
			return pkg.replace('.', '/') + '/';
		}
		return "";
	}
	String getLinkedClass(String name, ClassDoc cls, String dstpath) {
		if (cls.isIncluded()) {
			StringBuffer sb = new StringBuffer();
			sb.append("<a href=");
			sb.append(dstpath + getPath(cls.containingPackage()) + PREFIX + cls.name() + ".html>");
			sb.append(name);
			sb.append("</a>");
			return sb.toString();
		}
		return name;
	}
	void writeClassName(PrintWriter out, ClassDoc cls, String dstpath, boolean thiscls) {
		if (cls.isIncluded()) {
			String prefix = PREFIX;
			if (thiscls) {
				if (!configuration().withstandard) {
					out.println(cls.name());
					return;
				}
				prefix = "";
			}
			out.print("<a href=" + dstpath + getPath(cls.containingPackage()) + prefix + cls.name() + ".html>");
			out.print(cls.name());
			out.println("</a>");
		} else
			out.println(cls.name());
	}
	// util
    static String encode(String s) {
		StringBuffer res = new StringBuffer();
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c == '>')
				res.append("&gt;");
			else if (c == '<')
				res.append("&lt;");
			else
				res.append(c);
		}
		return res.toString();
	}
    static void makeContainer(String fn) {
		//new File(fn).mkdirs();
	    int n = fn.lastIndexOf(File.separatorChar);
	    if (n < 0) {
	    	n = fn.lastIndexOf('/');
	        fn = fn.replace('/', File.separatorChar); // need?
	    }
	    if (n >= 0)
    	    new File(fn.substring(0, n)).mkdirs();
    }
    // image
    void writeToFile(String image, String dst) throws IOException {
		OutputStream os = new BufferedOutputStream(new FileOutputStream(dst));
		for (int i = 0; i < image.length() - 1; i++) {
			char c = image.charAt(i);
			int n = (int)c & 0xff;
			os.write(n);
			os.write((int)(c >> 8));
		}
		char c = image.charAt(image.length() - 1);
		if (c != '\u00ff')
			os.write((int)(c >> 8));
		os.close();
	}
	static final String EXT_IMAGE =
		"\u4947\u3846\u6139\u000C\u0012\u0080\u0000\u0000\uFFFF\u2CFF\u0000\u0000" +
		"\u000C\u0012\u0200\u8C1B\uA98F\uEDCB\u1F0F\u6070\u69B2\uCD95\u7328\u55C3" +
		"\u1562\u1778\u60D8\uA7D4\u0529\u2100\u0BFF\u444A\u4249\u3233\u2020\u2E31" +
		"\u3E30\u0A0A\u4947\u2046\u6E45\u6F63\u6564\u2072\u6F66\u2072\u6957\u336E" +
		"\u2032\u4A28\u4944\u3342\u2932\u4320\u706F\u7279\u6769\u7468\u3120\u3939" +
		"\u2D36\u3931\u3739\u4420\u494F\u6863\u6E61\u0A21\u3B00\u00ff";
	static final String INNER_IMAGE =
		"\u4947\u3846\u6139\u0040\u0012\u0080\u0000\u0000\uFFFF\u2CFF\u0000\u0000" +
		"\u0040\u0012\u0200\u8C50\uA98F\uC08B\uA30F\u6E9C\u8BDA\uDE5B\uD5FC\u760E" +
		"\u17DF\u1196\uA669\uAA10\uC9B6\u3818\u2540\uB38F\uBB71\u51DE\u31E3\uE31D" +
		"\u7505\u1F3B\u38EC\uB1BB\u4288\uF0A3\u62F8\uA032\u6ACE\u2970\u5EA5\u499B" +
		"\u549F\uFD81\uC286\u5162\u45D9\u673E\u9DD2\u0002\uFF21\u4A0B\u4944\u3342" +
		"\u2032\u3120\u302E\u0A3E\u470A\u4649\u4520\u636E\u646F\u7265\u6620\u726F" +
		"\u5720\u6E69\u3233\u2820\u444A\u4249\u3233\u2029\u6F43\u7970\u6972\u6867" +
		"\u2074\u3931\u3639\u312D\u3939\u2037\u4F44\u6349\u6168\u216E\n\u3B00";
	static final String OUTER_IMAGE =
		"\u4947\u3846\u6139\u0040\u0012\u0080\u0000\u0000\uFFFF\u2CFF\u0000\u0000" +
		"\u0040\u0012\u0200\u8C54\uA98F\uC08B\uA30F\u6E9C\u8BDA\uDE5B\uD5FC\u760E" +
		"\u485F\u235E\u9674\uCA5C\u2AB0\u01F6\u11B2\uB307\uDF7D\u4A2F\u4E1F\u015D" +
		"\u24EE\u9F41\u18CD\uCD94\u435C\uB1E3\u7C27\u9B3A\u1EAD\uF694\u1EB3\uD499" +
		"\u6E98\u5569\uA96A\u2E0A\u6439\u8B3E\u5DD4\u3376\u29CD\u0000\uFF21\u4A0B" +
		"\u4944\u3342\u2032\u3120\u302E\u0A3E\u470A\u4649\u4520\u636E\u646F\u7265" +
		"\u6620\u726F\u5720\u6E69\u3233\u2820\u444A\u4249\u3233\u2029\u6F43\u7970" +
		"\u6972\u6867\u2074\u3931\u3639\u312D\u3939\u2037\u4F44\u6349\u6168\u216E" +
		"\n\u3B00";
}
