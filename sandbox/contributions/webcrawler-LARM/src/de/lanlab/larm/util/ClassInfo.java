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

package de.lanlab.larm.util;

import java.lang.reflect.*;
import java.io.*;
import java.util.*;

/**
 *  prints class information with the reflection api
 *  for debugging only
 */
public class ClassInfo
{

    public ClassInfo()
    {
    }

    /**
     * Usage: java ClassInfo PackageName.MyNewClassName PackageName.DerivedClassName
     */
    public static void main(String[] args)
    {

        String name = args[0];
        String derivedName = args[1];
        LinkedList l = new LinkedList();
        ListIterator itry = l.listIterator();

        try
        {
            Class cls = Class.forName(name);
            name = cls.getName();
            String pkg =  getPackageName(name);
            String clss = getClassName(name);

            StringWriter importsWriter = new StringWriter();
            PrintWriter imports = new PrintWriter(importsWriter);
            StringWriter outWriter = new StringWriter();
            PrintWriter out = new PrintWriter(outWriter);

            TreeSet importClasses = new TreeSet();
            importClasses.add(getImportStatement(name));

            out.println("/**\n * (class description here)\n */\npublic class " + derivedName + " " + (cls.isInterface() ? "implements " : "extends ") + clss + "\n{");

            Method[] m = cls.getMethods();
            for(int i= 0; i< m.length; i++)
            {
                Method thism = m[i];
                if((thism.getModifiers() & Modifier.PRIVATE) == 0 && ((thism.getModifiers() & Modifier.FINAL) == 0)
                   && (thism.getDeclaringClass().getName() != "java.lang.Object"))
                {
                    out.println("    /**");
                    out.println("     * (method description here)");
                    out.println("     * defined in " + thism.getDeclaringClass().getName());

                    Class[] parameters = thism.getParameterTypes();
                    for(int j = 0; j < parameters.length; j ++)
                    {
                        if(getPackageName(parameters[j].getName()) != "")
                        {
                            importClasses.add(getImportStatement(parameters[j].getName()));
                        }
                        out.println("     * @param p" + j + " (parameter description here)");
                    }

                    if(thism.getReturnType().getName() != "void")
                    {
                        String returnPackage = getPackageName(thism.getReturnType().getName());
                        if(returnPackage != "")
                        {
                            importClasses.add(getImportStatement(thism.getReturnType().getName()));
                        }
                        out.println("     * @return (return value description here)");
                    }

                    out.println("     */");

                    out.print("    " + getModifierString(thism.getModifiers()) + getClassName(thism.getReturnType().getName()) + " ");
                    out.print(thism.getName() + "(");

                    for(int j = 0; j < parameters.length; j ++)
                    {
                        if(j>0)
                        {
                            out.print(", ");
                        }
                        out.print(getClassName(parameters[j].getName()) + " p" + j);
                    }
                    out.print(")");
                    Class[] exceptions = thism.getExceptionTypes();

                    if (exceptions.length > 0)
                    {
                       out.print(" throws ");
                    }

                    for(int k = 0; k < exceptions.length; k++)
                    {
                       if(k > 0)
                       {
                           out.print(", ");
                       }
                       String exCompleteName = exceptions[k].getName();
                       String exName = getClassName(exCompleteName);
                       importClasses.add(getImportStatement(exCompleteName));

                       out.print(exName);
                    }
                    out.print("\n" +
                              "    {\n" +
                              "        /**@todo: Implement this " + thism.getName() + "() method */\n" +
                              "        throw new UnsupportedOperationException(\"Method " + thism.getName() + "() not yet implemented.\");\n" +
                              "    }\n\n");


                }
            }
            out.println("}");

            Iterator importIterator = importClasses.iterator();
            while(importIterator.hasNext())
            {
                String importName = (String)importIterator.next();
                if(!importName.startsWith("java.lang"))
                {
                    imports.println("import " + importName + ";");
                }
            }

            out.flush();
            imports.flush();

            if(getPackageName(derivedName) != "")
            {
                System.out.println("package " + getPackageName(derivedName) + ";\n");
            }
            System.out.println( "/**\n" +
                                " * Title:        \n" +
                                " * Description:\n" +
                                " * Copyright:    Copyright (c)\n" +
                                " * Company:\n" +
                                " * @author\n" +
                                " * @version 1.0\n" +
                                " */\n");
            System.out.println(importsWriter.getBuffer());
            System.out.print(outWriter.getBuffer());
        }
        catch(Throwable t)
        {
            t.printStackTrace();
        }
    }

    public static String getPackageName(String className)
    {
        if(className.charAt(0) == '[')
        {
            switch(className.charAt(1))
            {
                case 'L':
                     return getPackageName(className.substring(2,className.length()-1));
                default:
                    return "";
            }
        }
        String name = className.lastIndexOf(".") != -1 ? className.substring(0, className.lastIndexOf(".")) : "";
        //System.out.println("Package: " + name);
        return name;
    }

    public static String getClassName(String className)
    {
        if(className.charAt(0) == '[')
        {
            switch(className.charAt(1))
            {
                case 'L':
                     return getClassName(className.substring(2,className.length()-1)) + "[]";
                case 'C':
                     return "char[]";
                case 'I':
                     return "int[]";
                case 'B':
                     return "byte[]";
                // rest is missing here

            }
        }
        String name = (className.lastIndexOf(".") > -1) ? className.substring(className.lastIndexOf(".")+1) : className;
        //System.out.println("Class: "  + name);
        return name;
    }

    static String getImportStatement(String className)
    {
        String pack = getPackageName(className);
        String clss = getClassName(className);
        if(clss.indexOf("[]") > -1)
        {
            return pack + "." + clss.substring(0,clss.length() - 2);
        }
        else
        {
            return pack + "." + clss;
        }
    }

    public static String getModifierString(int modifiers)
    {
        StringBuffer mods = new StringBuffer();
        if((modifiers & Modifier.ABSTRACT) != 0)
        {
            mods.append("abstract ");
        }
        if((modifiers & Modifier.FINAL) != 0)
        {
            mods.append("final ");
        }
        if((modifiers & Modifier.INTERFACE) != 0)
        {
            mods.append("interface ");
        }
        if((modifiers & Modifier.NATIVE) != 0)
        {
            mods.append("native ");
        }
        if((modifiers & Modifier.PRIVATE) != 0)
        {
            mods.append("private ");
        }
        if((modifiers & Modifier.PROTECTED) != 0)
        {
            mods.append("protected ");
        }
        if((modifiers & Modifier.PUBLIC) != 0)
        {
            mods.append("public ");
        }
        if((modifiers & Modifier.STATIC) != 0)
        {
            mods.append("static ");
        }
        if((modifiers & Modifier.STRICT) != 0)
        {
            mods.append("strictfp ");
        }
        if((modifiers & Modifier.SYNCHRONIZED) != 0)
        {
            mods.append("synchronized ");
        }
        if((modifiers & Modifier.TRANSIENT) != 0)
        {
            mods.append("transient ");
        }
        if((modifiers & Modifier.VOLATILE) != 0)
        {
            mods.append("volatile ");
        }
        return mods.toString();
    }


}