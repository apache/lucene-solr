/*****************************************************************************
 * Copyright (C) The Apache Software Foundation. All rights reserved.        *
 * ------------------------------------------------------------------------- *
 * This software is published under the terms of the Apache Software License *
 * version 1.1, a copy of which has been included  with this distribution in *
 * the LICENSE file.                                                         *
 *****************************************************************************/

package org.krysalis.centipede.ant.task;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import org.apache.tools.ant.*;
import org.apache.tools.ant.taskdefs.*;
import org.apache.tools.ant.types.*;

/**
 * Will set the given property if the requested class is available in the
 * specified classpath. The found class is not loaded!
 * This class is heavily based on the available task in the ant package:
 * @author Stefano Mazzocchi <a href="mailto:stefano@apache.org">stefano@apache.org</a>
 *
 * This task searches only in the defined path but not in the parents path
 * unless explicitly overridden by the value of ${build.sysclasspath}
 * like the original available task does.
 * @author <a href="mailto:cziegeler@apache.org">Carsten Ziegeler</a>
 * @version CVS $Revision$ $Date$
 */

public class ClassAvailableTask extends Task {

    /**
     * A hashtable of zip files opened by the classloader
     */
    private Hashtable zipFiles = new Hashtable();

    private String property;
    private String classname;
    private Path classpath;
    private String value = "true";

    public void setClasspath(Path classpath) {
        createClasspath().append(classpath);
    }

    public Path createClasspath() {
        if (this.classpath == null) {
            this.classpath = new Path(this.project);
        }
        return this.classpath.createPath();
    }

    public void setClasspathRef(Reference r) {
        createClasspath().setRefid(r);
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setClassname(String classname) {
        if (!"".equals(classname)) {
            this.classname = classname;
        }
    }

    public void execute() throws BuildException {
        if (property == null) {
            throw new BuildException("property attribute is required", location);
        }

        if (eval()) {
            this.project.setProperty(property, value);
        }
    }

    public boolean  eval() throws BuildException {
        if (classname == null) {
            throw new BuildException("At least one of (classname|file|resource) is required", location);
        }

        if (classpath != null) {
            classpath.setProject(project);
            classpath = classpath.concatSystemClasspath("ignore");
        }

        if (!findClassInComponents(classname)) {
            log("Unable to load class " + classname + " to set property " + property, Project.MSG_VERBOSE);
            return false;
        }

        return true;
    }

    /**
     * Get an inputstream to a given resource in the given file which may
     * either be a directory or a zip file.
     *
     * @param file the file (directory or jar) in which to search for the resource.
     * @param resourceName the name of the resource for which a stream is required.
     *
     * @return a stream to the required resource or null if the resource cannot be
     * found in the given file object
     */
    private boolean contains(File file, String resourceName) {
        try {
            if (!file.exists()) {
                return false;
            }

            if (file.isDirectory()) {
                File resource = new File(file, resourceName);

                if (resource.exists()) {
                    return true;
                }
            }
            else {
                // is the zip file in the cache
                ZipFile zipFile = (ZipFile)zipFiles.get(file);
                if (zipFile == null) {
                    zipFile = new ZipFile(file);
                    zipFiles.put(file, zipFile);
                }
                ZipEntry entry = zipFile.getEntry(resourceName);
                if (entry != null) {
                    return true;
                }
            }
        }
        catch (Exception e) {
            log("Ignoring Exception " + e.getClass().getName() + ": " + e.getMessage() +
                " reading resource " + resourceName + " from " + file, Project.MSG_VERBOSE);
        }

        return false;
    }

    /**
     * Find a class on the given classpath.
     */
    private boolean findClassInComponents(String name) {
        // we need to search the components of the path to see if we can find the
        // class we want.
        final String classname = name.replace('.', '/') + ".class";
        final String[] list = classpath.list();
        boolean found = false;
        int i = 0;
        while (i < list.length && found == false) {
            final File pathComponent = (File)project.resolveFile(list[i]);
            found = this.contains(pathComponent, classname);
            i++;
        }
        return found;
    }
}
