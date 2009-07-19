======================================================================

DESCRIPTION:

** NOTICE ** THIS VERSION OF THE SOFTWARE IS "BETA," MEANING IT IS IS
NOT YET READY FOR USE IN A PRODUCTION ENVIRONEMNT. IT IS MADE
AVAILABLE FOR PREVIEW AND TESTING PURPOSES ONLY.

This is Version 2 of JsDoc Toolkit, an automatic documentation
generation tool for JavaScript. It is written in JavaScript and is run
from a command line (or terminal) using the Java runtime engine.

Using this tool you can automatically turn JavaDoc-like comments in
your JavaScript source code into published output files, such as HTML
or XML.

For more information, to report a bug, or to browse the technical
documentation for this tool please visit the official JsDoc Toolkit
project homepage at http://code.google.com/p/jsdoc-toolkit/

For the most up-to-date documentation on Version 2 of JsDoc Toolkit
see the Verion 2 wiki at http://jsdoctoolkit.org/wiki


======================================================================

REQUIREMENTS:

JsDoc Toolkit is known to work with:
java version "1.6.0_03"
Java(TM) SE Runtime Environment (build 1.6.0_03-b05)
on Windows XP,
and java version "1.5.0_13"
Java(TM) 2 Runtime Environment, Standard Edition (build 1.5.0_13-b05-241)
on Mac OS X 10.4.

Other versions of java may or may not work with JsDoc Toolkit.

======================================================================

USAGE:

Running JsDoc Toolkit requires you to have Java installed on your
computer. For more information see http://www.java.com/getjava/

Before running the JsDoc Toolkit app you should change your current
working directory to the jsdoc-toolkit folder. Then follow the
examples below, or as shown on the project wiki.

On a computer running Windows a valid command line to run JsDoc
Toolkit might look like this:

> java -jar jsrun.jar app\run.js -a -t=templates\jsdoc mycode.js

On Mac OS X or Linux the same command would look like this:

$ java -jar jsrun.jar app/run.js -a -t=templates/jsdoc mycode.js

The above assumes your current working directory contains jsrun.jar,
the "app" and "templates" subdirectories from the standard JsDoc
Toolkit distribution and that the relative path to the code you wish
to document is "mycode.js".

The output documentation files will be saved to a new directory named
"out" (by default) in the current directory, or if you specify a
-d=somewhere_else option, to the somewhere_else directory.

For help (usage notes) enter this on the command line:

$ java -jar jsrun.jar app/run.js --help

More information about the various command line options used by JsDoc
Toolkit are available on the project wiki.

======================================================================

TESTING:

To run the suite of unit tests included with JsDoc Toolkit enter this
on the command line:

$ java -jar jsrun.jar app/run.js -T

To see a dump of the internal data structure that JsDoc Toolkit has
built from your source files use this command:

$ java -jar jsrun.jar app/run.js mycode.js -Z


======================================================================

LICENSE:

JSDoc.pm

This project is based on the JSDoc.pm tool, created by Michael
Mathews and Gabriel Reid. More information on JsDoc.pm can
be found on the JSDoc.pm homepage: http://jsdoc.sourceforge.net/

Complete documentation on JsDoc Toolkit can be found on the project
wiki at http://code.google.com/p/jsdoc-toolkit/w/list

Rhino

Rhino (JavaScript in Java) is open source and licensed by Mozilla
under the MPL 1.1 or later/GPL 2.0 or later licenses, the text of
which is available at http://www.mozilla.org/MPL/

You can obtain the source code for Rhino from the Mozilla web site at
http://www.mozilla.org/rhino/download.html

JsDoc Toolkit is a larger work that uses the Rhino JavaScript engine
but is not derived from it in any way. The Rhino library is used 
without modification and without any claims whatsoever.

The Rhino Debugger

You can obtain more information about the Rhino Debugger from the 
Mozilla web site at http://www.mozilla.org/rhino/debugger.html

JsDoc Toolkit is a larger work that uses the Rhino Debugger but
is not derived from it in any way. The Rhino Debugger is used
without modification and without any claims whatsoever.

JsDoc Toolkit

All code specific to JsDoc Toolkit are free, open source and licensed
for use under the X11/MIT License.

JsDoc Toolkit is Copyright (c)2008 Michael Mathews <micmath@gmail.com>

This program is free software; you can redistribute it and/or
modify it under the terms below.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions: The above copyright notice and this
permission notice must be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
