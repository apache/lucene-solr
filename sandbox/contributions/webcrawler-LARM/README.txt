$Id$

This is the README file for webcrawler-LARM contribution to Lucene Sandbox.

This contribution requires:

a) HTTPClient.jar (not Jakarta's, but this one:
    http://www.innovation.ch/java/HTTPClient/
b) Jakarta ORO package for regular expressions

Put the .jars into the lib directory. 

Some of the HTTPClient source files will be replaced during the build, so they 
will be needed during the build. Sorry, I remember I couldn't do that with
inheritance.

- This contribution also uses portions of the HeX HTML parser, which is
included.

OG>  I am not sure if Clemens' modified this parser in any way.  If not,
OG>  maybe we don't have to include it and can instead just add it to the
OG>  list of required packages.

The parser was put upside down. Although it apparently still needs some 
of the original interfaces, most of them can probably be removed. I will check
that out.

OG>  This code requires(?) JDK 1.4, as it uses assert keyword.

No. It still contains a method called assert() for testing. I will probably 
rename this sometime (e.g. when changing the tests to JUnit).

$Id$