$Id$

This is the README file for webcrawler-LARM contribution to Lucene Sandbox.


- This contribution requires:
  a) HTTPClient (not Jakarta's, but this one:
    http://www.innovation.ch/java/HTTPClient/
b) Jakarta ORO package for regular expressions

- The original archive file that I got from Clemens had ORO and
HTTPClient in lib directory.  I don't think we should include those
there, so I took them out.

- This contribution also uses 3rd party (X?)HTML parser, which is
included.
  I am not sure if Clemens' modified this parser in any way.  If not,
maybe we don't have to include it and can instead just add it to the
list of required packages.

- This code requires(?) JDK 1.4, as it uses assert keyword.


$Id$