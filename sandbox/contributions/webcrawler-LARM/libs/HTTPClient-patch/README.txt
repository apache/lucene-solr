This directory contains a patch to the HTTPClient which adds a maximum bytes
attribute to the getData() method. This method retrieves a file at once but
would crash the server if the remote file was too large. 

To compile it, download HTTPClient.jar from http://www.innovation.ch, extract it,
overwrite the source files with the ones in this folder, and compile it.

The file ../HTTPClient-0.3.3-patched.jar already contains a patched version

