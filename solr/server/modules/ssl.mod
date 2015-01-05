#
# SSL Keystore module
#

[depend]
server

[xml]
etc/jetty-ssl.xml

[files]
http://git.eclipse.org/c/jetty/org.eclipse.jetty.project.git/plain/jetty-server/src/main/config/etc/keystore|etc/keystore

[ini-template]
### SSL Keystore Configuration
# define the port to use for secure redirection
jetty.secure.port=8443

## Setup a demonstration keystore and truststore
jetty.keystore=etc/keystore
jetty.truststore=etc/keystore

## Set the demonstration passwords.
## Note that OBF passwords are not secure, just protected from casual observation
## See http://www.eclipse.org/jetty/documentation/current/configuring-security-secure-passwords.html
jetty.keystore.password=OBF:1vny1zlo1x8e1vnw1vn61x8g1zlu1vn4
jetty.keymanager.password=OBF:1u2u1wml1z7s1z7a1wnl1u2g
jetty.truststore.password=OBF:1vny1zlo1x8e1vnw1vn61x8g1zlu1vn4

### Set the client auth behavior
## Set to true if client certificate authentication is required
# jetty.ssl.needClientAuth=true
## Set to true if client certificate authentication is desired
# jetty.ssl.wantClientAuth=true

## Parameters to control the number and priority of acceptors and selectors
# ssl.selectors=1
# ssl.acceptors=1
# ssl.selectorPriorityDelta=0
ssl.acceptorPriorityDelta=0
