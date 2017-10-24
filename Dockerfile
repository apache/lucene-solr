FROM frekele/ant
WORKDIR /usr/src/app
CMD [ "mkdir /root/.ant/;","mkdir /root/.ant/lib;","cd /usr/src/app;","ant ivy-bootstrap;" ]