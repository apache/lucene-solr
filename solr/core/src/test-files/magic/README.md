The two binary files were created by the following commands:

```bash
echo "Hello" > hello.txt && \
  tar -cvf hello.tar.bin hello.txt && \
  rm hello.txt

cp HelloWorld.java.txt HelloWorld.java && \
  javac HelloWorld.java && \
  mv HelloWorld.class HelloWorldJavaClass.class.bin && \
  rm HelloWorld.java
```