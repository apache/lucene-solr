# rules to enable the running of "make jar" and the like from any dir..

# directories containing java source code
DIRS = store util document analysis analysis/standard index search queryParser
PACKAGES = $(subst /,.,$(patsubst %,com.lucene.%,$(DIRS)))

ifeq ($(JAVALINK),) 
  JAVALINK = http://java.sun.com/products/jdk/1.3/docs/api/
endif

# OLDJAVA does not have a -link option
ifeq ($(OLDJAVA),)
  JLINK_OPT = -link $(JAVALINK)
  JAR_CMD = $(JAR) -cvfm lucene.jar com/lucene/manifest
else
  JAR_CMD = $(JAR) -cvf lucene.jar
endif

.PHONY: jar doc demo release

jar:	all_classes
	cd $(ROOT) && $(JAR_CMD) \
	 `ls com/lucene/*/*.class` `ls com/lucene/*/*/*.class`

doc:	all_classes
	if [ -d $(ROOT)/doc/api ]; then rm -rf $(ROOT)/doc/api ;fi
	mkdir $(ROOT)/doc/api
	$(JAVADOC) -classpath '$(CLASSPATH)' -author -version \
	 -d $(ROOT)/doc/api $(JLINK_OPT) $(PACKAGES)

demo: all_classes
	$(MAKE) -C $(ROOT)/demo/HTMLParser -w
	$(MAKE) -C $(ROOT)/demo -w CLASSPATH=..

release: jar demo doc
	cd $(ROOT) && tar cvf lucene.tar lucene.jar doc/*.html doc/api \
	   demo/*.java demo/*.class demo/*.html demo/*.jhtml \
	   demo/HTMLParser/*.class demo/HTMLParser/*.jj \
	   demo/HTMLParser/*.java

# make all the Lucene classes 
all_classes : TARGET = classes
all_classes : $(DIRS)

.PHONY: $(DIRS)
$(DIRS):
	$(MAKE) -C $(ROOT)/com/lucene/$@ -w $(TARGET)

# Removes all generated files from src directories.
src_clean: TARGET = clean
src_clean: $(DIRS) clean

# Removes all generated files.
real_clean: DIRS += demo
real_clean: DIRS += demo/HTMLParser
real_clean: TARGET = clean
real_clean: $(DIRS) clean
	cd $(ROOT) && rm -rf lucene.jar lucene.tar doc/api
