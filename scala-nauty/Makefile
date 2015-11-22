.PHONY: all clean

JAVA_HOME=$(shell $$(dirname $$(readlink $$(which java)))/java_home)
JAVA_INCLUDES=-I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/darwin

all: Binding.class Binding.h libBinding.dylib

Binding.class: Binding.scala
	javac $^
Binding.h: Binding.class
	javah -jni $(^:.class=)
libBinding.dylib: binding.c Binding.h
	clang -Wall -Winline -Wshadow -Wextra $(JAVA_INCLUDES) -shared binding.c -o $@

clean:
	-rm Binding.class Binding.h libBinding.dylib
