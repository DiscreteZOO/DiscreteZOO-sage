#include<stdio.h>
#include<jni.h>

#include"Binding.h"

JNIEXPORT jstring JNICALL Java_Binding_foo(JNIEnv *env, jclass thisclass, jint a, jint b, jint c, jstring s)
{
	const char *sbytes = (*env)->GetStringUTFChars(env,s,NULL);
	char ret[4096];
	snprintf(ret,sizeof(ret),"arg was: %s, with numbers %d, %d, %d",sbytes,a,b,c);
	(*env)->ReleaseStringUTFChars(env,s,sbytes);
	return (*env)->NewStringUTF(env,ret);
}

