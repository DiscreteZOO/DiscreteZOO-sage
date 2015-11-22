#include <stdio.h>
#include <stdlib.h>

#include <map>
#include <set>
#include <utility>
#include <vector>

#include <jni.h>

#include"binding.h"

extern "C"
{
	#include "nausparse.h" // includes nauty.h
	#include "gtools.h"
}

using namespace std;

JNIEXPORT jstring JNICALL Java_Binding_sparseNauty(
    JNIEnv *env,
    jobject thisObject,
    jintArray arrayIndices,
    jintArray arrayDegrees,
    jintArray arrayNeighbours,
    jint mininvarlevel,
    jint maxinvarlevel,
    jint invararg)
{
	jsize order = env->GetArrayLength(arrayIndices);
	jsize directedEdgesNum = env->GetArrayLength(arrayNeighbours);
	jint *indices = env->GetIntArrayElements(arrayIndices, 0);
	jint *degrees = env->GetIntArrayElements(arrayDegrees, 0);
	jint *neighbours = env->GetIntArrayElements(arrayNeighbours, 0);

    DYNALLSTAT(int, lab, lab_sz);
    DYNALLSTAT(int, ptn, ptn_sz);
    DYNALLSTAT(int, orbits, orbits_sz);
    DYNALLSTAT(int, nmap, nmap_sz);

    static DEFAULTOPTIONS_SPARSEGRAPH(options);
	statsblk stats;
	/* Declare and initialize sparse graph structures */
    SG_DECL(sg);
	SG_DECL(cg);

    options.getcanon = TRUE; // option for canonical labelling
	options.mininvarlevel = mininvarlevel; // best 0, 1, 2
	options.maxinvarlevel = maxinvarlevel; // best 0, 1, 2
	options.invararg = invararg; // 0, 8 for sparse graphs

    nauty_check(WORDSIZE, SETWORDSNEEDED(order), order, NAUTYVERSIONID);

    DYNALLOC1(int, lab, lab_sz, order, (char *) "malloc");
    DYNALLOC1(int, ptn, ptn_sz, order, (char *) "malloc");
	DYNALLOC1(int, orbits, orbits_sz, order, (char *) "malloc");
	DYNALLOC1(int, nmap, nmap_sz, order, (char *) "malloc");
	SG_ALLOC(sg, order, directedEdgesNum, (char *) "malloc");

	sg.nv = order; // number of vertices
	sg.nde = directedEdgesNum; // number of directed edges
	sg.e = neighbours;
	sg.d = degrees;
	int i;
	for (i = 0; i < order; ++i) sg.v[i] = indices[i];

	sparsenauty(&sg, lab, ptn, orbits, &options, &stats, &cg);
	char* sparse6string = sgtos6(&sg);
	jstring result = env->NewStringUTF(sparse6string);
	free(sparse6string);
	return result;
}
