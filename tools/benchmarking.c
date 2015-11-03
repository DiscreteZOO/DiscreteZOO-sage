#include "nausparse.h"    /* includes nauty.h */
#include "gtools.h"

int main(int argc, char *argv[])
{
  DYNALLSTAT(int,lab,lab_sz);
  DYNALLSTAT(int,ptn,ptn_sz);
  DYNALLSTAT(int,orbits,orbits_sz);
  DYNALLSTAT(int,map,map_sz);
  static DEFAULTOPTIONS_SPARSEGRAPH(options);
  statsblk stats;
	/* Declare and initialize sparse graph structures */
  SG_DECL(sg);
  SG_DECL(cg);
  int n,m,i;
  options.getcanon = TRUE; // option for canonical labelling

	n = 16;
	m = SETWORDSNEEDED(n);
  nauty_check(WORDSIZE,m,n,NAUTYVERSIONID);

  DYNALLOC1(int,lab,lab_sz,n,"malloc");
  DYNALLOC1(int,ptn,ptn_sz,n,"malloc");
	DYNALLOC1(int,orbits,orbits_sz,n,"malloc");
	DYNALLOC1(int,map,map_sz,n,"malloc");
	SG_ALLOC(sg,n,3*n,"malloc");
	sg.nv = n; // number of vertices
	sg.nde = 3*n; // number of directed edges
	int indices[n];
	for (i = 0; i < n; ++i) {
		sg.v[i] = 3*i; // position of vertex i in the array of neighbours
    sg.d[i] = 3; // degree of vertex i
		indices[i] = 3*i;
	}
	int edges[][2] = {{3,4}, {11,5}, {11,14}, {1,16}, {13,16}, {15,16}, {1,10}, {12,6}, {14,15}, {12,8}, {2,10}, {3,5}, {2,4}, {7,9}, {3,14}, {15,6}, {13,5}, {4,7}, {1,8}, {12,10}, {2,9}, {11,9}, {7,8}, {13,6}};
	size_t edges_num = sizeof edges / sizeof *edges;
	for (i = 0; i < edges_num; i++) {
		int e = edges[i][0]-1;
		int f = edges[i][1]-1;
		sg.e[indices[e]] = f;
		indices[e]++;
		sg.e[indices[f]] = e;
		indices[f]++;
	}
	sparsenauty(&sg, lab, ptn, orbits, &options, &stats, &cg);
	printf("%s\n", sgtos6(&sg)); // sgtos6
	exit(0);
}
