/* for affinity functions */
#define _GNU_SOURCE
#include <sched.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

#include <stdio.h>

void debug_affinity(void);


int main(int argc, char **argv) {
	debug_affinity();
	return 0;
}


void debug_affinity(void) {
	cpu_set_t *cs;
	int count, size, i, first;

	cs = CPU_ALLOC(CPU_SETSIZE);
	assert(cs != NULL);

	size = CPU_ALLOC_SIZE(CPU_SETSIZE);
	CPU_ZERO_S(size, cs);

	sched_getaffinity(0, size, cs);

	count = CPU_COUNT(cs);
	first = 1;
	printf("cpu affinity (%d count): ", count);
	for (i = 0; i < CPU_SETSIZE; ++i) {
		if (CPU_ISSET(i, cs)) {
			if (!first)
				printf(",");
			printf("%d", i);
			first = 0;
		}
	}
	printf("\n");

	CPU_FREE(cs);
}
