/* for affinity functions */
#define _GNU_SOURCE
#include <sched.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

#include <omp.h>
#include <stdio.h>


void debug_affinity(void);


int main(int argc, char **argv) {
	printf("omp_num_procs: %d (available cpus)\n", omp_get_num_procs());
	printf("omp_max_threads: %d (allowed threads)\n", omp_get_max_threads());
	printf("omp_num_threads: %d (threads in current block)\n", omp_get_num_threads());
	printf("omp_thread_num: %d (id of main thread)\n", omp_get_thread_num());

	debug_affinity();

#pragma omp parallel
	printf("%d/%d thread ready\n", omp_get_thread_num(), omp_get_num_procs());

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
