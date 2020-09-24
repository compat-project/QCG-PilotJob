/* for affinity functions */
#define _GNU_SOURCE
#include <sched.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <strings.h>

#include <stdio.h>
#include <mpi.h>

void debug_affinity(char*);

int main(int argc, char **argv) {
	int rank, size;
	char hostname[256];
	char affinity_buff[1024];

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	bzero(hostname, 256);
	bzero(affinity_buff, 1024);

	gethostname(hostname, 256);

	debug_affinity(affinity_buff);
	printf("%d/%d @ %s process ready, %s", rank, size, hostname, affinity_buff);

	MPI_Finalize();

	return 0;
}


void debug_affinity(char *buff) {
	cpu_set_t *cs;
	int count, size, i, first;
	char tmp_buff[24];

	cs = CPU_ALLOC(CPU_SETSIZE);
	assert(cs != NULL);

	size = CPU_ALLOC_SIZE(CPU_SETSIZE);
	CPU_ZERO_S(size, cs);

	sched_getaffinity(0, size, cs);

	count = CPU_COUNT(cs);
	first = 1;
	sprintf(buff, "cpu affinity (%d count): ", count);
	for (i = 0; i < CPU_SETSIZE; ++i) {
		if (CPU_ISSET(i, cs)) {
			if (!first)
				strcat(buff, ",");
			sprintf(tmp_buff, "%d", i);
			strcat(buff, tmp_buff);
			first = 0;
		}
	}
	strcat(buff, "\n");

	CPU_FREE(cs);
}
