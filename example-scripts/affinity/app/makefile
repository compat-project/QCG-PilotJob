.PHONY: all clean


all: aff omp_app mpi_app

clean:
	rm -f aff omp_app mpi_app

aff: aff.c
	gcc -Wall -O0 -g -o aff aff.c

omp_app: omp_app.c
	gcc -Wall -O0 -g -fopenmp -o omp_app omp_app.c

mpi_app: mpi_app.c
	mpicc -Wall -O0 -g -o mpi_app mpi_app.c
