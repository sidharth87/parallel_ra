#ifndef RELATION_H
#define RELATION_H

#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <limits.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdint.h>
#include <ctype.h>
#include <vector>

#include <mpi.h>

class relation{

private:

    int rank;
    int nprocs;
    MPI_Comm comm;

    int number_of_columns;
    int global_number_of_rows;
    int local_number_of_rows;

    int **initial_data;
    int **hashed_data;



public:

    relation();
    relation(const relation &r);
    //~relation();

    void set_rank(int rank);
    void set_nprocs(int nprocs);
    void set_comm(MPI_Comm cmomm);

    int get_number_of_columns();
    void set_number_of_columns(int col_count);

    int get_number_of_global_rows();
    void set_number_of_global_rows(int row_count);

    int get_number_of_local_rows();
    void set_number_of_local_rows(int row_count);

    void create_init_data();
    void assign_init_data(int* data);
    void print_init_data();
    void free_init_data();

    void hash_init_data();

};


#endif
