/*
Input G(0,1), output T(0,1)

Sequence 0:
  MOVE T <- G                 // Copies G to T
  GOTO 1                      // Jumps to sequence 1

Sequence 1:
  MOVE T' <- T                // Move the old T into T', so we can compare at the end
  REORDER G' <- G {1,0}       // Swap the columns in G and call that G'
  JOIN J <- G', T (keys=1)    // Join G' and T where only the 0th column is a key and call the result J and trim the second column
  UNION T <- T, J''           // union these new transitive paths with the old ones into T
  COND T, T' {yes=2,no=1}     // Check if T has changed, if they're equal goto 2, if they are not (it changed) goto 1

Sequence 2:
  HALT
*/



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
#include <mpi.h>

#include "../data_struct/tuple.h"
#include "../data_struct/relation.h"
#include "../ra/ra_operations.h"

static int rank = 0;
static int nprocs = 1;

static int rule = 1;

int main(int argc, char **argv)
{
    // Initializing MPI
    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int bucket_count = 128;
    int inner_bucket_count = 128;
    if (argc == 4)
    {
        bucket_count = atoi(argv[2]);
        inner_bucket_count = atoi(argv[3]);
    }

    if (argc == 5)
    {
        bucket_count = atoi(argv[2]);
        inner_bucket_count = atoi(argv[3]);
        rule = atoi(argv[4]);
    }

    int global_row_count;
    int col_count = 2;

    if (rank == 0)
    {
        char meta_data_filename[1024];
        sprintf(meta_data_filename, "%s/meta_data.txt", argv[1]);
        printf("Opening File %s\n", meta_data_filename);

        FILE *fp_in;
        fp_in = fopen(meta_data_filename, "r");
        fscanf (fp_in, "(row count)\n%d\n(col count)\n2", &global_row_count);
        fclose(fp_in);
        printf("Total number of rows = %d\n", global_row_count);
    }

    MPI_Bcast(&global_row_count, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int read_offset;
    int local_row_count;

    read_offset = ceil((float)global_row_count / nprocs) * rank;
    if (read_offset + ceil((float)global_row_count / nprocs) > global_row_count)
      local_row_count = global_row_count - read_offset;
    else
      local_row_count = (int) ceil((float)global_row_count / nprocs);

    if (local_row_count < 0)
        local_row_count = 0;

    char data_filename[1024];
    sprintf(data_filename, "%s/data.raw", argv[1]);
    int fp = open(data_filename, O_RDONLY);
    int read_buffer[local_row_count * col_count];
    pread(fp, read_buffer, local_row_count * col_count * sizeof(int), read_offset * col_count * sizeof(int));
    close(fp);

    //if (rank == 199)
    //printf("Rank %d reads %d elements from %d offset from %s\n", rank, local_row_count, read_offset, data_filename);


    relation input;
    input.set_rank(rank);
    input.set_nprocs(nprocs);
    input.set_comm(MPI_COMM_WORLD);
    //input.set_number_of_columns(col_count);
    input.set_number_of_global_rows(global_row_count);
    input.set_number_of_local_rows(local_row_count);
    input.create_init_data();
    //input.create_hash_buckets(bucket_count, inner_bucket_count);
    input.create_hash_buckets();

#if 1
    input.assign_init_data(read_buffer);
#if 0
    char init_file_name[1024];
    sprintf(init_file_name, "init_data_%d.txt", rank);
    input.print_init_data(init_file_name);
#endif

    input.hash_init_data();

#if 0
    char hash_file[1024];
    sprintf(hash_file, "hash_data_%d.txt", rank);
    input.print_outer_hash_data(hash_file);
#endif

#if 0
    char inner_hash_file[1024];
    sprintf(inner_hash_file, "inner_hash_data_%d.txt", rank);
    input.print_inner_hash_data(inner_hash_file);
#endif




    relation reordered_input;
    reordered_input.set_rank(rank);
    reordered_input.set_nprocs(nprocs);
    reordered_input.set_comm(MPI_COMM_WORLD);
    //reordered_input.set_number_of_columns(col_count);
    reordered_input.set_number_of_global_rows(global_row_count);
    reordered_input.set_number_of_local_rows(local_row_count);
    reordered_input.create_init_data();
    //reordered_input.create_hash_buckets(bucket_count, inner_bucket_count);
    reordered_input.create_hash_buckets();


    reordered_input.assign_inverted_data(read_buffer);
#if 0
    char reordered_init_file_name[1024];
    sprintf(reordered_init_file_name, "reordered_init_data_%d.txt", rank);
    reordered_input.print_init_data(reordered_init_file_name);
#endif

    reordered_input.hash_init_data();
#if 0
    char reordered_hash_file[1024];
    sprintf(reordered_hash_file, "reordered_hash_data_%d.txt", rank);
    reordered_input.print_outer_hash_data(reordered_hash_file);
#endif

#if 0
    char reordered_inner_hash_file[1024];
    sprintf(reordered_inner_hash_file, "reordered_inner_hash_data_%d.txt", rank);
    reordered_input.print_inner_hash_data(reordered_inner_hash_file);
#endif




    //char inner_hash_file_after_join[1024];
    //sprintf(inner_hash_file_after_join, "inner_hash_data_before_join_%d.txt", rank);
    //input.print_inner_hash_data(inner_hash_file_after_join);

#if 0
    char reorderd_inner_hash_file_after_join[1024];
    sprintf(reorderd_inner_hash_file_after_join, "reorderd_inner_hash_data_before_join_%d.txt", rank);
    reordered_input.print_inner_hash_data(reorderd_inner_hash_file_after_join);
#endif

    reordered_input.hash_init_data_free();
    reordered_input.free_init_data();

    input.hash_init_data_free();
    input.free_init_data();

    int loop_count = 0;
    int ret = 0;
    do {
        reordered_input.set_rule(rule);
        ret = reordered_input.join(&input, loop_count);

        //char loop_join[1024];
        //sprintf(loop_join, "loop_join_%d_%d.txt", loop_count, rank);
        //input.print_inner_hash_data(loop_join);

        loop_count++;
    }
    while (ret != 1);



    //char inner_hash_file_after_join2[1024];
    //sprintf(inner_hash_file_after_join2, "inner_hash_data_after_join_X_%d.txt", rank);
    //input.print_inner_hash_data(inner_hash_file_after_join2);

#if 0
    char reorderd_inner_hash_file_after_join2[1024];
    sprintf(reorderd_inner_hash_file_after_join2, "reorderd_inner_hash_data_after_join_%d.txt", rank);
    reordered_input.print_inner_hash_data(reorderd_inner_hash_file_after_join2);
#endif

    reordered_input.free_hash_buckets();



    input.free_hash_buckets();

#endif
    MPI_Finalize();
    return 0;
}
