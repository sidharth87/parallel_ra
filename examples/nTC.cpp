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



typedef uint64_t u64;
typedef uint32_t u32;

static void read_input_relation_from_file_to_local_buffer(char **argv);
static void local_buffer_with_input_relation();

static int rank = 0;
static int nprocs = 1;
static u32 local_row_count;
static u32 global_row_count;
static int *read_buffer;

int main(int argc, char **argv)
{
    // Initializing MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Read from input file
    read_input_relation_from_file_to_local_buffer(argv);

    // Create input relation
    relation G(rank, nprocs, MPI_COMM_WORLD, global_row_count, local_row_count);
    G.create_init_data();
    G.create_hash_buckets();
    G.assign_init_data(read_buffer);

    // Hash input relation
    G.hash_init_data(0);

    // Free the intermediate buffers
    G.hash_init_data_free();
    G.free_init_data();

    // For debug purpose
    //char G_hash_file_name[1024];
    //sprintf(G_hash_file_name, "G_hased_data_%d.txt", rank);
    //G.print_inner_hash_data(G_hash_file_name);



    // Create output (TC) relation
    relation T(rank, nprocs, MPI_COMM_WORLD, global_row_count, local_row_count);
    T.create_init_data();
    T.create_hash_buckets();
    T.assign_init_data(read_buffer);

    // Hash input relation
    T.hash_init_data(1);

    // Free the intermediate buffers
    T.hash_init_data_free();
    T.free_init_data();

    // For debug purpose
    //char T_hash_file_name[1024];
    //sprintf(T_hash_file_name, "T_hased_data_%d.txt", rank);
    //T.print_inner_hash_data(T_hash_file_name);


    // Create output (TC) relation
    relation dT(rank, nprocs, MPI_COMM_WORLD, global_row_count, local_row_count);
    dT.create_init_data();
    dT.create_hash_buckets();
    dT.assign_init_data(read_buffer);

    // Hash input relation
    dT.hash_init_data(1);

    // Free the intermediate buffers
    dT.hash_init_data_free();
    dT.free_init_data();

    // For debug purpose
    //char dT_hash_file_name[1024];
    //sprintf(dT_hash_file_name, "dT_hased_data_%d.txt", rank);
    //dT.print_inner_hash_data(dT_hash_file_name);

    MPI_Barrier(MPI_COMM_WORLD);
    double start_time = MPI_Wtime();
    int loop_count = 0;
    int ret = 0;
    do {
        ret = T.join(&G, &dT, loop_count);

        //char loop_join[1024];
        //sprintf(loop_join, "loop_join_%d_%d.txt", loop_count, rank);
        //T.print_inner_hash_data(loop_join);

        loop_count++;
    }
    while (ret != 1);

    MPI_Barrier(MPI_COMM_WORLD);
    double end_time = MPI_Wtime();

    if (rank == 0)
        printf("Total time to complete %f\n", end_time - start_time);




    // Finalizing MPI
    MPI_Finalize();
    return 0;
}



static void read_input_relation_from_file_to_local_buffer(char **argv)
{
    if (rank == 0)
    {
        char meta_data_filename[1024];
        sprintf(meta_data_filename, "%s/meta_data.txt", argv[1]);
        printf("Opening File %s\n", meta_data_filename);

        FILE *fp_in;
        fp_in = fopen(meta_data_filename, "r");
        if (fscanf (fp_in, "(row count)\n%d\n(col count)\n2", &global_row_count) != 1)
        {
            printf("Wrong input format (Meta Data)\n");
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        fclose(fp_in);
        printf("Total number of rows = %d\n", global_row_count);
    }

    MPI_Bcast(&global_row_count, 1, MPI_INT, 0, MPI_COMM_WORLD);

    u32 read_offset;


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

    read_buffer = new int[local_row_count * COL_COUNT];
    if (pread(fp, read_buffer, local_row_count * COL_COUNT * sizeof(int), read_offset * COL_COUNT * sizeof(int)) != local_row_count * COL_COUNT * sizeof(int))
    {
        printf("Wrong input format (DATA)\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    close(fp);

    if (rank == 0)
        printf("Rank %d reads %d elements from %d offset from %s\n", rank, local_row_count, read_offset, data_filename);

    return;
}

static void local_buffer_with_input_relation()
{
    delete read_buffer;
    return;
}
