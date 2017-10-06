#include <sys/stat.h>
#include <errno.h>
#include <limits.h>
#include <arpa/inet.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdint.h>
#include <ctype.h>
#include <vector>
#include <math.h>

static int COL_COUNT = 2;
static int rank = 0;
static int nprocs = 1;
static int local_row_count;
static int global_row_count;
static int *read_buffer;
static MPI_Comm comm = MPI_COMM_WORLD;

static uint64_t tunedhash(const uint8_t* bp, const uint32_t len);
static uint64_t all_column_hash(uint64_t a, uint64_t b);
static uint64_t outer_hash(const uint64_t val);
static uint64_t inner_hash(uint64_t val);

static void read_input_relation_from_file_to_local_buffer(char **argv) {
    
    if (rank == 0) {
        char meta_data_filename[1024];
        sprintf(meta_data_filename, "%s/meta_data.txt", argv[1]);
        //printf("Opening File %s\n", meta_data_filename);

        FILE *fp_in;
        fp_in = fopen(meta_data_filename, "r");
        if (fscanf(fp_in, "(row count)\n%d\n(col count)\n2", &global_row_count) != 1) {
            printf("Wrong input format (Meta Data)\n");
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        fclose(fp_in);
        //printf("Total number of rows = %d\n", global_row_count);
    }

    MPI_Bcast(&global_row_count, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int read_offset;


    read_offset = ceil((float) global_row_count / nprocs) * rank;
    if (read_offset + ceil((float) global_row_count / nprocs) > global_row_count)
        local_row_count = global_row_count - read_offset;
    else
        local_row_count = (int) ceil((float) global_row_count / nprocs);

    if (local_row_count < 0)
        local_row_count = 0;

    char data_filename[1024];
    sprintf(data_filename, "%s/data.raw", argv[1]);
    int fp = open(data_filename, O_RDONLY);

    read_buffer = new int[local_row_count * COL_COUNT];
    if (pread(fp, read_buffer, local_row_count * COL_COUNT * sizeof (int), read_offset * COL_COUNT * sizeof (int)) != local_row_count * COL_COUNT * sizeof (int)) {
        printf("Wrong input format (DATA)\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    close(fp);

    //if (rank == 0)
    printf("Rank %d reads %d elements from %d offset from %s\n", rank, local_row_count, read_offset, data_filename);

    return;
}

void hash_init_data(int hash_column_index, char **argv)
{
    /* process_size[j] stores the number of samples to be sent to process with rank j */
    int process_size[nprocs];
    memset(process_size, 0, nprocs * sizeof(int));

    /* vector[i] contains the data that needs to be sent to process i */
    std::vector<int> process_data_vector[nprocs];
    for (uint i = 0; i < local_row_count; ++i)
    {
        uint64_t index = outer_hash(read_buffer[i * COL_COUNT + hash_column_index])%nprocs;
        //uint64_t index = initial_data[i][hash_column_index]%nprocs;
        process_size[index] = process_size[index] + COL_COUNT;

        for (uint j = 0; j < COL_COUNT; j++)
            process_data_vector[index].push_back(read_buffer[i * COL_COUNT + j]);
    }

    int prefix_sum_process_size[nprocs];
    memset(prefix_sum_process_size, 0, nprocs * sizeof(int));
    for (int i = 1; i < nprocs; i++)
        prefix_sum_process_size[i] = prefix_sum_process_size[i - 1] + process_size[i - 1];

    int process_data_buffer_size = prefix_sum_process_size[nprocs - 1] + process_size[nprocs - 1];
    int process_data[process_data_buffer_size];

    for (int i = 0; i < nprocs; i++)
        memcpy(process_data + prefix_sum_process_size[i], &process_data_vector[i][0], process_data_vector[i].size() * sizeof(int));

    /* This step prepares for actual data transfer */
    /* Every process sends to every other process the amount of data it is going to send */
    int recv_process_size_buffer[nprocs];
    memset(recv_process_size_buffer, 0, nprocs * sizeof(int));
    MPI_Alltoall(process_size, 1, MPI_INT, recv_process_size_buffer, 1, MPI_INT, comm);

    int prefix_sum_recv_process_size_buffer[nprocs];
    memset(prefix_sum_recv_process_size_buffer, 0, nprocs * sizeof(int));
    for (int i = 1; i < nprocs; i++)
        prefix_sum_recv_process_size_buffer[i] = prefix_sum_recv_process_size_buffer[i - 1] + recv_process_size_buffer[i - 1];

#if 0
    if (rank == 7)
    {
        printf("Rank %d: ", rank);
        for(int i = 0; i < nprocs; ++i)
            printf(" %lld [%lld] ", (unsigned long long)process_size[i], (unsigned long long)prefix_sum_process_size[i]);
        printf("\n");
    }
#endif

    /* Sending data to all processes */
    /* What is the buffer size to allocate */
    uint32_t outer_hash_buffer_size = 0;
    for(int i = 0; i < nprocs; i++)
        outer_hash_buffer_size = outer_hash_buffer_size + recv_process_size_buffer[i];

    int** outer_hash_data = new int*[outer_hash_buffer_size / COL_COUNT];
    for(uint i = 0; i < (outer_hash_buffer_size / COL_COUNT); ++i)
        outer_hash_data[i] = new int[COL_COUNT];

#if 1
    uint total_row_size = 0;
    MPI_Allreduce(&outer_hash_buffer_size, &total_row_size, 1, MPI_INT, MPI_SUM, comm);
    if(total_row_size != global_row_count * COL_COUNT)
    {
        printf("Incorrect distribution\n");
        MPI_Abort(comm, -1);
    }
    //printf("[%d] Buffer size after hashing %lld [%lld]\n", rank, (unsigned long long)outer_hash_buffer_size, (unsigned long long)total_row_size);
#endif

    int hash_buffer[outer_hash_buffer_size];
    MPI_Alltoallv(process_data, process_size, prefix_sum_process_size, MPI_INT, hash_buffer, recv_process_size_buffer, prefix_sum_recv_process_size_buffer, MPI_INT, comm);

    char filename[1024];
    sprintf(filename, "%s_%d.txt", argv[1], rank);
    FILE * fp = fopen(filename, "w");
    for(uint i = 0; i < outer_hash_buffer_size; i = i + 2)
        fprintf(fp, "%d\t%d\n", hash_buffer[i], hash_buffer[i + 1]);
    
    fclose(fp);

    return;
}


inline static uint64_t tunedhash(const uint8_t* bp, const uint32_t len)
{ 
    uint64_t h0 = 0xb97a19cb491c291d;
    uint64_t h1 = 0xc18292e6c9371a17;
    const uint8_t* const ep = bp+len;
    while (bp < ep)
    {
        h1 ^= *bp;
        h1 *= 31;
        h0 ^= (((uint64_t)*bp) << 17) ^ *bp;
        h0 *= 0x100000001b3;
        h0 = (h0 >> 7) | (h0 << 57);
        ++bp;
    }
    
    return h0 ^ h1;// ^ (h1 << 31);
}

static uint64_t all_column_hash(uint64_t a, uint64_t b)
{
    const uint64_t both[2] = {a,b};
    return tunedhash((uint8_t*)(&both),2*sizeof(uint64_t));
}


static uint64_t outer_hash(const uint64_t val)
{
    return tunedhash((uint8_t*)(&val),sizeof(uint64_t));
}

static uint64_t inner_hash(uint64_t val)
{
    val = (val << 23) | (val >> 41);
    return tunedhash((uint8_t*)(&val),sizeof(uint64_t));
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    read_input_relation_from_file_to_local_buffer(argv);
    hash_init_data(0, argv);
    
    
    MPI_Finalize();
}