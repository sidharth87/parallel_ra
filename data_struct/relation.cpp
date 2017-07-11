#include "relation.h"


relation::relation()
{
    rank = 0;
    nprocs = 1;
}

relation::relation(const relation &r)
{
    rank = r.rank;
    nprocs = r.nprocs;
    comm = r.comm;
    number_of_columns = r.number_of_columns;
    global_number_of_rows = r.global_number_of_rows;
    local_number_of_rows = r.local_number_of_rows;

    hash_buffer_size = r.hash_buffer_size;

    hashed_data = new int*[hash_buffer_size / number_of_columns];
    for(int i = 0; i < hash_buffer_size / number_of_columns; ++i)
        hashed_data[i] = new int[number_of_columns];

    for(int i = 0; i < hash_buffer_size / number_of_columns; ++i)
        memcpy(hashed_data[i], r.hashed_data[i], number_of_columns * sizeof(int));
}

/*
relation::~relation()
{
    for(int i = 0; i < local_number_of_rows; ++i)
        delete[] initial_data[i];

    delete[] initial_data;
}
*/

void relation::set_rank(int r)
{
    rank = r;
    return;
}


void relation::set_nprocs(int n)
{
    nprocs = n;
    return;
}

void relation::set_comm(MPI_Comm c)
{
    comm = c;
    return;
}


void relation::create_init_data()
{
    initial_data = new int*[local_number_of_rows];
    for(int i = 0; i < local_number_of_rows; ++i)
        initial_data[i] = new int[number_of_columns];
}

void relation::assign_init_data(int* data)
{
    int count = 0;
    for(int i = 0; i < local_number_of_rows; ++i)
    {
        for(int j = 0; j < number_of_columns; ++j)
        {
            initial_data[i][j] = data[count++];
        }
    }
}

void relation::print_init_data()
{
    char local_file_name[1024];
    sprintf(local_file_name, "init_data_%d.txt", rank);
    FILE * fp = fopen(local_file_name, "w");

    for(int i = 0; i < local_number_of_rows; ++i)
    {
        for(int j = 0; j < number_of_columns; ++j)
        {
            if (j == number_of_columns - 1)
                fprintf(fp, "%d", initial_data[i][j]);
            else
                fprintf(fp, "%d\t", initial_data[i][j]);
        }
        fprintf(fp, "\n");
    }

    fclose(fp);
}


void relation::print_hashed_data(char* filename)
{
    FILE * fp = fopen(filename, "w");

    for(int i = 0; i < hash_buffer_size / number_of_columns; ++i)
    {
        for(int j = 0; j < number_of_columns; ++j)
        {
            if (j == number_of_columns - 1)
                fprintf(fp, "%d", hashed_data[i][j]);
            else
                fprintf(fp, "%d\t", hashed_data[i][j]);
        }
        fprintf(fp, "\n");
    }

    fclose(fp);
}


void relation::free_init_data()
{
    for(int i = 0; i < local_number_of_rows; ++i)
        delete[] initial_data[i];

    delete[] initial_data;
}

int relation::get_number_of_columns()
{
    return number_of_columns;
}

void relation::set_number_of_columns(int n)
{
    number_of_columns = n;
    return;
}

int relation::get_number_of_global_rows()
{
    return global_number_of_rows;
}

void relation::set_number_of_global_rows(int n)
{
    global_number_of_rows = n;
    return;
}

int relation::get_number_of_local_rows()
{
    return local_number_of_rows;
}

void relation::set_number_of_local_rows(int n)
{
    local_number_of_rows = n;
    return;
}


void relation::hash_init_data()
{
    /* process_size[j] stores the number of samples to be sent to process with rank j */
    int process_size[nprocs];
    memset(process_size, 0, nprocs * sizeof(int));

    /* vector[i] contains the data that needs to be sent to process i */
    std::vector<int> process_data_vector[nprocs];
    int hash_column_index = 0;
    for (int i = 0; i < local_number_of_rows; ++i)
    {
        process_size[initial_data[i][hash_column_index]%nprocs] = process_size[initial_data[i][hash_column_index]%nprocs] + number_of_columns;
        for (int j = 0; j < number_of_columns; j++)
            process_data_vector[initial_data[i][hash_column_index]%nprocs].push_back(initial_data[i][j]);
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

#if 1
    if (rank == 7)
    {
    printf("Rank %d: ", rank);
    for(int i = 0; i < nprocs; ++i)
        printf(" %d [%d] ", process_size[i], prefix_sum_process_size[i]);
    printf("\n");
    }
#endif

    /* Sending data to all processes */

    hash_buffer_size = 0;
    /* What is the buffer size to allocate */
    for (int i = 0; i < nprocs; i++)
        hash_buffer_size = hash_buffer_size + recv_process_size_buffer[i];




    hashed_data = new int*[hash_buffer_size / number_of_columns];
    for(int i = 0; i < (hash_buffer_size / number_of_columns); ++i)
        hashed_data[i] = new int[number_of_columns];


#if 1
    int total_row_size = 0;
    MPI_Allreduce(&hash_buffer_size, &total_row_size, 1, MPI_INT, MPI_SUM, comm);
    assert(total_row_size == global_number_of_rows * number_of_columns);
    printf("[%d] Buffer size after hashing %d [%d]\n", rank, hash_buffer_size, total_row_size);
#endif

    int hash_buffer[hash_buffer_size];
    MPI_Alltoallv(process_data, process_size, prefix_sum_process_size, MPI_INT, hash_buffer, recv_process_size_buffer, prefix_sum_recv_process_size_buffer, MPI_INT, comm);


    // TODO: this is hard coded for two columns
    for (int i = 0; i < hash_buffer_size; i++)
        hashed_data[i / number_of_columns][i % number_of_columns] = hash_buffer[i];


#if 0
    char local_file_name[1024];
    sprintf(local_file_name, "hashed_data_%d.txt", rank);
    FILE * fp = fopen(local_file_name, "w");

    for (int i = 0; i < hash_buffer_size; i++)
    {
        if (i % 2 == 0)
            fprintf(fp, "%d\t", hash_buffer[i]);
        else
            fprintf(fp, "%d\n", hash_buffer[i]);
    }

    fclose(fp);
#endif

    return;
}


void relation::hash_init_data_free()
{
    for(int i = 0; i < hash_buffer_size / number_of_columns; ++i)
        delete[] hashed_data[i];

    delete[] hashed_data;
    return;
}


void relation::reorder_columns()
{
    int temp;
    // TODO: hardcoded for two columns
    for(int i = 0; i < hash_buffer_size / number_of_columns; ++i)
    {
        temp = hashed_data[i][0];
        hashed_data[i][0] = hashed_data[i][1];
        hashed_data[i][1] = temp;
    }
}
