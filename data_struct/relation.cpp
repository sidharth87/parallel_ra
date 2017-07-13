#include "relation.h"

static uint32_t inner_hash( uint32_t a);
static uint32_t outer_hash( uint32_t a);

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

    outer_hash_buffer_size = r.outer_hash_buffer_size;

    outer_hash_data = new int*[outer_hash_buffer_size / number_of_columns];
    for(int i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
        outer_hash_data[i] = new int[number_of_columns];

    for(int i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
        memcpy(outer_hash_data[i], r.outer_hash_data[i], number_of_columns * sizeof(int));
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


// TODO: this is hardcoded completely inverts
void relation::assign_inverted_data(int* data)
{
    int count = 0;
    for(int i = 0; i < local_number_of_rows; ++i)
    {
        for(int j = number_of_columns - 1; j >= 0; j--)
        {
            initial_data[i][j] = data[count++];
        }
    }
}


void relation::print_init_data(char* filename)
{
    FILE * fp = fopen(filename, "w");

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


void relation::print_outer_hash_data(char* filename)
{
    FILE * fp = fopen(filename, "w");

    for(int i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
    {
        for(int j = 0; j < number_of_columns; ++j)
        {
            if (j == number_of_columns - 1)
                fprintf(fp, "%d", outer_hash_data[i][j]);
            else
                fprintf(fp, "%d\t", outer_hash_data[i][j]);
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
        process_size[outer_hash(initial_data[i][hash_column_index])%nprocs] = process_size[outer_hash(initial_data[i][hash_column_index])%nprocs] + number_of_columns;
        for (int j = 0; j < number_of_columns; j++)
            process_data_vector[outer_hash(initial_data[i][hash_column_index])%nprocs].push_back(initial_data[i][j]);
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
    /* What is the buffer size to allocate */
    outer_hash_buffer_size = 0;
    for (int i = 0; i < nprocs; i++)
        outer_hash_buffer_size = outer_hash_buffer_size + recv_process_size_buffer[i];

    outer_hash_data = new int*[outer_hash_buffer_size / number_of_columns];
    for(int i = 0; i < (outer_hash_buffer_size / number_of_columns); ++i)
        outer_hash_data[i] = new int[number_of_columns];

#if 1
    int total_row_size = 0;
    MPI_Allreduce(&outer_hash_buffer_size, &total_row_size, 1, MPI_INT, MPI_SUM, comm);
    assert(total_row_size == global_number_of_rows * number_of_columns);
    printf("[%d] Buffer size after hashing %d [%d]\n", rank, outer_hash_buffer_size, total_row_size);
#endif

    int hash_buffer[outer_hash_buffer_size];
    MPI_Alltoallv(process_data, process_size, prefix_sum_process_size, MPI_INT, hash_buffer, recv_process_size_buffer, prefix_sum_recv_process_size_buffer, MPI_INT, comm);

    // TODO: this is hard coded for two columns
    for (int i = 0; i < outer_hash_buffer_size; i++)
        outer_hash_data[i / number_of_columns][i % number_of_columns] = hash_buffer[i];

    return;
}


void relation::hash_init_data_free()
{
    for(int i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
        delete[] outer_hash_data[i];

    delete[] outer_hash_data;
    return;
}


void relation::inner_hash_perform()
{
    int hash_column = 0;
    int inner_bucket_id;
    for(int i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
    {
        inner_bucket_id = inner_hash(outer_hash_data[i][hash_column]) % BUCKET_COUNT;
        for(int j = 0; j < number_of_columns; ++j)
        {
            inner_hash_data[inner_bucket_id].push_back(outer_hash_data[i][j]);
        }
    }

    return;
}


void relation::print_inner_hash_data(char* filename)
{
    FILE * fp = fopen(filename, "w");

    for(int i = 0; i < BUCKET_COUNT; ++i)
    {
        fprintf(fp, "[%d] Bucket size %d\n", i, (int)inner_hash_data[i].size());
        for(int j = 0; j < inner_hash_data[i].size(); ++j)
        {
            //
            if (j % number_of_columns != number_of_columns - 1)
                fprintf(fp, "%d\t", inner_hash_data[i][j]);
            else
                fprintf(fp, "%d\n", inner_hash_data[i][j]);
            //
        }
        //fprintf(fp, "\n");
    }
    fclose(fp);
}


void relation::join(relation* r, int lc)
{
    int lhs;
    std::vector<int> join_output;

    for (int i = 0; i < BUCKET_COUNT; i++)
    {
        for (int j = 0; j < this->inner_hash_data[i].size(); j = j + number_of_columns)
        {
            lhs = this->inner_hash_data[i][j];
            for (int k = 0; k < r->inner_hash_data[i].size(); k = k + number_of_columns)
            {
                if (lhs == r->inner_hash_data[i][k])
                {
                    join_output.push_back(this->inner_hash_data[i][j + 1]);
                    join_output.push_back(r->inner_hash_data[i][k+1]);
                }
            }
        }
    }

    if (rank == 0 && lc == 1)
    {
        for (int j = 0; j < join_output.size(); j = j + number_of_columns)
        {
            //printf("%d\t%d\n", join_output[j], join_output[j + 1]);
        }
    }


    // One options to send data to the relations, all at once and the re-bucketing at the reciever side

    {
    // Send Join output
    /* process_size[j] stores the number of samples to be sent to process with rank j */
    int process_size[nprocs];
    memset(process_size, 0, nprocs * sizeof(int));

    /* vector[i] contains the data that needs to be sent to process i */
    std::vector<int> process_data_vector[nprocs];

    for (int j = 0; j < join_output.size(); j = j + number_of_columns)
    {
        process_size[outer_hash(join_output[j])%nprocs] = process_size[outer_hash(join_output[j])%nprocs] + number_of_columns;
        for (int k = j; k < j + number_of_columns; k++)
            process_data_vector[outer_hash(join_output[j])%nprocs].push_back(join_output[k]);
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

    /* Sending data to all processes */
    /* What is the buffer size to allocate */
    int outer_hash_buffer_size = 0;
    for (int i = 0; i < nprocs; i++)
        outer_hash_buffer_size = outer_hash_buffer_size + recv_process_size_buffer[i];

    int hash_buffer[outer_hash_buffer_size];
    MPI_Alltoallv(process_data, process_size, prefix_sum_process_size, MPI_INT, hash_buffer, recv_process_size_buffer, prefix_sum_recv_process_size_buffer, MPI_INT, comm);

    //if (lc == 0)
    r->insert(hash_buffer, outer_hash_buffer_size);
    }


    {
        // Send Join output
        /* process_size[j] stores the number of samples to be sent to process with rank j */
        int process_size[nprocs];
        memset(process_size, 0, nprocs * sizeof(int));

        /* vector[i] contains the data that needs to be sent to process i */
        std::vector<int> process_data_vector[nprocs];

        for (int j = (number_of_columns - 1); j < join_output.size(); j = j + number_of_columns)
        {
            process_size[outer_hash(join_output[j])%nprocs] = process_size[outer_hash(join_output[j])%nprocs] + number_of_columns;
            for (int k = j; k >= j - (number_of_columns - 1); k--)
                process_data_vector[outer_hash(join_output[j])%nprocs].push_back(join_output[k]);
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

        /* Sending data to all processes */
        /* What is the buffer size to allocate */
        int outer_hash_buffer_size = 0;
        for (int i = 0; i < nprocs; i++)
            outer_hash_buffer_size = outer_hash_buffer_size + recv_process_size_buffer[i];

        int hash_buffer[outer_hash_buffer_size];
        MPI_Alltoallv(process_data, process_size, prefix_sum_process_size, MPI_INT, hash_buffer, recv_process_size_buffer, prefix_sum_recv_process_size_buffer, MPI_INT, comm);

        //if (lc == 0)
        this->insert(hash_buffer, outer_hash_buffer_size);
    }

    return;
}



void relation::insert(int *buffer, int buffer_size)
{
    for (int k = 0; k < buffer_size; k = k + number_of_columns)
    {
        int count = 0;
        int bucket_id = inner_hash(buffer[k]) % BUCKET_COUNT;
        //printf("bucket id %d %d\n", bucket_id, buffer[k]);

        for (int i = 0; i < this->inner_hash_data[bucket_id].size(); i = i + number_of_columns)
        {
            if (this->inner_hash_data[bucket_id][i] == buffer[k] && this->inner_hash_data[bucket_id][i + 1] == buffer[k + 1])
            {

            }
            else
                count++;
        }

        if (count == this->inner_hash_data[bucket_id].size() / number_of_columns)
        {
            this->inner_hash_data[bucket_id].push_back(buffer[k]);
            this->inner_hash_data[bucket_id].push_back(buffer[k + 1]);
            //if (rank == 0)
            //printf("val %d %d\n", buffer[k], buffer[k + 1]);
        }
    }

    return;
}

static uint32_t inner_hash( uint32_t a)
{
   a = (a+0x7ed55d16) + (a<<12);
   a = (a^0xc761c23c) ^ (a>>19);
   a = (a+0x165667b1) + (a<<5);
   a = (a+0xd3a2646c) ^ (a<<9);
   a = (a+0xfd7046c5) + (a<<3);
   a = (a^0xb55a4f09) ^ (a>>16);
   return a;
}


static uint32_t outer_hash( uint32_t a)
{
#if 0
   a = (a+0x7ed55d16) + (a<<12);
   a = (a^0xc761c23c) ^ (a>>19);
   a = (a+0x165667b1) + (a<<5);
   a = (a+0xd3a2646c) ^ (a<<9);
   a = (a+0xfd7046c5) + (a<<3);
   a = (a^0xb55a4f09) ^ (a>>16);
#endif
   return a;
}
