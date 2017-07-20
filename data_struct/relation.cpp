#include "relation.h"
#include <unordered_set>

typedef uint64_t u64;

static u64 inner_hash( u64 a);
static u64 outer_hash( u64 a);
static u64 all_column_hash(u64 a, u64 b);


//#define SETHASH
//#define HASH
//#undef HASH
//#undef SETHASH

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


void relation::create_hash_buckets(int bc, int ibc)
{
    bucket_count = bc;
    inner_bucket_count = ibc;

    inner_hash_data = new std::vector<int>*[bucket_count];
    for (int i = 0; i < bucket_count; i++)
        inner_hash_data[i] = new std::vector<int>[inner_bucket_count];
}


void relation::free_hash_buckets()
{
    for(int i = 0; i < bucket_count; ++i)
        delete[] inner_hash_data[i];

    delete[] inner_hash_data;
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
                fprintf(fp, "%lld", (unsigned long long)initial_data[i][j]);
            else
                fprintf(fp, "%lld\t", (unsigned long long)initial_data[i][j]);
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
                fprintf(fp, "%lld", (unsigned long long)outer_hash_data[i][j]);
            else
                fprintf(fp, "%lld\t", (unsigned long long)outer_hash_data[i][j]);
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
        uint64_t index = initial_data[i][hash_column_index];
        process_size[outer_hash(index)%nprocs] = process_size[outer_hash(index)%nprocs] + number_of_columns;
        for (int j = 0; j < number_of_columns; j++)
            process_data_vector[outer_hash(index)%nprocs].push_back(initial_data[i][j]);
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
    outer_hash_buffer_size = 0;
    for(int i = 0; i < nprocs; i++)
        outer_hash_buffer_size = outer_hash_buffer_size + recv_process_size_buffer[i];

    outer_hash_data = new int*[outer_hash_buffer_size / number_of_columns];
    for(int i = 0; i < (outer_hash_buffer_size / number_of_columns); ++i)
        outer_hash_data[i] = new int[number_of_columns];

#if 1
    int total_row_size = 0;
    MPI_Allreduce(&outer_hash_buffer_size, &total_row_size, 1, MPI_INT, MPI_SUM, comm);
    if(total_row_size != global_number_of_rows * number_of_columns)
    {
        printf("Incorrect distribution\n");
        MPI_Abort(comm, -1);
    }
    printf("[%d] Buffer size after hashing %lld [%lld]\n", rank, (unsigned long long)outer_hash_buffer_size, (unsigned long long)total_row_size);
#endif

    int hash_buffer[outer_hash_buffer_size];
    MPI_Alltoallv(process_data, process_size, prefix_sum_process_size, MPI_INT, hash_buffer, recv_process_size_buffer, prefix_sum_recv_process_size_buffer, MPI_INT, comm);

    // TODO: this is hard coded for two columns
    for(int i = 0; i < outer_hash_buffer_size; i++)
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
    int bucket_id;
    int inner_bucket_id = 0;
    for(int i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
    {
        bucket_id = inner_hash((uint64_t)outer_hash_data[i][hash_column]) % bucket_count;
        //inner_bucket_id = outer_hash((uint64_t)outer_hash_data[i][hash_column] ^ (uint64_t)outer_hash_data[i][hash_column + 1]) % inner_bucket_count;
        inner_bucket_id = all_column_hash((uint64_t)outer_hash_data[i][hash_column], (uint64_t)outer_hash_data[i][hash_column + 1]) % inner_bucket_count;

        for(int j = 0; j < number_of_columns; ++j)
        {
            inner_hash_data[bucket_id][inner_bucket_id].push_back(outer_hash_data[i][j]);
        }
    }

    return;
}


void relation::print_inner_hash_data(char* filename)
{
    FILE * fp = fopen(filename, "w");

    for(int i = 0; i < bucket_count; ++i)
    {
        //fprintf(fp, "[%lld] Bucket size %d\n", (unsigned long long)i, (int)inner_hash_data[i].size());
        for(int k = 0; k < inner_bucket_count; ++k)
        {
            for(int j = 0; j < inner_hash_data[i][k].size(); ++j)
            {
                if (j % number_of_columns != number_of_columns - 1)
                    fprintf(fp, "%lld\t", (unsigned long long)inner_hash_data[i][k][j]);
                else
                    fprintf(fp, "%lld\n", (unsigned long long)inner_hash_data[i][k][j]);
            }
        }
    }
    fclose(fp);

    return;
}

// 1 - 2   2 - 1
// 2 - 3   3 - 2

int relation::join(relation* r, int lc)
{
    int lhs;

    int join_output_bucket_size = 100000;//256;
    std::vector<int> *join_output;
    join_output = new std::vector<int>[join_output_bucket_size];

    double total1, total2;
    double j1, j2;
    double t1, t2, t3, t4;
    double b1, b2, b3, b4;
    double c1, c2, c3, c4;
    double m1, m2, m3, m4;
    double cond1, cond2;

    total1 = MPI_Wtime();
    j1 = MPI_Wtime();
    u64 hash = 0;
    u64 index = 0;
    int count = 1;
    int before1 = 0, after1 = 0, before2 = 0, after2 = 0;


    hashtable<two_tuple, bool> join_hash(512*1024);
    std::unordered_set<two_tuple> output_set;

    uint64_t lv, rv;
    int data_structure = VECTOR_HASH;
    //int data_structure = STD_UNORDERED_MAP;
    switch (data_structure)
    {
        case VECTOR_HASH:

        for(int b = 0; b < bucket_count; b++)
            for(int a = 0; a < inner_bucket_count; a++)
                before1 = before1 + r->inner_hash_data[b][a].size();

        for(int i1 = 0; i1 < bucket_count; i1++)
        {
            for(int i2 = 0; i2 < inner_bucket_count; i2++)
            {
                for(int j = 0; j < r->inner_hash_data[i1][i2].size(); j = j + number_of_columns)
                {
                    lhs = r->inner_hash_data[i1][i2][j];

                    for(int k1 = 0; k1 < inner_bucket_count; k1++)
                    {
                        for(int k2 = 0; k2 < this->inner_hash_data[i1][k1].size(); k2 = k2 + number_of_columns)
                        {
                            if (lhs == this->inner_hash_data[i1][k1][k2])
                            {
                                count = 1;
                                index = all_column_hash((uint64_t)this->inner_hash_data[i1][k1][k2 + 1], (uint64_t)r->inner_hash_data[i1][i2][j+1]) % join_output_bucket_size;
                                for (int m = 0; m < join_output[index].size(); m = m + number_of_columns)
                                {

                                    if (this->inner_hash_data[i1][k1][k2 + 1] == join_output[index][m] && r->inner_hash_data[i1][i2][j+1] == join_output[index][m + 1])
                                    {
                                        count = 0;
                                        break;
                                    }
                                }
                                if (count == 1)
                                {
                                    lv = this->inner_hash_data[i1][k1][k2 + 1];
                                    rv = r->inner_hash_data[i1][i2][j+1];
                                    join_output[index].push_back(lv);
                                    join_output[index].push_back(rv);


                                    int countx = 1;
                                    int bucket_id = inner_hash((uint64_t)lv) % bucket_count;
                                    int inner_bucket_id = all_column_hash((uint64_t)lv, (uint64_t)rv) % inner_bucket_count;

                                    for(int ix1 = 0; ix1 < r->inner_hash_data[bucket_id][inner_bucket_id].size(); ix1 = ix1 + number_of_columns)
                                    {
                                        if (r->inner_hash_data[bucket_id][inner_bucket_id][ix1] == lv && r->inner_hash_data[bucket_id][inner_bucket_id][ix1 + 1] == rv)
                                        {
                                            countx = 0;
                                            break;
                                        }
                                    }

                                    if (countx == 1)
                                    {
                                        r->inner_hash_data[bucket_id][inner_bucket_id].push_back(lv);
                                        r->inner_hash_data[bucket_id][inner_bucket_id].push_back(rv);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        delete[] join_output;

        after1 = 0;
        for(int b = 0; b < bucket_count; b++)
            for(int a = 0; a < inner_bucket_count; a++)
                after1 = after1 + r->inner_hash_data[b][a].size();

        if (before1 == after1)
        {
            printf("F [%d] Graph size = %d\n", lc, after1);
            return 1;
        }
        else
        {
            printf("T [%d] Graph size = %d\n", lc, after1);
            return 0;
        }


        break;

        case STD_UNORDERED_MAP:

        for(int i1 = 0; i1 < bucket_count; i1++)
        {
            for(int i2 = 0; i2 < inner_bucket_count; i2++)
            {
                for(int j = 0; j < r->inner_hash_data[i1][i2].size(); j = j + number_of_columns)
                {
                    lhs = r->inner_hash_data[i1][i2][j];

                    for(int k1 = 0; k1 < inner_bucket_count; k1++)
                    {
                        for(int k2 = 0; k2 < this->inner_hash_data[i1][k1].size(); k2 = k2 + number_of_columns)
                        {
                            if (lhs == this->inner_hash_data[i1][k1][k2])
                            {
                                two_tuple temp = {this->inner_hash_data[i1][k1][k2 + 1], r->inner_hash_data[i1][i2][j+1]};
                                output_set.insert(temp);
                            }
                        }
                    }
                }
            }
        }

        break;

        case LINEAR_PROBING_HASH:

        for(int i1 = 0; i1 < bucket_count; i1++)
        {
            for(int i2 = 0; i2 < inner_bucket_count; i2++)
            {
                for(int j = 0; j < r->inner_hash_data[i1][i2].size(); j = j + number_of_columns)
                {
                    lhs = r->inner_hash_data[i1][i2][j];

                    for(int k1 = 0; k1 < inner_bucket_count; k1++)
                    {
                        for(int k2 = 0; k2 < this->inner_hash_data[i1][k1].size(); k2 = k2 + number_of_columns)
                        {
                            if (lhs == this->inner_hash_data[i1][k1][k2])
                            {
                                hash = all_column_hash((uint64_t)this->inner_hash_data[i1][k1][k2 + 1], (uint64_t)r->inner_hash_data[i1][i2][j+1]);
                                two_tuple temp = {this->inner_hash_data[i1][k1][k2 + 1], r->inner_hash_data[i1][i2][j+1]};
                                join_hash.insert(hash, temp, true);
                            }
                        }
                    }
                }
            }
        }

        break;

        default:
        break;

    }
    j2 = MPI_Wtime();



    t1 = MPI_Wtime();

    {
        // Send Join output
        /* process_size[j] stores the number of samples to be sent to process with rank j */
        c1 = MPI_Wtime();

        int process_size[nprocs];
        memset(process_size, 0, nprocs * sizeof(int));

        /* vector[i] contains the data that needs to be sent to process i */
        std::vector<int> *process_data_vector;
        process_data_vector = new std::vector<int>[nprocs];

        switch (data_structure)
        {

            case VECTOR_HASH:
#if 0
            for (int i = 0; i < join_output_bucket_size; i++)
            {
                for(int j = 0; j < join_output[i].size(); j = j + number_of_columns)
                {
                    uint64_t index = outer_hash((uint64_t)join_output[i][j])%nprocs;
                    process_size[index] = process_size[index] + number_of_columns;
                    for(int k = j; k < j + number_of_columns; k++)
                        process_data_vector[index].push_back(join_output[i][k]);
                }
            }
#endif
            break;

            case LINEAR_PROBING_HASH:
            for (hashtable<two_tuple, bool>::iter it = join_hash.iterator(); it.more(); it.advance())
            {
                two_tuple temp = it.getK();
                uint64_t index = outer_hash(temp.a)%nprocs;
                process_size[index] = process_size[index] + number_of_columns;
                process_data_vector[index].push_back(temp.a);
                process_data_vector[index].push_back(temp.b);
            }
            break;

            case STD_UNORDERED_MAP:
            for (const two_tuple& tup: output_set)
            {
                uint64_t index = outer_hash(tup.a)%nprocs;
                process_size[index] = process_size[index] + number_of_columns;
                process_data_vector[index].push_back(tup.a);
                process_data_vector[index].push_back(tup.b);
            }
            break;
        }


        int prefix_sum_process_size[nprocs];
        memset(prefix_sum_process_size, 0, nprocs * sizeof(int));
        for(int i = 1; i < nprocs; i++)
            prefix_sum_process_size[i] = prefix_sum_process_size[i - 1] + process_size[i - 1];

        int process_data_buffer_size = prefix_sum_process_size[nprocs - 1] + process_size[nprocs - 1];

        int* process_data;
        try
        {
            process_data = new int[process_data_buffer_size];
            memset(process_data, 0, process_data_buffer_size * sizeof(int));
        }
        catch (const std::bad_alloc& e)
        {
            printf("[1] Allocation failed: %s\n", e.what());
            printf("R: %d %d\n", rank, process_data_buffer_size);
        }

        for(int i = 0; i < nprocs; i++)
            memcpy(process_data + prefix_sum_process_size[i], &process_data_vector[i][0], process_data_vector[i].size() * sizeof(int));

        delete[] process_data_vector;
        c2 = MPI_Wtime();


        /* This step prepares for actual data transfer */
        /* Every process sends to every other process the amount of data it is going to send */
        b1 = MPI_Wtime();

        int recv_process_size_buffer[nprocs];
        memset(recv_process_size_buffer, 0, nprocs * sizeof(int));
        MPI_Alltoall(process_size, 1, MPI_INT, recv_process_size_buffer, 1, MPI_INT, comm);

        int prefix_sum_recv_process_size_buffer[nprocs];
        memset(prefix_sum_recv_process_size_buffer, 0, nprocs * sizeof(int));
        for(int i = 1; i < nprocs; i++)
            prefix_sum_recv_process_size_buffer[i] = prefix_sum_recv_process_size_buffer[i - 1] + recv_process_size_buffer[i - 1];

        /* Sending data to all processes */
        /* What is the buffer size to allocate */
        int outer_hash_buffer_size = 0;
        for(int i = 0; i < nprocs; i++)
            outer_hash_buffer_size = outer_hash_buffer_size + recv_process_size_buffer[i];

        int *hash_buffer;
        try
        {
            hash_buffer = new int[outer_hash_buffer_size];
            memset(hash_buffer, 0, outer_hash_buffer_size * sizeof(int));
        }
        catch (const std::bad_alloc& e)
        {
            printf("[2] Allocation failed: %s\n", e.what());
            printf("R: %d %d\n", rank, outer_hash_buffer_size);
        }


        MPI_Alltoallv(process_data, process_size, prefix_sum_process_size, MPI_INT, hash_buffer, recv_process_size_buffer, prefix_sum_recv_process_size_buffer, MPI_INT, comm);
        b2 = MPI_Wtime();


        m1 = MPI_Wtime();
        before1 = 0;
        for(int b = 0; b < bucket_count; b++)
            for(int a = 0; a < inner_bucket_count; a++)
                before1 = before1 + r->inner_hash_data[b][a].size();

        r->insert(hash_buffer, outer_hash_buffer_size);

        after1 = 0;
        for(int b = 0; b < bucket_count; b++)
            for(int a = 0; a < inner_bucket_count; a++)
                after1 = after1 + r->inner_hash_data[b][a].size();

        delete[] hash_buffer;
        delete[] process_data;
        m2 = MPI_Wtime();

    }
    t2 = MPI_Wtime();


    //delete[] join_output;
    t4 = MPI_Wtime();

    cond1 = MPI_Wtime();
    int done;
    if (before1 == after1 && before2 == after2)
        done = 1;
    else
        done = 0;

    int sum = 0;
    MPI_Allreduce(&done, &sum, 1, MPI_INT, MPI_SUM, comm);
    cond2 = MPI_Wtime();

    total2 = MPI_Wtime();

    int fsum = 0;
    MPI_Allreduce(&after1, &fsum, 1, MPI_INT, MPI_SUM, comm);
    int fsum2 = 0;
    MPI_Allreduce(&after2, &fsum2, 1, MPI_INT, MPI_SUM, comm);

    double total_time1 = ((j2 - j1) + (c2 - c1) + (b2 - b1) + (m2 - m1) + (cond2 - cond1));
    double total_time2 = (j2 - j1) + (t2 - t1) + (cond2 - cond1);

    if (sum == nprocs)
    {
        if (rank == 0)
            printf("[T] %d: [%d %d %f %f %f] Join %f R1 [%f = %f + %f + %f] C %f\n", lc, fsum, fsum2, (total2 - total1), total_time1, total_time2, (j2 - j1), (t2 - t1), (c2 - c1), (b2 - b1), (m2 - m1), (cond2 - cond1));
        return 1;
    }
    else
    {
        if (rank == 0)
            printf("[F] %d: [%d %d %f %f %f] Join %f R1 [%f = %f + %f + %f] C %f\n", lc, fsum, fsum2, (total2 - total1), total_time1, total_time2, (j2 - j1), (t2 - t1), (c2 - c1), (b2 - b1), (m2 - m1), (cond2 - cond1));
        return 0;
    }
}



void relation::insert(int *buffer, int buffer_size)
{
    for(int k = 0; k < buffer_size; k = k + number_of_columns)
    {
        int count = 0;
        int bucket_id = inner_hash((uint64_t)buffer[k]) % bucket_count;
        //int inner_bucket_id = outer_hash((uint64_t)buffer[k] ^ (uint64_t)buffer[k + 1]) % inner_bucket_count;
        int inner_bucket_id = all_column_hash((uint64_t)buffer[k], (uint64_t)buffer[k + 1]) % inner_bucket_count;
        //printf("bucket id %d %d\n", bucket_id, buffer[k]);

        for(int i = 0; i < this->inner_hash_data[bucket_id][inner_bucket_id].size(); i = i + number_of_columns)
        {
            if (this->inner_hash_data[bucket_id][inner_bucket_id][i] == buffer[k] && this->inner_hash_data[bucket_id][inner_bucket_id][i + 1] == buffer[k + 1])
            {
                count = 0;
                break;
            }
            else
                count++;
        }

        if (count == this->inner_hash_data[bucket_id][inner_bucket_id].size() / number_of_columns)
        {
            this->inner_hash_data[bucket_id][inner_bucket_id].push_back(buffer[k]);
            this->inner_hash_data[bucket_id][inner_bucket_id].push_back(buffer[k + 1]);
            //if (rank == 0)
            //printf("val %d %d\n", buffer[k], buffer[k + 1]);
        }
    }

    return;
}

#if 0
static int inner_hash( uint32_t a)
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
#endif


static u64 all_column_hash(u64 a, u64 b)
{
    if (a >= b)
        return a * a + a + b;
    else
        return a + b * b;
}


static u64 outer_hash(u64 val)
{

    const u64 fnvprime = 0x100000001b3;
    const u64 fnvoffset = 0xcbf29ce484222325;

    u64 chunks[9];
    chunks[0] = (val >> 56);
    chunks[1] = (val >> 48) & 0x00000000000000ff;
    chunks[2] = (val >> 40) & 0x00000000000000ff;
    chunks[3] = (val >> 32) & 0x00000000000000ff;
    chunks[4] = (val >> 24) & 0x00000000000000ff;
    chunks[5] = (val >> 16) & 0x00000000000000ff;
    chunks[6] = (val >> 8) & 0x00000000000000ff;
    chunks[7] = val & 0x00000000000000ff;
    chunks[8] = 0x0000000000000003;

    val = fnvoffset;
    for (u64 i = 0; i < 9; ++i)
    {
        val = val ^ chunks[i];
        val = val * fnvprime;
    }

    return val;
}

static u64 inner_hash(u64 val)
{
    const u64 fnvprime = 0x100000001b3;
    const u64 fnvoffset = 0xcbf29ce484222325;

    u64 chunks[9];
    chunks[0] = (val >> 56);
    chunks[1] = (val >> 48) & 0x00000000000000ff;
    chunks[2] = (val >> 40) & 0x00000000000000ff;
    chunks[3] = (val >> 32) & 0x00000000000000ff;
    chunks[4] = (val >> 24) & 0x00000000000000ff;
    chunks[5] = (val >> 16) & 0x00000000000000ff;
    chunks[6] = (val >> 8) & 0x00000000000000ff;
    chunks[7] = val & 0x00000000000000ff;
    chunks[8] = 0x0000000000000017;

    val = fnvoffset;
    for (u64 i = 0; i < 9; ++i)
    {
        val = val ^ chunks[i];
        val = val * fnvprime;
    }

    return val;
}
