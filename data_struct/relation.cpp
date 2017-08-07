#include "relation.h"
#include <unordered_set>

typedef uint64_t u64;

static u64 inner_hash(u64);
static u64 outer_hash(u64);
static u64 all_column_hash(u64, u64);

//static int data_structure = TWO_LEVEL_HASH;
static int data_structure = VECTOR_HASH;

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
    for(uint i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
        outer_hash_data[i] = new int[number_of_columns];

    for(uint i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
        memcpy(outer_hash_data[i], r.outer_hash_data[i], number_of_columns * sizeof(int));
}


void relation::create_hash_buckets(int bc, int ibc)
{
    t_inner_hash = new hashset<two_tuple>();
    s_inner_hash = new vector_hashset<int, int>();
    s_inner_hash->create_hash_data();
}


void relation::free_hash_buckets()
{
    delete t_inner_hash;

    s_inner_hash->free_hash_data();
    delete s_inner_hash;
}



void relation::create_init_data()
{
    initial_data = new int*[local_number_of_rows];
    for(uint i = 0; i < local_number_of_rows; ++i)
        initial_data[i] = new int[number_of_columns];
}

void relation::assign_init_data(int* data)
{
    int count = 0;
    for(uint i = 0; i < local_number_of_rows; ++i)
    {
        for(uint j = 0; j < number_of_columns; ++j)
        {
            initial_data[i][j] = data[count++];
        }
    }
}


// TODO: this is hardcoded completely inverts
void relation::assign_inverted_data(int* data)
{
    int count = 0;
    for(uint i = 0; i < local_number_of_rows; ++i)
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

    for(uint i = 0; i < local_number_of_rows; ++i)
    {
        for(uint j = 0; j < number_of_columns; ++j)
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

    for(uint i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
    {
        for(uint j = 0; j < number_of_columns; ++j)
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
    for(uint i = 0; i < local_number_of_rows; ++i)
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
    for (uint i = 0; i < local_number_of_rows; ++i)
    {
        uint64_t index = initial_data[i][hash_column_index];
        process_size[outer_hash(index)%nprocs] = process_size[outer_hash(index)%nprocs] + number_of_columns;
        for (uint j = 0; j < number_of_columns; j++)
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
    for(uint i = 0; i < (outer_hash_buffer_size / number_of_columns); ++i)
        outer_hash_data[i] = new int[number_of_columns];

#if 1
    uint total_row_size = 0;
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
    for(uint i = 0; i < outer_hash_buffer_size; i++)
        outer_hash_data[i / number_of_columns][i % number_of_columns] = hash_buffer[i];

    int hash_column = 0;
    int bucket_id;
    int inner_bucket_id = 0;
    for(uint i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
    {
        bucket_id = inner_hash((uint64_t)outer_hash_data[i][hash_column]);
        inner_bucket_id = all_column_hash((uint64_t)outer_hash_data[i][hash_column], (uint64_t)outer_hash_data[i][hash_column + 1]);

        two_tuple* tup = new two_tuple();
        tup->a = (uint64_t)outer_hash_data[i][0];
        tup->b = (uint64_t)outer_hash_data[i][1];
        const two_tuple* sttup = t_inner_hash->add(tup, bucket_id, inner_bucket_id);
        if (sttup != tup)
            delete tup;

        s_inner_hash->add(outer_hash_data[i][0], outer_hash_data[i][1], bucket_id, inner_bucket_id);
    }

    return;
}


void relation::hash_init_data_free()
{
    for(uint i = 0; i < outer_hash_buffer_size / number_of_columns; ++i)
        delete[] outer_hash_data[i];

    delete[] outer_hash_data;
    return;
}




void relation::print_inner_hash_data(char* filename)
{
#if 1
    FILE * fp = fopen(filename, "w");

    for(uint i = 0; i < OUTER_BUCKET_COUNT; ++i)
    {
        //fprintf(fp, "[%lld] Bucket size %d\n", (unsigned long long)i, (int)inner_hash_data[i].size());
        for(uint k = 0; k < INNER_BUCKET_COUNT; ++k)
        {
            for(uint j = 0; j < s_inner_hash->inner_hash_data[i][k].size(); ++j)
            {
                if (j % number_of_columns != number_of_columns - 1)
                    fprintf(fp, "%lld\t", (unsigned long long)s_inner_hash->inner_hash_data[i][k][j]);
                else
                    fprintf(fp, "%lld\n", (unsigned long long)s_inner_hash->inner_hash_data[i][k][j]);
            }
        }
    }
    fclose(fp);
#endif
    return;
}


int relation::join(relation* r, int lc)
{
    uint join_output_bucket_size = 10000;//256;
    std::vector<int> *join_output;
    join_output = new std::vector<int>[join_output_bucket_size];

    double total1, total2;
    double j1, j2;
    double b1, b2, b3, b4;
    double c1, c2, c3, c4;
    double m1, m2, m3, m4;
    double cond1, cond2;

    total1 = MPI_Wtime();

    j1 = MPI_Wtime();
    u32 hash1 = 0;
    u32 hash2 = 0;
    u64 index = 0;
    int count = 1;
    int before1 = 0, after1 = 0, before2 = 0, after2 = 0;


    hashset<two_tuple>* st = new hashset<two_tuple>();

    int insert_t = 0;
    int insert_f = 0;

    switch (data_structure)
    {
        case VECTOR_HASH:
        for(uint i1 = 0; i1 < OUTER_BUCKET_COUNT; i1++)
        {
            for(uint i2 = 0; i2 < INNER_BUCKET_COUNT; i2++)
            {
                for(uint j = 0; j < r->s_inner_hash->inner_hash_data[i1][i2].size(); j = j + number_of_columns)
                {
                    for(uint k1 = 0; k1 < INNER_BUCKET_COUNT; k1++)
                    {
                        for(uint k2 = 0; k2 < this->s_inner_hash->inner_hash_data[i1][k1].size(); k2 = k2 + number_of_columns)
                        {
                            if (r->s_inner_hash->inner_hash_data[i1][i2][j] == this->s_inner_hash->inner_hash_data[i1][k1][k2])
                            {
                                index = all_column_hash((uint64_t)this->s_inner_hash->inner_hash_data[i1][k1][k2 + 1], (uint64_t)r->s_inner_hash->inner_hash_data[i1][i2][j+1]) % join_output_bucket_size;



                                if (join_output[index].size() == 0)
                                {
                                    join_output[index].push_back(this->s_inner_hash->inner_hash_data[i1][k1][k2 + 1]);
                                    join_output[index].push_back(r->s_inner_hash->inner_hash_data[i1][i2][j+1]);
                                    //printf("H [%d] --> %d %d\n", r->s_inner_hash->inner_hash_data[i1][i2][j], this->s_inner_hash->inner_hash_data[i1][k1][k2 + 1], r->s_inner_hash->inner_hash_data[i1][i2][j+1]);
                                    insert_t++;
                                }
                                else
                                {
                                    count = 1;
                                    for (uint m = 0; m < join_output[index].size(); m = m + number_of_columns)
                                    {
                                        if (this->s_inner_hash->inner_hash_data[i1][k1][k2 + 1] == join_output[index][m] && r->s_inner_hash->inner_hash_data[i1][i2][j+1] == join_output[index][m + 1])
                                        {
                                            //printf("M [%d] --> %d %d\n", r->s_inner_hash->inner_hash_data[i1][i2][j], this->s_inner_hash->inner_hash_data[i1][k1][k2 + 1], r->s_inner_hash->inner_hash_data[i1][i2][j+1]);
                                            count = 0;
                                            break;
                                        }
                                    }
                                    if (count == 1)
                                    {
                                        join_output[index].push_back(this->s_inner_hash->inner_hash_data[i1][k1][k2 + 1]);
                                        join_output[index].push_back(r->s_inner_hash->inner_hash_data[i1][i2][j+1]);

                                        insert_t++;
                                    }
                                    else
                                        insert_f++;
                                }
                            }
                        }
                    }
                }
            }
        }
        break;


        case TWO_LEVEL_HASH:

        for (u32 i1 = 0; i1 < t_inner_hash->bucket_count(); ++i1)
        {
            for (hashset<two_tuple>::bucket_iter it(*(r->t_inner_hash), i1); it.more(); ++it)
            {
                const two_tuple* tup = it.get();
                for (hashset<two_tuple>::bucket_iter it2(*(t_inner_hash), i1); it2.more(); ++it2)
                {
                    const two_tuple* tup2 = it2.get();

                    if (tup->a == tup2->a)
                    {
                        hash1 = inner_hash((uint64_t)tup2->b);
                        hash2 = all_column_hash((uint64_t)tup2->b, (uint64_t)tup->b);

                        two_tuple* tup3 = new two_tuple();
                        tup3->a = (uint64_t)tup2->b;
                        tup3->b = (uint64_t)tup->b;
                        const two_tuple* sttup = st->add(tup3, hash1, hash2);
                        if (sttup != tup3)
                        {
                            insert_f++;
                            delete tup3;
                        }
                        else
                            insert_t++;
                    }
                }
            }
        }

        break;

        default:
        break;

    }

    //MPI_Barrier(comm);
    j2 = MPI_Wtime();

#if 1
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
            for (uint i = 0; i < join_output_bucket_size; i++)
            {
                for(uint j = 0; j < join_output[i].size(); j = j + number_of_columns)
                {
                    uint64_t index = outer_hash((uint64_t)join_output[i][j])%nprocs;
                    process_size[index] = process_size[index] + number_of_columns;
                    process_data_vector[index].push_back(join_output[i][j]);
                    process_data_vector[index].push_back(join_output[i][j + 1]);
                }
            }
            break;


            case TWO_LEVEL_HASH:
            for (u32 bi = 0; bi < st->bucket_count(); ++bi)
            {
                for (hashset<two_tuple>::bucket_iter it(*st, bi); it.more(); ++it)
                {
                    const two_tuple* tup = it.get();
                    uint64_t index = outer_hash(tup->a)%nprocs;
                    process_size[index] = process_size[index] + number_of_columns;
                    process_data_vector[index].push_back(tup->a);
                    process_data_vector[index].push_back(tup->b);
                }
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

        if (data_structure == VECTOR_HASH)
        {

            before1 = 0;
            for(uint b = 0; b < OUTER_BUCKET_COUNT; b++)
                for(uint a = 0; a < INNER_BUCKET_COUNT; a++)
                    before1 = before1 + r->s_inner_hash->inner_hash_data[b][a].size();

            r->insert(hash_buffer, outer_hash_buffer_size);

            after1 = 0;
            for(uint b = 0; b < OUTER_BUCKET_COUNT; b++)
                for(uint a = 0; a < INNER_BUCKET_COUNT; a++)
                    after1 = after1 + r->s_inner_hash->inner_hash_data[b][a].size();
        }
        else
        {
            before1 = 0;
            for(uint b = 0; b < r->t_inner_hash->bucket_count(); b++)
                for (hashset<two_tuple>::bucket_iter it(*(r->t_inner_hash), b); it.more(); ++it)
                    before1 = before1 + it.get_bsize();

            r->insert(hash_buffer, outer_hash_buffer_size);

            after1 = 0;
            for(uint b = 0; b < r->t_inner_hash->bucket_count(); b++)
                for (hashset<two_tuple>::bucket_iter it(*(r->t_inner_hash), b); it.more(); ++it)
                    after1 = after1 + it.get_bsize();
        }

        delete[] hash_buffer;
        delete[] process_data;
        m2 = MPI_Wtime();
    }


    if (this->rule == 1)
    {
        // Send Join output
        /* process_size[j] stores the number of samples to be sent to process with rank j */
        c3 = MPI_Wtime();

        int process_size[nprocs];
        memset(process_size, 0, nprocs * sizeof(int));

        /* vector[i] contains the data that needs to be sent to process i */
        std::vector<int> *process_data_vector;
        process_data_vector = new std::vector<int>[nprocs];

        switch (data_structure)
        {

            case VECTOR_HASH:
            for (uint i = 0; i < join_output_bucket_size; i++)
            {
                for(uint j = (number_of_columns - 1); j < join_output[i].size(); j = j + number_of_columns)
                {
                    uint64_t index = outer_hash((uint64_t)join_output[i][j])%nprocs;
                    process_size[index] = process_size[index] + number_of_columns;
                    process_data_vector[index].push_back(join_output[i][j]);
                    process_data_vector[index].push_back(join_output[i][j-1]);
                }
            }
            break;

            case TWO_LEVEL_HASH:
            for (u32 bi = 0; bi < st->bucket_count(); ++bi)
            {
                for (hashset<two_tuple>::bucket_iter it(*st, bi); it.more(); ++it)
                {
                    const two_tuple* tup = it.get();
                    uint64_t index = outer_hash(tup->b)%nprocs;
                    process_size[index] = process_size[index] + number_of_columns;
                    process_data_vector[index].push_back(tup->b);
                    process_data_vector[index].push_back(tup->a);
                    delete tup;
                }
            }
            delete st;
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
        c4 = MPI_Wtime();


        /* This step prepares for actual data transfer */
        /* Every process sends to every other process the amount of data it is going to send */
        b3 = MPI_Wtime();

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
        b4 = MPI_Wtime();


        m3 = MPI_Wtime();
        if (data_structure == VECTOR_HASH)
        {
            before2 = 0;
            for(uint b = 0; b < OUTER_BUCKET_COUNT; b++)
                for(uint a = 0; a < INNER_BUCKET_COUNT; a++)
                    before2 = before2 + this->s_inner_hash->inner_hash_data[b][a].size();

            this->insert(hash_buffer, outer_hash_buffer_size);

            after2 = 0;
            for(uint b = 0; b < OUTER_BUCKET_COUNT; b++)
                for(uint a = 0; a < INNER_BUCKET_COUNT; a++)
                    after2 = after2 + this->s_inner_hash->inner_hash_data[b][a].size();
        }
        else
        {
            before2 = 0;
            for(uint b = 0; b < this->t_inner_hash->bucket_count(); b++)
                for (hashset<two_tuple>::bucket_iter it(*(t_inner_hash), b); it.more(); ++it)
                    before2 = before2 + it.get_bsize();

            this->insert(hash_buffer, outer_hash_buffer_size);

            after2 = 0;
            for(uint b = 0; b < this->t_inner_hash->bucket_count(); b++)
                for (hashset<two_tuple>::bucket_iter it(*(t_inner_hash), b); it.more(); ++it)
                    after2 = after2 + it.get_bsize();
        }

        delete[] hash_buffer;
        delete[] process_data;
        m4 = MPI_Wtime();
    }


    delete[] join_output;

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

    double join_time = j2 - j1;
    double comm1 = (c2 - c1) + (b2 - b1) + (m2 - m1);
    double comm2 = (c4 - c3) + (b4 - b3) + (m4 - m3);
    double cond = (cond2 - cond1);

    double total_time = join_time + comm1 + comm2 + cond;

    if (sum == nprocs)
    {
        if (rank == 0)
            printf("[T %d] ] [%d %d] %d: [%d %d %f %f] Join %f R1 [%f = %f + %f + %f] R2 [%f = %f + %f + %f] C %f\n", data_structure, insert_t, insert_f, lc, fsum/2, fsum2/2, (total2 - total1), total_time, (j2 - j1), comm1, (c2 - c1), (b2 - b1), (m2 - m1), comm2, (c4 - c3), (b4 - b3), (m4 - m3), (cond2 - cond1));
        return 1;
    }
    else
    {
        if (rank == 0)
            printf("[F %d] [%d %d] %d: [%d %d %f %f] Join %f R1 [%f = %f + %f + %f] R2 [%f = %f + %f + %f] C %f\n", data_structure, insert_t, insert_f, lc, fsum/2, fsum2/2, (total2 - total1), total_time, (j2 - j1), comm1, (c2 - c1), (b2 - b1), (m2 - m1), comm2, (c4 - c3), (b4 - b3), (m4 - m3), (cond2 - cond1));
        return 0;
    }

#endif
}



void relation::insert(int *buffer, int buffer_size)
{

    for(int k = 0; k < buffer_size; k = k + number_of_columns)
    {
        int bucket_id = inner_hash((uint64_t)buffer[k]);
        int inner_bucket_id = all_column_hash((uint64_t)buffer[k], (uint64_t)buffer[k + 1]);

        if (data_structure == TWO_LEVEL_HASH)
        {
            two_tuple* tup = new two_tuple();
            tup->a = (uint64_t)buffer[k];
            tup->b = (uint64_t)buffer[k + 1];
            const two_tuple* sttup = t_inner_hash->add(tup, bucket_id, inner_bucket_id);
            if (sttup != tup)
                delete tup;
        }
        else
            s_inner_hash->add(buffer[k], buffer[k + 1], bucket_id, inner_bucket_id);
    }

    return;
}


inline static u64 tunedhash(const u8* bp, const u32 len)
{ 
    u64 h0 = 0xb97a19cb491c291d;
    u64 h1 = 0xc18292e6c9371a17;
    const u8* const ep = bp+len;
    while (bp < ep)
    {
        h1 ^= *bp;
        h1 *= 31; 
        h0 ^= (((u64)*bp) << 17) ^ *bp;
        h0 *= 0x100000001b3;
        h0 = (h0 >> 7) | (h0 << 57);
        ++bp;
    }
    
    return h0 ^ h1;// ^ (h1 << 31);
}

static u64 all_column_hash(u64 a, u64 b)
{
    const u64 both[2] = {a,b};
    return tunedhash((u8*)(&both),2*sizeof(u64));
}


static u64 outer_hash(const u64 val)
{
    return tunedhash((u8*)(&val),sizeof(u64));
}

static u64 inner_hash(u64 val)
{
    val = (val << 23) | (val >> 41);
    return tunedhash((u8*)(&val),sizeof(u64));
}
