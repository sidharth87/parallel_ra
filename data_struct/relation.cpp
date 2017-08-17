#include "relation.h"

typedef uint64_t u64;

static u64 inner_hash(u64);
static u64 outer_hash(u64);
static u64 all_column_hash(u64, u64);

relation::relation()
{
    rank = 0;
    nprocs = 1;
}

void relation::cleanupall()
{
    this->t_inner_hash->deleteAllElements();
    delete this->t_inner_hash;

    return;
}


void relation::cleanup()
{
    delete this->t_inner_hash;

    return;
}


relation::relation(int r, int n, MPI_Comm c, int grc, int lrc)
{
    rank = r;
    nprocs = n;
    comm = c;
    global_number_of_rows = grc;
    local_number_of_rows = lrc;
}

relation::relation(const relation &r)
{
    rank = r.rank;
    nprocs = r.nprocs;
    comm = r.comm;
    global_number_of_rows = r.global_number_of_rows;
    local_number_of_rows = r.local_number_of_rows;

    outer_hash_buffer_size = r.outer_hash_buffer_size;

    outer_hash_data = new int*[outer_hash_buffer_size / COL_COUNT];
    for(uint i = 0; i < outer_hash_buffer_size / COL_COUNT; ++i)
        outer_hash_data[i] = new int[COL_COUNT];

    for(uint i = 0; i < outer_hash_buffer_size / COL_COUNT; ++i)
        memcpy(outer_hash_data[i], r.outer_hash_data[i], COL_COUNT * sizeof(int));
}


void relation::create_hash_buckets()
{
    t_inner_hash = new hashset<two_tuple>();
    return;
}


void relation::free_hash_buckets()
{
    delete t_inner_hash;
    return;
}



void relation::create_init_data()
{
    initial_data = new int*[local_number_of_rows];
    for(uint i = 0; i < local_number_of_rows; ++i)
        initial_data[i] = new int[COL_COUNT];
}

void relation::assign_init_data(int* data)
{
    int count = 0;
    for(uint i = 0; i < local_number_of_rows; ++i)
        for(uint j = 0; j < COL_COUNT; ++j)
            initial_data[i][j] = data[count++];
}


// TODO: this is hardcoded completely inverts
void relation::assign_inverted_data(int* data)
{
    int count = 0;
    for(uint i = 0; i < local_number_of_rows; ++i)
    {
        for(int j = COL_COUNT - 1; j >= 0; j--)
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
        for(uint j = 0; j < COL_COUNT; ++j)
        {
            if (j == COL_COUNT - 1)
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

    for(uint i = 0; i < outer_hash_buffer_size / COL_COUNT; ++i)
    {
        for(uint j = 0; j < COL_COUNT; ++j)
        {
            if (j == COL_COUNT - 1)
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


void relation::hash_init_data(int hash_column_index)
{
    /* process_size[j] stores the number of samples to be sent to process with rank j */
    int process_size[nprocs];
    memset(process_size, 0, nprocs * sizeof(int));

    /* vector[i] contains the data that needs to be sent to process i */
    std::vector<int> process_data_vector[nprocs];
    for (uint i = 0; i < local_number_of_rows; ++i)
    {
        uint64_t index = outer_hash(initial_data[i][hash_column_index])%nprocs;
        //uint64_t index = initial_data[i][hash_column_index]%nprocs;
        process_size[index] = process_size[index] + COL_COUNT;

        for (uint j = 0; j < COL_COUNT; j++)
            process_data_vector[index].push_back(initial_data[i][j]);
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

    outer_hash_data = new int*[outer_hash_buffer_size / COL_COUNT];
    for(uint i = 0; i < (outer_hash_buffer_size / COL_COUNT); ++i)
        outer_hash_data[i] = new int[COL_COUNT];

#if 1
    uint total_row_size = 0;
    MPI_Allreduce(&outer_hash_buffer_size, &total_row_size, 1, MPI_INT, MPI_SUM, comm);
    if(total_row_size != global_number_of_rows * COL_COUNT)
    {
        printf("Incorrect distribution\n");
        MPI_Abort(comm, -1);
    }
    //printf("[%d] Buffer size after hashing %lld [%lld]\n", rank, (unsigned long long)outer_hash_buffer_size, (unsigned long long)total_row_size);
#endif

    int hash_buffer[outer_hash_buffer_size];
    MPI_Alltoallv(process_data, process_size, prefix_sum_process_size, MPI_INT, hash_buffer, recv_process_size_buffer, prefix_sum_recv_process_size_buffer, MPI_INT, comm);

    // TODO: this is hard coded for two columns
    for(uint i = 0; i < outer_hash_buffer_size; i++)
        outer_hash_data[i / COL_COUNT][i % COL_COUNT] = hash_buffer[i];

    // TODO: this is hard coded for two columns as well
    u32 bucket_id;
    u32 inner_bucket_id = 0;
    int cc1 = 0;
    for(uint i = 0; i < outer_hash_buffer_size / COL_COUNT; ++i)
    {
        bucket_id = inner_hash((uint64_t)outer_hash_data[i][hash_column_index]);
        inner_bucket_id = all_column_hash((uint64_t)outer_hash_data[i][0], (uint64_t)outer_hash_data[i][1]);

        two_tuple* tup = new two_tuple();
        tup->a = (uint64_t)outer_hash_data[i][0];
        tup->b = (uint64_t)outer_hash_data[i][1];
        const two_tuple* sttup = t_inner_hash->add(tup, bucket_id, inner_bucket_id);
        if (sttup != tup)
            delete tup;
        else
            cc1++;
    }

    return;
}


void relation::hash_init_data_free()
{
    for(uint i = 0; i < outer_hash_buffer_size / COL_COUNT; ++i)
        delete[] outer_hash_data[i];

    delete[] outer_hash_data;
    return;
}




void relation::print_inner_hash_data(char* filename)
{

    FILE * fp = fopen(filename, "w");

    for (u32 bi = 0; bi < t_inner_hash->bucket_count(); ++bi)
    {
        for (hashset<two_tuple>::bucket_iter it(*t_inner_hash, bi); it.more(); ++it)
        {
            const two_tuple* tup = it.get();
            fprintf(fp,"%d\t%d\n", (int)tup->a, (int)tup->b);
            //printf("XXX %d %d\n", (int)tup->a, (int)tup->b);
        }
    }

    fclose(fp);

    return;
}

int relation::join(relation* G, relation* dt, int lc)
{
    // counters to check for fixed point
    int before1 = 0, after1 = 0;

    // timers
    double j1 = 0, j2 = 0;
    double b1 = 0, b2 = 0;
    double m1 = 0, m2 = 0;
    double cond1 = 0, cond2 = 0;
    double total1 = 0, total2 = 0;

    // total run time start
    total1 = MPI_Wtime();

    // join time start
    j1 = MPI_Wtime();

    // hash codes
    u32 hash1 = 0;
    u32 hash2 = 0;

    // hashset storing join output
    hashset<two_tuple>* st = new hashset<two_tuple>();

    // Send Join output
    /* process_size[j] stores the number of samples to be sent to process with rank j */
    int process_size[nprocs];
    memset(process_size, 0, nprocs * sizeof(int));

    /* vector[i] contains the data that needs to be sent to process i */
    std::vector<int> *process_data_vector;
    process_data_vector = new std::vector<int>[nprocs];


    for (u32 i1 = 0; i1 < dt->t_inner_hash->bucket_count(); ++i1)
    {
        for (hashset<two_tuple>::bucket_iter it(*(G->t_inner_hash), i1); it.more(); ++it)
        {
            const two_tuple* tupG = it.get();
            for (hashset<two_tuple>::bucket_iter it2(*(dt->t_inner_hash), i1); it2.more(); ++it2)
            {
                const two_tuple* tupdT = it2.get();
                if (tupG->a == tupdT->b)
                {
                    hash1 = inner_hash((uint64_t)tupG->b);
                    hash2 = all_column_hash((uint64_t)tupdT->a, (uint64_t)tupG->b);

                    two_tuple* tup3 = new two_tuple();
                    tup3->a = (uint64_t)tupdT->a;
                    tup3->b = (uint64_t)tupG->b;

                    const two_tuple* sttup = st->add(tup3, hash1, hash2);
                    if (sttup != tup3)
                        delete tup3;
                    else
                    {
                        uint64_t index = outer_hash(tup3->b)%nprocs;
                        process_size[index] = process_size[index] + COL_COUNT;
                        process_data_vector[index].push_back(tup3->a);
                        process_data_vector[index].push_back(tup3->b);
                    }
                }
            }
        }
    }
    st->deleteAllElements();
    delete st;
    j2 = MPI_Wtime();



    b1 = MPI_Wtime();

    int prefix_sum_process_size[nprocs];
    memset(prefix_sum_process_size, 0, nprocs * sizeof(int));
    for(int i = 1; i < nprocs; i++)
        prefix_sum_process_size[i] = prefix_sum_process_size[i - 1] + process_size[i - 1];

    int process_data_buffer_size = prefix_sum_process_size[nprocs - 1] + process_size[nprocs - 1];

    int* process_data = 0;
    process_data = new int[process_data_buffer_size];
    memset(process_data, 0, process_data_buffer_size * sizeof(int));

    for(int i = 0; i < nprocs; i++)
        memcpy(process_data + prefix_sum_process_size[i], &process_data_vector[i][0], process_data_vector[i].size() * sizeof(int));

    delete[] process_data_vector;


    /* This step prepares for actual data transfer */
    /* Every process sends to every other process the amount of data it is going to send */

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

    int *hash_buffer = 0;
    hash_buffer = new int[outer_hash_buffer_size];
    memset(hash_buffer, 0, outer_hash_buffer_size * sizeof(int));


    MPI_Alltoallv(process_data, process_size, prefix_sum_process_size, MPI_INT, hash_buffer, recv_process_size_buffer, prefix_sum_recv_process_size_buffer, MPI_INT, comm);
    b2 = MPI_Wtime();

    m1 = MPI_Wtime();

    before1 = this->t_inner_hash->size();
    this->insert(hash_buffer, outer_hash_buffer_size, dt);
    after1 = this->t_inner_hash->size();

    delete[] hash_buffer;
    delete[] process_data;
    m2 = MPI_Wtime();


    cond1 = MPI_Wtime();
    int done = 0;
    if (before1 == after1)
        done = 1;

    int sum = 0;
    MPI_Allreduce(&done, &sum, 1, MPI_INT, MPI_BOR, comm);
    cond2 = MPI_Wtime();

    total2 = MPI_Wtime();

    double total_time = (j2 - j1) + (b2 - b1) + (m2 - m1) + (cond2 - cond1);

    if (sum == 1)
    {
        if (rank == 0)
            printf("[T] %d: Total %f %f Join %f Comm %f Insert %f Cond %f Packet size %d\n", lc, (total2 - total1), total_time, (j2 - j1), (b2 - b1), (m2 - m1), (cond2 - cond1), process_data_buffer_size);
        return 1;
    }
    else
    {
        if (rank == 0)
            printf("[F] %d: Total %f %f Join %f Comm %f Insert %f Cond %f Packet size %d\n", lc, (total2 - total1), total_time, (j2 - j1), (b2 - b1), (m2 - m1), (cond2 - cond1), process_data_buffer_size);
        return 0;
    }
}



void relation::insert(int *buffer, int buffer_size, relation* dt)
{
    hashset<two_tuple>* dt_temp = new hashset<two_tuple>();
    for(int k = 0; k < buffer_size; k = k + COL_COUNT)
    {
        u32 bucket_id = inner_hash((uint64_t)buffer[k + 1]);
        u32 inner_bucket_id = all_column_hash((uint64_t)buffer[k], (uint64_t)buffer[k + 1]);

        two_tuple* tup = new two_tuple();
        tup->a = (uint64_t)buffer[k];
        tup->b = (uint64_t)buffer[k + 1];

        const two_tuple* sttup = t_inner_hash->add(tup, bucket_id, inner_bucket_id);
        if (sttup != tup)
            delete tup;
        else
            dt_temp->add(tup, bucket_id, inner_bucket_id);
    }

    delete dt->t_inner_hash;
    dt->t_inner_hash = dt_temp;
    return;
}


void relation::insert(int *buffer, int buffer_size)
{
    for(int k = 0; k < buffer_size; k = k + COL_COUNT)
    {
        u32 bucket_id = inner_hash((uint64_t)buffer[k]);
        u32 inner_bucket_id = all_column_hash((uint64_t)buffer[k], (uint64_t)buffer[k + 1]);


        two_tuple* tup = new two_tuple();
        tup->a = (uint64_t)buffer[k];
        tup->b = (uint64_t)buffer[k + 1];
        const two_tuple* sttup = t_inner_hash->add(tup, bucket_id, inner_bucket_id);
        if (sttup != tup)
            delete tup;

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
