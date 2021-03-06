#ifndef RELATION_H
#define RELATION_H

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <mpi.h>
#include "../hash/hashtable.h"
#include "../hash/hashset.h"
#include "../hash/twohashtable_vector.h"

#define COL_COUNT 2


struct two_tuple {
    uint64_t a;
    uint64_t b;
    bool operator ==(const two_tuple& o) const {
        return a==o.a && b==o.b;
    };
};



class relation{

private:

    int rank;
    int nprocs;
    MPI_Comm comm;

    int rule;

    uint global_number_of_rows;
    uint local_number_of_rows;

    int **initial_data;

    uint outer_hash_buffer_size;
    int **outer_hash_data;

    uint number_of_inner_hash_buckets;
    int *inner_hash_bucket_size;


    hashset<two_tuple>* t_inner_hash;


public:

    relation();
    relation(const relation &r);
    relation(int r, int n, MPI_Comm c, int grc, int lrc);

    /* MPI Setup */
    void set_rank (int r) {rank = r;}
    void set_nprocs (int n) {nprocs = n;}
    void set_comm (MPI_Comm c) {comm = c;}

    void create_hash_buckets();
    void free_hash_buckets();

    int get_hash_size() {return t_inner_hash->size();}
    void cleanupall();
    void cleanup();

    int get_number_of_global_rows() {return global_number_of_rows;}
    void set_number_of_global_rows (int rc) {global_number_of_rows = rc;}

    int get_number_of_local_rows() {return local_number_of_rows;}
    void set_number_of_local_rows(int rc) {local_number_of_rows = rc;}

    int get_rule() {return rule;}
    void set_rule(int rc) {rule = rc;}

    //int get_ds() {return data_structure;}
    //void set_ds(int rc) {data_structure = rc;}

    /* Setting up buffer for first I/O */
    void create_init_data();
    void assign_init_data(int* data);
    void assign_inverted_data(int* data);
    void print_init_data(char* filename);
    void free_init_data();

    /* Setting up outer hash */
    void hash_init_data(int hash_column);
    void print_outer_hash_data(char* filename);
    void hash_init_data_free();

    /* Setting up inner hash */
    void set_number_of_inner_hash_buckets(int b) {number_of_inner_hash_buckets = b;}
    int get_number_of_inner_hash_buckets() {return number_of_inner_hash_buckets;}
    void inner_hash_perform();
    void print_inner_hash_data(char* filename);


    //int join(relation* r, int lc);
    int join(relation* r, relation* dt, int lc);
    void insert(int* buffer, int buffer_size, relation* dt);
};


#endif
