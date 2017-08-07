#pragma once

#include <string.h>
#include "compat.h"
#include <new>
#include <iostream>
#include <assert.h>

#define OUTER_BUCKET_COUNT 256
#define INNER_BUCKET_COUNT 256
#define COLUMN_COUNT 2


template<typename K1, typename K2>
class vector_hashset
{
    public:
        uint bucket_count;
        uint inner_bucket_count;
        std::vector<int> **inner_hash_data;


        inline void create_hash_data()
        {
            inner_hash_data = new std::vector<int>*[OUTER_BUCKET_COUNT];
            for (uint i = 0; i < OUTER_BUCKET_COUNT; i++)
                inner_hash_data[i] = new std::vector<int>[INNER_BUCKET_COUNT];
        }

        inline void free_hash_data()
        {
            for(uint i = 0; i < OUTER_BUCKET_COUNT; ++i)
                delete[] inner_hash_data[i];

            delete[] inner_hash_data;
        }

        inline bool add(K1 val1, K2 val2, u32 outer_hash, u32 inner_hash)
        {
            outer_hash = outer_hash % OUTER_BUCKET_COUNT;
            inner_hash = inner_hash % INNER_BUCKET_COUNT;

            int count = 1;
            for(uint i = 0; i < inner_hash_data[outer_hash][inner_hash].size(); i = i + COLUMN_COUNT)
            {
                if (inner_hash_data[outer_hash][inner_hash][i] == val1 && inner_hash_data[outer_hash][inner_hash][i + 1] == val2)
                {
                    count = 0;
                    break;
                }
            }

            if (count == 1)
            {
                inner_hash_data[outer_hash][inner_hash].push_back(val1);
                inner_hash_data[outer_hash][inner_hash].push_back(val2);
                return true;
            }
            else
                return false;
        }
};
