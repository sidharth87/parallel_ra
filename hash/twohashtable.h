// Copyright (C) Thomas Gilray, 2017
// See the notice in LICENSE.md

#ifndef TWOHASHTABLE__H
#define TWOHASHTABLE__H

#include "compat.h"
#include "hashtable.h"
#include <new>
#include <math.h>

#if 0
// Reallocating 2-layer hashtable class
template<typename K, typename V>
class twohashtable
{
private:
    array<hashtable<K, V> > tables;
    u64 size;

public:
    // Constructs an empty table
    hashtable(u32 buckets = 1024)
      : tables(), size(0)
    {
	tables.reallocate(buckets);
        for (u32 i = 0; i < buckets; ++i)
	    tables.push_back(hashtable<K,V>());
    }

    hashtable<K,V>::iter bucketiterator(u64 h0)
    {
	return tables[h0%tables.size()].iterator();
    }

    V* find(u64 h0, h64 h1, const K& key)
    {
        h0 = h0 % tables.size();
	return tables[h0].find(h1, key);
    }

    bool insert(u64 h0, u64 h1, const K& key, const V& value)
    {
	h0 = h0 % tables.size();
	return tables[h0].insert(h1, key, value);
    }

    // Returns the number of key-value pairs
    u64 count() const
    {
        return size;
    }
};

#endif
#endif // HASHTABLE__H


