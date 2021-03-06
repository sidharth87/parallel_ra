// Copyright (C) Thomas Gilray, 2017
// See the notice in LICENSE.md

#ifndef HASHTABLE__H
#define HASHTABLE__H

#include "compat.h"
#include "array.h"
#include <new>
#include <iostream>

// Reallocating hashtable class
template<typename K, typename V>
class hashtable
{
    struct KV
    {
        u64 h;
        K k;
        V v;

        KV() : h(0) { }
        KV(const KV& kv) { h = kv.h; k = kv.k; v = kv.v; }
    };

private:
    array<KV> table;
    u64 count;

public:
    // Constructs an empty table
    hashtable(u64 initial_size = 0)
        : table(), count(0)
    {
        for (u32 i = 0; i < initial_size; ++i)
            table.push_back(KV());
    }
    
    // Reallocates space for the table
    void check_reallocate()
    {
        if (table.size() == 0)
        {
            // allocate a starter table of size 4
            // if never before allocated (we might be inserting)
            for (u32 i = 0; i < 5; ++i)
                table.push_back(KV());
        }
        else if (count * 2 <= table.size())
            // less than half full
            return;
        else
        {
            // quadruple its size
            array<KV> old = table;

            table = array<KV>(old.size()*4);
            for (u32 i = 0; i < old.size()*4; ++i)
                table.push_back(KV());

            for (u32 i = 0; i < old.size(); ++i)
                if (old[i].h != 0)
                    insert(old[i].h, old[i].k, old[i].v);
        }
    }

    // Either returns a pointer to the V matching key K (with u64 hash) or null
    V* find(u64 hash, const K& key)
    {
        // Probably klugy but I want to reserve 0 to mean unoccupied
        if (hash == 0) hash = 1;

        if (table.size()==0) return 0;
        
        // Find, starting at hash mod table size
        u64 pos = hash % table.size();
        while (true)
        {
            // Skip until a hash shallowly matches or a zero is found
            while (table[pos].h != 0 && table[pos].h != hash)
                pos = (pos + 1) % table.size();

            // return if deeply matches or is a zero, otherwise continue
            if (table[pos].h == 0)
                return 0;
            else if (table[pos].k == key)
                return &(table[pos].v);
            
            // Scan forward at least one
            pos = (pos+1) % table.size();
        }
    }

    // Insert an element into the hashmap
    bool insert(u64 hash, const K& key, const V& value)
    {
        KV _kv;
        _kv.h = (hash==0) ? 1 : hash; _kv.k = key; _kv.v = value;
        // I want to reserve h == 0 to mean unoccupied
        const KV& kv = _kv;

        // Reallocate if necessary
        // after this call there are guarenteed to be open spaces
        // and > 50% chance of vacancy per index (up to quality of hashing)
        check_reallocate();

        // Insert
        u64 pos = kv.h % table.size();
        while (true)
        {
            // Skip until a hash shallowly matches or a zero is found
            while (table[pos].h != 0 && table[pos].h != kv.h)
                pos = (pos + 1) % table.size();

            // assign if zero, otherwise check deep equality and either assign or continue
            if (table[pos].h == 0)
            {
                ++count;
                table[pos] = kv;
                return true;
            }
            else if (table[pos].k == kv.k)
            {
                table[pos] = kv;
                return false;
            }

            // Scan forward one
            pos = (pos+1) % table.size();
        }
    }

    // Returns the number of key-value pairs
    u64 size() const
    {
        return count;
    }

    class iter //: public iterator
    {
    private:
        const array<KV>& table;
        u32 i;

    public:
        iter(const hashtable<K,V>& ht) : table(ht.table), i(-1) { advance(); }

        iter(const iter& it) : table(it.table), i(it.i) {}

        void advance()
        {
            ++i;
            while (i < table.size() && table[i].h == 0) ++i;
        }

        bool more()
        {
            return i < table.size();
        }

        u64 getH()
        {
            return table[i].h;
        }

        const K& getK()
        {
            return table[i].k;
        }

        const V& getV()
        {
            return table[i].v;
        }
    };

    iter iterator()
    {
        return iter(*this);
    }
};


#endif // HASHTABLE__H


