
// Simple tests and examples for hashtable.h


#include "hashtable.h"
#include "twohashtable.h"
#include <iostream>


u64 hash_outer(u64 val)
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


u64 hash_inner(u64 val)
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


int main()
{
    hashtable<u64, u64> hash;

    for (u64 i = 1; i < 5000000; ++i)
    {
	// Add a bunch of integers, none should be a duplicate
	if (hash.insert(hash_outer(i), i, hash_outer(i)%1000) == false)
	    return 1;
    }

    for (u64 i = 1; i < hash.size()/10; ++i)
    {
	// Add a bunch of integers, all should be a duplicate
	if (!hash.insert(hash_outer(i), i, hash_outer(i)%1000) == false)
	    return 1;
    }
    
    for (u64 i = 0; i < 5000101; ++i)
    {
        // Do a bunch of lookups
	u64* b = hash.find(hash_outer(i), i);
	if (b) /* b points to a key */;
	else /* no element with this hash and key exists */;
    }

    for (hashtable<u64,u64>::iter it = hash.iterator(); it.more(); it.advance())
    {
	if (it.getV()%989999==99999)
	    std::cout << it.getK() << " --> " << it.getV() << std::endl;
    }

    std::cout << std::endl;
    
    return 0;
}





