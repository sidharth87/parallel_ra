// Copyright (C) Thomas Gilray, 2017
// See the notice in LICENSE.md


#pragma once

#include <string.h>
#include "compat.h"
#include <new>
#include <iostream>
#include <assert.h>


// Reallocating two-tiered mutable hashset class
template<typename K>
class hashset
{
    class hblock
    {
    public:
        // contains 3*21bit hashes in the lowest 63bits
        u64 hashes;
        const K* keys[3];
        
        hblock() : hashes(0), keys{0,0,0} {}
    };

    // We use the bottom bits ..000 to code the size of the pointer
    // on a 0-7 scale defined just below
    union hblock_ptr
    {
        u64 code;
        hblock* blocks;
        hblock_ptr() : code(0) {}
    };

    inline void reallocate(const u32 h0, hblock* const old, u32 code)
    {
        // reallocate table[h0]
        const u32 oldsize = to_size[code];
        code++;
        if (code >= 8) {
            exit(8);
            /*Hmm, for now we exit with some nonzero value, 
              I guess we want some kind of nicer error here*/
        }
        
        const u32 newsize = to_size[code];
        hblock* const blocks = new hblock[newsize];
        memset(blocks, 0, newsize*sizeof(hblock));
        this->table[h0].blocks = blocks;
        this->table[h0].code |= code;
        
        u32 cc = 0;
        for (u32 bi = 0; bi < oldsize; ++bi)
            for (u32 ii = 0; ii < 3; ++ii)
                if (old[bi].keys[ii])
                {
                    cc++;
                    const u32 h1 = (old[bi].hashes >> (21*ii)) & 0x1fffff;
                    add_internal(old[bi].keys[ii], h0, h1);
                }

        //std::cout << "Reallocating inner table h0=" << h0 << " bsize= " << oldsize << " with ratio ";
        //std::cout << ((double)cc)/(oldsize*3) << std::endl;
        
        delete [] old;
    }

    inline const K* add_internal(const K* key, const u32 h0, const u32 h1)
    {
        // The bottom 3 bits of the pointer are used to code the buffer's size
        u32 code = table[h0].code % 8;

        // Pull out the inner table's pointer (its beginning)
        hblock* const blocks = reinterpret_cast<hblock*>(table[h0].code & 0xfffffffffffffff8);

        // Lookup the size and compute the ideal inner point (the first i in the block h1 mod bsize)
        // There are bsize blocks and bi is the current block index.
        // Any value i is the overall index, so a block bi's initial index is some i==bi*3
        const u32 bsize = to_size[code];
        u32 bi = h1%bsize;
        
        //
        // Basically the while(true) loop below is unrolled for the first two keys
        //
        
        // Check first key at bi==ideal/3 and ii==0
        if (blocks[bi].keys[0] == 0)
        {
            // Not at 3bi+0, we can add and return it
            blocks[bi].keys[0] = key;
            blocks[bi].hashes = ((blocks[bi].hashes & 0xffffffffffe00000) | (u64)h1);
            
            // We consider two adjascent blocks merged if the third position of
            // the preceeding block is filled---keys might overflow.
            // So long as we've inserted before the end of the first block,
            // we can't have merged blocks and introduced longer merged sequences,
            // so we don't check for this until the loop down below, starting at 3bi+2.
            return key;
        }
        
        // Check against the first key with a mask allowing the bottom 21 bits
        // Short-circuit eval of && checks the key deeply iif the hash matches
        const u32 h1ii0 = blocks[bi].hashes & 0x1fffff;
        const K* kii0 = blocks[bi].keys[0];
        if (h1 == h1ii0 && (kii0 == key || *kii0 == *key))
            return kii0;
            
        // Otherwise check second key at 3bi+1
        else if (blocks[bi].keys[1] == 0)
        {   
            // Not at block i key 1, we can add and return it
            blocks[bi].keys[1] = key;
            const u64 clearedhashes = blocks[bi].hashes & (0xffffffffffffffff ^ ((u64)0x1fffff << 21));
            blocks[bi].hashes = clearedhashes | (((u64)h1) << 21);
            
            // Again, so long as we've inserted before the end of the first block,
            // we can't have merged blocks and introduced a longer merged sequence of blocks.
            return key;
        }

        // Check against the second key (exactly as before)
        const u32 h1ii1 = (blocks[bi].hashes >> 21) & 0x1fffff;
        const K* kii1 = blocks[bi].keys[1];
        if (h1 == h1ii1 && (kii1 == key || *kii1 == *key))
            return kii1;
        
        // Now we go into a fallback loop after the initial two keys are exhausted
        // Starting with 3bi+2 we simply scan forward for a zero or a match
        // At each +2 index, if we xmatch, we do a reallocation check because blocks were merged
        u32 i = (bi*3+2), ii = 2, stepcount = 2;
        while (true)
        {
            if (blocks[bi].keys[ii] == 0)
            {
                // Not here, we can add and return it
                blocks[bi].keys[ii] = key;
                const u64 newhash = (u64)h1 << (21*ii);
                const u64 mask = (((u64)0xffffffffffffffff) ^ (((u64)0x1fffff) << (21*ii)));
                blocks[bi].hashes = (blocks[bi].hashes & mask) | newhash;
                
                // If we filled the end of a block, we have merged it with the next    
                if (ii == 2)
                {
                    // So we need to do a check of the total contiguous number of keys (stepcount)
                    // Once it reaches levelup we do a reallocation.
                    const u32 levelup = to_levelup[code];
                    
                    do
                    {
                        ++stepcount;
                        i = (i+1) % (bsize*3);
                        ii = i % 3;
                        bi = i / 3;
                    } while (stepcount < levelup && blocks[bi].keys[ii] != 0);
                    if (stepcount < levelup)
                    {
                        // Keep going and count at the front of ideal too
                        i = ((h1%bsize)*3-1+bsize*3) % (bsize*3);
                        ii = i % 3;
                        bi = i / 3;
                        while (stepcount < levelup && blocks[bi].keys[ii] != 0)
                        {
                            ++stepcount;
                            i = (i-1+bsize*3) % (bsize*3);
                            ii = i % 3;
                            bi = i / 3;
                        }
                    }
                                
                    // When the total length of this dense chunk is too high, resize
                    if (stepcount >= levelup)
                        reallocate(h0, blocks, code);
                }
                 
                return key;
            }
            else
            {
                const u32 h1ii = (blocks[bi].hashes >> (21*ii)) & 0x1fffff;
                const K* kii = blocks[bi].keys[ii];
                if (h1 == h1ii && (kii == key || *kii == *key))
                    return kii;
            }

            ++stepcount;
            i = (i+1) % (bsize*3);
            ii = i % 3;
            bi = i / 3;
        }
    }
    
    inline void repair_unmerged_block(hblock* const blocks, const u32 bsize, const u32 split_bi)
    {
        // 3bi+2 has a garbage value in it, scan forward to pull back the FARTHEST
        // value that should be in bi, or zero it out if none exists.
        // If removing that value unmerges blocks, call tail-recusively for another iteration once done.
        
        // First we scan backward to find out how long this contiguous sequence was in blocks
        u32 bi = split_bi;
        u32 seqlength = 1;
        while (true)
        {
            // Jump back one block
            bi = (bi+bsize-1) % bsize;

            // Check it's final slot and break when it's empty
            if (blocks[bi].keys[2] == 0)
                break;

            ++seqlength;
        }

        // Now we scan forward to find the farthest i in this old sequence that belongs in bi or previous blocks
        bi = (split_bi+1) % bsize;
        u32 i = bi*3, ii = 0;
        s32 lasti = -1;
        while (true)
        {
            if (blocks[bi].keys[ii] == 0)
            {
                if (lasti < 0)
                {
                    // No value needs to be pulled back to block bi
                    // We zero out the key (but may leave junk in .hashes)
                    blocks[split_bi].keys[2] = 0;
                    blocks[split_bi].hashes &= 0x000003ffffffffff; // shouldn't be needed
                    return;
                }
                else
                {
                    // Move the value at lasti back to 3split_bi+2
                    i = lasti; ii = i%3; bi = i/3;
                    blocks[split_bi].keys[2] = blocks[bi].keys[ii];
                    const u64 h = (blocks[bi].hashes >> (21*ii)) & 0x1fffff;
                    blocks[split_bi].hashes = (blocks[split_bi].hashes & 0x000003ffffffffff) | (h << 42);

                    // Pull back any keys just after lasti but within the same block
                    while (ii < 2)
                    {
                        blocks[bi].keys[ii] = blocks[bi].keys[ii+1];
                        const u64 newhash = (blocks[bi].hashes >> (21*ii+21)) & 0x1fffff;
                        const u64 mask = (((u64)0xffffffffffffffff) ^ (((u64)0x1fffff) << (21*ii)));
                        blocks[bi].hashes = (blocks[bi].hashes & mask) | (newhash << (21*ii));
                        ++ii;
                    }

                    // If this just unmerged another block, tail-recur
                    if (blocks[bi].keys[2])
                        repair_unmerged_block(blocks, bsize, bi);

                    return;
                }
            }
            else 
            {
                // Compute the ideal block for the key at index i=3bi+ii
                const u32 idealb = ((blocks[bi].hashes >> (21*ii)) & 0x1fffff) % bsize;
                if ((idealb <= split_bi && idealb > split_bi-seqlength)
                    || (idealb > split_bi && idealb > (split_bi+bsize)-seqlength))
                    // If the ideal block is at split_bi or lower within the seqlength
                    // or ahead by enough to be behind by less than seqlength modulo bsize
                    // Save this index as the farthest one to move back.
                    lasti = i;
            }
            
            i = (i + 1) % (bsize*3);
            bi = i/3;
            ii = i%3;
        }
    }

    inline bool remove_internal(const K* key, const u32 h0, const u32 h1)
    {
        // The bottom 3 bits of the pointer are used to code the buffer's size
        u32 code = table[h0].code % 8;

        // Pull out the inner table's pointer (its beginning)
        hblock* const blocks = reinterpret_cast<hblock*>(table[h0].code & 0xfffffffffffffff8);

        // Lookup the size and compute the ideal inner point (the first i in the block h1 mod bsize)
        // There are bsize blocks and bi is the current block index.
        // Any value i is the overall index, so a block bi's initial index is some i==bi*3
        const u32 bsize = to_size[code];
        u32 bi = h1 % bsize;
        const u32 ideal = bi*3;

        // Scan forward to find key
        u32 i = ideal, ii = 0;
        while (true)
        {
            if (blocks[bi].keys[ii] == 0)
                // Wasn't here to begin with
                return false;
            else        
            {   
                const u32 h1ii = (blocks[bi].hashes >> (21*ii)) & 0x1fffff;
                const K* kii = blocks[bi].keys[ii];
                if (h1 == h1ii && (kii == key || *kii == *key))
                {   
                    // Found the item to remove; remove it and shrink this hblock (pull back the next 0,1, or 2 keys)
                    while (ii < 2)
                    {
                        blocks[bi].keys[ii] = blocks[bi].keys[ii+1];
                        const u64 newhash = (blocks[bi].hashes >> (21*ii+21)) & 0x1fffff;
                        const u64 mask = (((u64)0xffffffffffffffff) ^ (((u64)0x1fffff) << (21*ii)));
                        blocks[bi].hashes = (blocks[bi].hashes & mask) | (newhash << (21*ii));
                        ++ii;
                    } 
                    
                    // The last slot in bi is now unused (but not yet zeroed out; if non-zero it was just removed)
                    // Now, as we've unmerged hblocks, we must check that values aren't in the wrong contiguous sequences
                    if (blocks[bi].keys[2])
                        repair_unmerged_block(blocks, bsize, bi);
                    else
                        blocks[bi].hashes &= 0x000003ffffffffff; /// shouldn't be needed
                        
                    return true;
                }
            }

            i = (i+1) % (bsize*3);
            ii = i % 3;
            bi = i / 3;
        } 
    }
    
private:
    static const u32 buckets = 90909;
    hblock_ptr table[buckets];
    u64 count;
    // The roughly-log_{6-7}() growing schedule of primes used as the 8 sizes
    const u32 to_size[8] = {4,31,191,1151,7753,51151,233777,2*1024*1024};
    // Size of contiguous filled sequence at which to levelup a size; at 3300 it dies
    const u32 to_levelup[8] = {3, 6, 12, 18, 27, 51, 151, 3300};

public:
    // Constructs an empty table with the smallest node at every bucket
    hashset() 
    {
        for (u32 i = 0; i < buckets; ++i)
        {
            table[i].blocks = new hblock[to_size[0]];
            memset(table[i].blocks, 0, to_size[0]*sizeof(hblock));
        }
    }
        
    // Insert an element into the hashset
    // Either determines another k == key exists in the set
    // and returns a pointer to it, or adds key and returns a pointer to it
    const K* add(const K* key)
    {
        const u64 h = key->hash();
        const u32 h0 = (h >> 21) % buckets;
        const u32 h1 = h & 0x1fffff; 

        const K* ret = add_internal(key, h0, h1);
        if (ret == key)
            return key;
        else
        {
            ++count;
            return ret;
        }
    }

    const K* add(const K* key, u64 h)
    {
        const u32 h0 = (h >> 21) % buckets;
        const u32 h1 = h & 0x1fffff; 

        const K* ret = add_internal(key, h0, h1);
        if (ret == key)
            return key;
        else
        {
            ++count;
            return ret;
        }        
    }

    const K* add(const K* key, u32 _h0, u32 _h1)
    {
        const u32 h0 = _h0 % buckets;
        const u32 h1 = _h1 % 0x200000; 

        const K* ret = add_internal(key, h0, h1);
        if (ret == key)
            return key;
        else
        {
            ++count;
            return ret;
        }        
    }

    const void remove(const K* key)
    {
        const u64 h = key->hash();
        const u32 h0 = (h >> 21) % buckets;
        const u32 h1 = h & 0x1fffff; 

        bool b = remove_internal(key, h0, h1);
        if (b) --count;
    }

    const void remove(const K* key, u64 h)
    {
        const u32 h0 = (h >> 21) % buckets;
        const u32 h1 = h & 0x1fffff; 

        bool b = remove_internal(key, h0, h1);
        if (b) --count;
    }
    
    // Returns the number of keys
    u64 size() const
    {
        return count;
    }

    class bucket_iter
    {
    private:
        const hblock* const blocks;
        const u32 bsize;
        u32 i;
        
    public:
        bucket_iter(const hashset<K>& hs, const u32 h0)
            : blocks(reinterpret_cast<hblock*>(hs.table[h0%buckets].code & 0xfffffffffffffff8)),
            bsize(hs.to_size[hs.table[h0%buckets].code&0x7]),
            i(-1)
        {
            operator++();
        }

        inline bucket_iter& operator++()
        {
            // Advance the iterator to be at the next key to return
            ++i;
            while (i/3 < bsize && blocks[i/3].keys[i%3] == 0) ++i;
            
            return *this;
        }

        inline const K* get()
        {
            return blocks[i/3].keys[i%3];
        }

        inline bool more()
        {
            return i/3 < bsize;
        }
    };

    class full_iter
    {
    private:
        u32 bucket;
        const hashset<K>& hs;
        bucket_iter* iit;
        
    public:
        full_iter(const hashset<K>& _hs)
            : bucket(0), hs(_hs), iit(new bucket_iter(hs,bucket))
        {
            while (!iit->more() && ++bucket < buckets)
            {
                delete iit;
                iit = new bucket_iter(hs,bucket);
            }
            if (bucket >= buckets)
            {
                delete iit;
                iit = 0;
            }
        }

        ~full_iter()
        {
            if (iit) delete iit;
        }

        inline bucket_iter& operator++()
        {
            ++(*iit);
            while (!iit->more() && ++bucket < buckets)
            {
                delete iit;
                iit = new bucket_iter(hs,bucket);
            }
            if (bucket >= buckets)
            {
                delete iit;
                iit = 0;
            }
        }

        inline const K* get()
        {
            return iit->get();
        }

        inline bool more()
        {
            return iit;
        }
    };
};


