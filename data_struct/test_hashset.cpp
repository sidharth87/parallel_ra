

#include "hashset.h"
#include "compat.h"
#include <cstdlib>
#include <chrono>
#include <unordered_set>
#include <thread>


u64 utime()
{
    return ((u64)std::chrono::high_resolution_clock::now().time_since_epoch().count()) / 1000;
}


class tuple
{
public:
    const u32 x;
    const u32 y;
    const u32 z;

    tuple(u32 x, u32 y, u32 z) : x(x), y(y), z(z)
    {
        
    }

    u64 hash() const
    {
        const u16* data = reinterpret_cast<const u16*>(this);
        u64 h0 = 0xcbf29ce484222325; // fnv-1a
        //u64 h2 = 0; // sdbm
        //u64 h3 = 0; // djb2
        for (u32 i = 0; i < 12/2; ++i && ++data)
        {
            h0 = (h0 ^ *data) * 0x100000001b3;
            //h2 = *data + (h2 << 6) + (h2 << 16) - h2;
            //h3 = h3*33 ^ *data;
        }

        return h0/*^h2^h3*/;// & 0xc08010101101010f; // for intentionally making it terrible
    }
    
    bool operator==(const tuple& t) const
    {
        return t.x == this->x
            && t.y == this->y
            && t.z == this->z;
    }
};


void testround(u32 round = 0)
{   
    const u32 loops = 3000000;
    const u32 offset = round*loops;//1000+(std::rand() % 0x10000000);
    std::cout << "Test round " << round << " (offset=" << offset << "):" << std::endl;
    
    hashset<tuple>* st = new hashset<tuple>();
    //std::unordered_set<tuple> uost = new unordered_set<tuple>();
    
    // Add values
    for (u32 i = offset; i < offset+loops; ++i)
    {
        //std::cout << i-offset << std::endl;
        tuple* const t = new tuple(i,i+1,i*i);
        u64 thash = t->hash();
        const tuple* it = st->add(t,thash>>32,thash&0xffffffff); // you can specify two hashes manually
        if (it != t) {std::cout << "error 1"; exit(1);}
    }

    // add those values again x2
    for (u32 j = 0; j < 2; ++j)
    for (u32 i = offset; i < offset+loops; ++i)
    {
        const tuple* const t = new tuple(i,i+1,i*i);
        u64 thash = t->hash();
        const tuple* it = st->add(t,thash>>32,thash&0xffffffff);
        if (t == it) {
            std::cout << "error 2:" << i << " " << j; exit(2);}
    }
    
    const tuple* const t = new tuple(offset+43, offset+43+1, offset+43*43);
    u64 thash = t->hash();
    hashset<tuple>::bucket_iter it(*st, thash>>32); // creates an iterator for *outer hash* matching tuple x==43
    for (;it.more(); ++it)
        // ->get() accesses the current value
        // The iterator hits every tuple in a bucket, YOU have to descriminate whether each is really a matching value or not
        std::cout << "Found a matching x via iterator: " << it.get()->x << ",\thash: " << (it.get()->hash() >> 32)%15111 << " \n";
    
    //std::cout << "Table count: " << st->size() << std::endl;
}


int main()
{
    // This script leaks like crazy, but it's just a sanity check
    u32 rounds = 5;
    
    std::srand(12345);//utime());

    u64 best = 0xffffffffffffffff;
    u64 sum = 0;
    for (u32 i = 0; i < rounds; ++i)
    {
        u64 start = utime();
        testround(i);
        u64 end = utime();
        if ((end - start) < best)
            best = end - start;
        sum += (end - start);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    
    std::cout << "Best timing: " << ((double)(best/1000)/1000.0) << "sec \t\t";
    std::cout << "Avg. timing: " << ((double)((sum/(rounds))/1000)/1000.0) << "sec" << std::endl << std::endl;

    return 0;
}





