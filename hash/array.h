// Copyright (C) Thomas Gilray, 2015
// See the notice in LICENSE.md


#ifndef ARRAY__H
#define ARRAY__H

#include "compat.h"
#include <new>


// Reallocating array class
template<class T>
class array
{
private:
    T* data;
    u32 max;
    u32 length;

public:
    // Constructs an empty array
    array()
        : data(0), max(0), length(0)
    {
        
    }


    // Constructs an empty array and allocates some memory
    array(u32 allocate)
        : data(0), max(0), length(0)
    {
        reallocate(allocate);
    }


    // Copy Constructor
    array(const array<T>& a)
        : data(0)
    {
        *this = a;
    }


    // Destructor
    ~array()
    {
        clear();
    }


    // Erases all elements
    void clear()
    {
        for (u32 i = 0; i < length; ++i)
            (&data[i])->~T();
        if (data)
            operator delete(data);

        data = 0;
        length = 0;
        max = 0;
    }


    // Reallocates space for the array
    void reallocate(u32 space)
    {
        T* old = data;
        u32 oldlength = length;

        max = space;
        data = (T*) operator new(max * sizeof(T));

        // Construct new elements within the new list
        if (max < length)
        {
            for (u32 i = 0; i < max; ++i)
                new (&data[i]) T(old[i]);
            length = max;
        }
        else
        {
            for (u32 i = 0; i < length; ++i)
                new (&data[i]) T(old[i]);
        }

        // Call destructor on old elements and deallocate the old memory
        for (u32 i = 0; i < oldlength; ++i)
            (&old[i])->~T();

        if (old)
            operator delete(old);
    }


    // Insert an element into the array
    void insert(const T& element, u32 position = 0)
    {
        // Reallocate if necessary
        if (length >= max)
            reallocate(max > 0 ? max * 2 : 1);

        // Create a space for the new element
        for (u32 i = length; i > position; --i)
        {
            new (&data[i]) T(data[i-1]);
            (&data[i-1])->~T();
        }

        // Add the element in this space
        new (&data[position]) T(element);
        ++length;
    }


    // Inserts a new element at the front of the array
    void push_front(const T& element)
    {
        insert(element, 0);
    }


    // Puts a new element at the back of the array
    void push_back(const T& element)
    {
        insert(element, length);
    }


    // Erases elements from the array
    void erase(u32 position, u32 num = 1)
    {
        if (num > 0)
        {
            for (u32 i = position; i < position + num; ++i)
                (&data[i])->~T();

            for (u32 i = position + num; i < length; ++i)
            {
                if (i > position + num)
                    (&data[i-num])->~T();
                new (&data[i-num]) T(data[i]);
                if (i >= length - num)
                    (&data[i])->~T();
            }

            length -= num;
        }
    }


    // Assignment operator
    void operator =(const array<T>& a)
    {
        clear();
        if (a.max > 0)
            reallocate(a.max);

        length = a.length;
        max = a.max;

        for (u32 i = 0; i < length; ++i)
            new (&data[i]) T(a.data[i]);
    }


    // Checks two arrays against each other, returns true if all elements are equal
    bool operator ==(const array<T>& a) const
    {
        if (length != a.size())
            return false;

        for (u32 i = 0; i < length; ++i)
            if (!(data[i] == a[i]))
                return false;

        return true;
    }


    // Checks two arrays against each other, returns true if there is some difference
    bool operator !=(const array<T>& a) const
    {
        return !this->operator ==(a);
    }


    // Index the array and return a reference to an element
    T& operator [](u32 i)
    {
        return data[i];
    }


    // Index the array and return a const reference to an element
    const T& operator [](u32 i) const
    {
        return data[i];
    }


    // Returns the number of elements in the array (the actual length)
    u32 size() const
    {
        return length;
    }
};


#endif // ARRAY__H


