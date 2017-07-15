// Copyright (C) Thomas Gilray, 2015
// See the notice in license.txt


#ifndef STRING__H
#define STRING__H


#include "compat.h"
#include <stdio.h>


// A mutable string class
template<class T>
class string
{
private:
    T* data;
    u32 max;
    u32 length;

public:
    // Constructor that creates an empty string
    string()
        : data(0), max(1), length(1)
    {
        data = (T*) operator new(sizeof(T));
        data[0] = 0;
    }


    // Copy Constructor
    string(const string<T>& other)
        : data(0), max(0), length(0)
    {
        *this = other;
    }


    // Constructor that converts from a double
    string(const double v)
        : data(0), max(0), length(0)
    {
        c8 str[20];
        snprintf(str, 20, "%.8f", v);
        *this = str;
    }


    // Constructor for any kind of c-style string
    template<class X> string(const X* const str)
        : data(0), max(0), length(0)
    {
        *this = str;
    }


    // Destructor
    ~string()
    {
        if (data)
            operator delete(data);
    }


    // Reallocate space for the string
    void reallocate(u32 space)
    {
        T* old = data;

        max = space;
        data = (T*) operator new(max * sizeof(T));

        if (max < length)
        {
            for (u32 i = 0; i < max; ++i)
                data[i] = old[i];
            data[max-1] = 0;
            length = max;
        }
        else
        {
            for (u32 i = 0; i < length; ++i)
                data[i] = old[i];
            data[length-1] = 0;
        }

        if (old)
            operator delete(old);
    }


    // Assignment operator
    string<T>& operator =(const string<T>& s)
    {
        if (this == &s)
            return *this;

        if (data)
            operator delete(data);

        max = s.size() + 1;
        length = max;

        data = (T*) operator new(length * sizeof(T));
        const T* c_str = s.c_str();
        for (u32 i = 0; i < length; ++i)
            data[i] = c_str[i];

        return *this;
    }


    // Sets a string to an unsigned integer
    string<T>& operator =(u32 n)
    {
        c8 str[12];
        str[11] = 0;

        if (n == 0)
        {
            str[10] = '0';
            *this = &str[10];
        }
        else
        {
            u32 i = 11;
            while(n > 0 && i-- > 0)
            {
                str[i] = static_cast<c8>('0' + (n % 10));
                n /= 10;
            }

            *this = &str[i];
        }
    }


    // Sets a string to an unsigned long integer
    string<T>& operator =(u64 n)
    {
        c8 str[24];
        str[23] = 0;

        if (n == 0)
        {
            str[22] = '0';
            *this = &str[22];
        }
        else
        {
            u32 i = 23;
            while(n > 0 && i-- > 0)
            {
                str[i] = static_cast<c8>('0' + (n % 10));
                n /= 10;
            }

            *this = &str[i];
        }
    }


    // Sets a string to a signed integer
    string<T>& operator =(const s32& n)
    {
        if (n >= 0)
            *this = static_cast<u32>(n);
        else
        {
            *this = static_cast<u32>(-n);
            insert('-',0);
        }
    }


    // Sets a string to a signed long integer
    string<T>& operator =(const s64& n)
    {
        if (n >= 0)
            *this = static_cast<u64>(n);
        else
        {
            *this = static_cast<u64>(-n);
            insert('-', 0);
        }
    }


    // Assignment operator for any kind of string
    template<class X> string<T>& operator =(const X* const s)
    {
        if ((void*)s == (void*)data)
            return *this;

        if (s == 0)
        {
            if (!data)
            {
                data = (T*) operator new(sizeof(T));
                max = 1;
            }

            length = 1;
            data[0] = 0;
            return *this;
        }

        u32 slength = 0;
        while(s[slength])
            ++slength;
        ++slength;

        T* old = data;
        max = slength;
        length = slength;
        data = (T*) operator new(length * sizeof(T));
        for (u32 i = 0; i < length; ++i)
            data[i] = static_cast<T>(s[i]);

        if (old)
            operator delete(old);

        return *this;
    }


    // Addition operator
    string<T> operator +(const string<T>& s) const
    {
        string<T> str(*this);
        str.append(s);
        return str;
    }


    // Addition operator for c-style strings
    string<T> operator +(const T* const s) const
    {
        string<T> str(*this);
        str.append(s);
        return str;
    }


    // Friend Addition operator for T* + string<T>
    template<class X> friend string<T> operator +(const X* const s1, const string<T>& s2)
    {
        return string<T>(s1) + s2;
    }


    // Direct access operator
    T& operator [](const u32 i)
    {
        return data[i];
    }


    // Direct access operator
    const T& operator [](const u32 i) const
    {
        return data[i];
    }


    // Comparison = operator
    bool operator ==(const string<T>& s) const
    {
        for (u32 i = 0; data[i] != 0 && s.data[i] != 0; ++i)
            if (data[i] != s.data[i])
                return false;

        return length == s.length;
    }


    // Comparison = operator for T*
    template<class X> bool operator ==(const X* const s) const
    {
        if (s == 0)
            return false;

        u32 i;
        for(i = 0; data[i] != 0 && s[i] != 0; ++i)
            if (data[i] != s[i])
                return false;

        return data[i] == s[i];
    }


    // Friend Comparison = operator for T* == string<T>
    template<class X> friend bool operator ==(const X* const s1, const string<T>& s2)
    {
        return string<T>(s1) == s2;
    }


    // Not-Equal operator
    bool operator !=(const string<T>& s) const
    {
        return !(*this == s);
    }


    // Not-Equal operator for any kind of c-style string
    template<class X> bool operator !=(const X* const s) const
    {
        return !(*this == s);
    }


    // Friend Not-Equal operator for T* != string<T>
    template<class X> friend bool operator !=(const X* const s1, const string<T>& s2)
    {
        return !(string<T>(s1) == s2);
    }


    // Less-Than operator
    bool operator <(const string<T>& s) const
    {
        for(u32 i = 0; data[i] != 0 && s.data[i] != 0; ++i)
        {
            s32 d = data[i] - s.data[i];
            if (d != 0)
                return d < 0;
        }

        return length < s.length;
    }


    // Greater-Than operator
    bool operator >(const string<T>& s) const
    {
        return s.operator <(*this);
    }


    // Less-Than-Or-Equal operator
    bool operator <=(const string<T>& s) const
    {
        return !(s.operator >(*this));
    }


    // Less-Than-Or-Equal operator
    bool operator >=(const string<T>& s) const
    {
        return !(s.operator <(*this));
    }


    // Returns size of string
    u32 size() const
    {
        return length - 1;
    }


    // Returns character string
    const T* c_str() const
    {
        return data;
    }


    // Makes the string lower case.
    void toLowerCase()
    {
        T d = (static_cast<T>('A') - static_cast<T>('a'));
        for (u32 i = 0; i < length; ++i)
            if (data[i] >= static_cast<T>('A') && data[i] <= static_cast<T>('Z'))
                data[i] -= d;
    }


    // Makes the string upper case.
    void toUpperCase()
    {
        T d = (static_cast<T>('A') - static_cast<T>('a'));
        for (u32 i = 0; i < length; ++i)
            if (data[i] >= static_cast<T>('a') && data[i] <= static_cast<T>('z'))
                data[i] += d;
    }


    // Appends a single character to the end of this string
    void append(T c)
    {
        if (length >= max)
            reallocate(length+(max<4?1:5));

        ++length;
        data[length-2] = c;
        data[length-1] = 0;
    }


    // Appends a c-style character string to the end of this string
    void append(const T* const s)
    {
        if (s == 0)
            return;

        u32 l = 0;
        while(s[l])
            ++l;

        if (length + l > max)
            reallocate(length + l);

        --length;
        ++l;

        for (u32 i = 0; i < l; ++i)
            data[length + i] = s[i];

        length += l;
    }


    // Appends a string to the end of this string
    void append(const string<T>& s)
    {
        u32 slength = s.size();

        if (length + slength > max)
            reallocate(length + slength + (max<4?1:5));

        for (u32 i = 0; i < slength+1; ++i)
            data[length+i-1] = s[i];
        length += slength;
    }


    // Inserts a character at a specific position
    void insert(T c, u32 position)
    {
        if (position >= length)
            position = length - 1;

        if (length >= max)
            reallocate(length+3);

        ++length;
        for (u32 i = length - 1; i > position; --i)
            data[i] = data[i-1];
        data[position] = c;
    }


    // Inserts a string at a specific position
    void insert(const string<T>& s, u32 position)
    {
        if (position >= length)
            position = length - 1;

        u32 slength = s.size();

        if (length + slength > max)
            reallocate(length + slength + 1);

        for (u32 i = length+slength+1; i > position; --i)
            if (i > position + slength)
                data[i-1] = data[i-slength-1];
            else
                data[i-1] = s[i-position-1];
    }


    // Replaces all instances of a certain character to another
    void replace(T change, T to)
    {
        for (u32 i = 0; i < length-1; ++i)
            if (data[i] == change)
                data[i] = to;
    }


    // Replaces all instances of a certain character to another
    void replace(const string<T>& change, const string<T>& to)
    {
        s32 p = -1;
        while ((p = find(change, p+1)) >= 0)
        {
            erase(p, change.size());
            insert(to, p);
        }
    }


    // Trims whitespace from the ends of the string
    void trim()
    {
        string<T> alpha = "\t\n\r ";

        u32 front;
        for (front = 0; front < size(); ++front)
            if (alpha.find(data[front]) < 0)
                break;

        if (front == size())
        {
            *this = "";
        }
        else
        {
            u32 back;
            for (back = size()-1; back > 0; --back)
                if (alpha.find(data[back]) < 0)
                    break;

            *this = substring(front, back-front+1);
        }
    }


    // Erases a section of the string
    void erase(u32 position, u32 l = 1)
    {
        if (position + l >= length)
            l = length - position - 1;
        for (u32 i = position + l; i < length; ++i)
            data[i - l] = data[i];
        length -= l;
    }


    // Finds a character in the string
    s32 find(T c, u32 startposition = 0) const
    {
        for (u32 i = startposition; i < length-1; ++i)
            if (data[i] == c)
                return i;
        return -1;
    }


    // Finds a character in the string, searching backwards
    s32 findLast(T c) const
    {
        for (s32 i = size()-1; i >= 0; --i)
            if (data[i] == c)
                return i;
        return -1;
    }


    // Finds a string within this string
    s32 find(const string<T>& s, u32 startposition = 0) const
    {
        u32 slength = s.size();
        for (u32 i = startposition; i < length-slength; ++i)
            for (u32 ii = i; ii < i + slength; ++ii)
                if (data[ii] != s[ii-i])
                    break;
                else if (ii + 1 == i + slength)
                    return i;
        return -1;
    }


    // Returns the substring of length "l" that starts at "start"
    string<T> substring(u32 position, u32 l) const
    {
        if (position + l >= length)
            l = length - position - 1;

        if (l == 0)
            return string<T>("");

        T* s = new T[l+1];
        for (u32 i = 0; i < l; ++i)
            s[i] = data[position+i];
        s[l] = 0;
        string<T> ss(s);
        delete [] s;
        return ss;
    }


    template<class X> string<T>& operator +=(const X& c)
    {
        append(c);
        return *this;
    }
};


typedef string<c8> cstring;
typedef string<c16> wstring;



#endif // STRING__H


