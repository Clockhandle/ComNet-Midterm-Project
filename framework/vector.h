#pragma once
#include <iostream>
#include <cmath>
// template<typename T>
// class Vector2
// {
// private:
//     T x, y;
// public:
//     Vector2(T x, T y) : x(x), y(y) {};
// };

template<typename T>
class Vec3
{
private:
    T x, y, z;
public:
    Vec3(T x = 0, T y = 0, T z = 0) : x(x), y(y), z(z) {};

// =========== OPERATOR OVERLOADING =============
    Vec3 operator+(const Vec3& rhs) const
    {
        return Vec3(x + rhs.x, y + rhs.y, z + rhs.z);
    }

    Vec3 operator-(const Vec3& rhs) const
    {
        return Vec3(x - rhs.x, y - rhs.y, z - rhs.z);
    }

    Vec3 operator*(T scalar) const 
    {
        return Vec3(x * scalar, y * scalar, z * scalar);
    }

    friend Vec3 operator*(T scalar, const Vec3& vec)
    {
        return vec * scalar;
    }

    friend std::ostream& operator<<(std::ostream& os, const Vec3& vec) {
        return os << "(" << vec.x << ", " << vec.y << ", " << vec.z << ")";
    }

// ================ MEMBER FUNCTIONS =================

    double sqrDistanceTo(const Vec3& dest) const
    {
        Vec3 result = *this - dest;
        return double(result.x * result.x + result.y * result.y + result.z * result.z);
    }

    double distanceTo(const Vec3& dest)
    {
        return std::sqrt(sqrDistanceTo(dest));
    }
    void print()
    {
        std::cout << "(" << x << ", " << y << ", " << z << ")\n"; 
    }
};