// Written in the D programming language.

/**
 * BSON spec implementation
 *
 * See_Also:
 *  $(LINK2 http://bsonspec.org/, BSON - Binary JSON)
 *
 * Copyright: Copyright Masahiro Nakagawa 2011-.
 * License:   <a href="http://www.apache.org/licenses/">Apache LICENSE Version 2.0</a>.
 * Authors:   Masahiro Nakagawa
 *
 *            Copyright Masahiro Nakagawa 2011-.
 *    Distributed under the Apache LICENSE Version 2.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *            http://www.apache.org/licenses/)
 */
module mongo.bson;

import core.stdc.string;  // Some operations in Phobos not safe, pure and nothrow, e.g. cmp

import std.conv;
import std.exception;  // assumeUnique
import std.datetime;   // Date, DateTime
import std.typecons;   // Tuple


enum Type : byte
{
    minKey       = -1,    /// Special type which compares lower than all other possible BSON element values
    eod          = 0x00,  /// End Of Document
    numberDouble = 0x01,  /// Floating point
    string       = 0x02,  /// UTF8 string
    embedded     = 0x03,  /// Embedded document
    array        = 0x04,  ///
    binData      = 0x05,  /// Binary data
    undefined    = 0x06,  /// Undefined - Deprecated
    oid          = 0x07,  /// ObjectID
    boolean      = 0x08,  /// Boolean - true or false
    date         = 0x09,  /// UTC datetime
    nil          = 0x0a,  /// Null value
    regex        = 0x0b,  /// Regular expression
    dbPointer    = 0x0c,  /// DBPointer - Deprecated
    code         = 0x0d,  /// JavaScript Code
    symbol       = 0x0e,  ///
    codeWScope   = 0x0f,  /// JavaScript code w/ scope
    int32        = 0x10,  /// 32-bit integer
    timestamp    = 0x11,  ///
    int64        = 0x12,  /// 64-bit integer
    maxKey       = 0x7f   /// Special type which compares higher than all other possible BSON element values
}


enum BinDataSubType : ubyte
{
    generic     = 0x00,  /// Binary / Generic
    func        = 0x01,  ///
    binary      = 0x02,  /// Binary (Old)
    uuid        = 0x03,  ///
    md5         = 0x05,  ///
    userDefined = 0x80   ///
}


struct Document
{
  private:
    ubyte[] data_;


  public:
    @property
    {
        bool empty()
        {
            return data_.length < 5;
        }


        size_t size()
        {
            return 0;
        }
        alias size length;
    }


    string[] getFieldNames()
    {
        return null;
    }


    Element opIndex(in string name)
    {
        return Element();
    }


    bool opEquals(ref const Document other) const
    {
        return true;
    }


    int opCmp(ref const Document other) const
    {
        return 0;
    }
}


/**
 * BSON element representation
 */
struct Element
{
  private:
    /*
     * -----
     * //data image:
     * +-----------------------------------+
     * | [type] | [key] | [val | unused... |
     * +-----------------------------------+
     *          ^ type offset(1)
     *                  ^ keySize
     *                         ^ size
     *                                     ^ data.length
     * -----
     */
    immutable ubyte[] data_;


  public:
    this(immutable ubyte[] data)
    {
        // In this time, Element does not parse a binary data.
        // This is lazy initialization for some efficient.
        data_ = data;
    }


    @property @safe const pure nothrow
    {
        bool isEod()
        {
            return data_.length == 0;
        }


        bool isNumber()
        {
            switch (type) {
            case Type.int32, Type.int64, Type.numberDouble:
                return true;
            default:
                return false;
            }
        }


        bool isSimple()
        {
            switch (type) {
            case Type.int32, Type.int64, Type.numberDouble, Type.string, Type.boolean, Type.date, Type.oid:                
                return true;
            default:
                return false;
            }
        }


        bool isTrue()
        {
            switch (type) {
            case Type.int32:
                return _int32() != 0;
            case Type.int64:
                return _int64() != 0L;
            case Type.numberDouble:
                return _double() != 0.0;
            case Type.boolean:
                return _boolean();
            case Type.eod, Type.nil, Type.undefined:
                return false;
            default:
                return true;
            }
        }


        bool isDocument()
        {
            switch (type) {
            case Type.embedded, Type.array:
                return true;
            default:
                return false;
            }
        }

        // need mayEncapsulate?
    }


    @property @safe const pure nothrow
    {
        Type type() 
        {
            if (isEod)
                return Type.eod;
            return cast(Type)data_[0];
        }


        byte canonicalType()
        {
            Type t = type;
            final switch (t) {
            case Type.minKey, Type.maxKey:
                return t;
            case Type.eod, Type.undefined:
                return 0;
            case Type.nil:
                return 5;
            case Type.numberDouble, Type.int32, Type.int64:
                return 10;
            case Type.string, Type.symbol:
                return 15;
            case Type.embedded:
                return 20;
            case Type.array:
                return 25;
            case Type.binData:
                return 30;
            case Type.oid:
                return 35;
            case Type.boolean:
                return 40;
            case Type.date, Type.timestamp:
                return 45;
            case Type.regex:
                return 50;
            case Type.dbPointer:
                return 55;
            case Type.code:
                return 60;
            case Type.codeWScope:
                return 65;
            }
        }
    }


    @property const pure nothrow
    {
        @trusted
        string key()
        {
            if (isEod)
                return null;

            immutable k = cast(string)data_[1..$];
            return k[0..strlen(k.ptr)];
        }


        @safe
        size_t keySize()
        {
            return key.length;
        }
    }


    @property @safe const pure nothrow
    {
        immutable(ubyte[]) value()
        {
            if (isEod)
                return null;

            return data_[1 + rawKeySize..size];
        }


        size_t valueSize()
        {
            return value.length;
        }
    }


    @property @trusted
    size_t size() const pure nothrow
    {
        size_t s;
        final switch (type) {
        case Type.minKey, Type.maxKey, Type.eod, Type.undefined, Type.nil:
            break;
        case Type.boolean:
            s = 1;
            break;
        case Type.int32:
            s = 4;
            break;
        case Type.numberDouble, Type.int64, Type.date, Type.timestamp:
            s = 8;
            break;
        case Type.oid:
            s = 12;
            break;
        case Type.embedded, Type.codeWScope, Type.array:
            s = bodySize;
            break;
        case Type.string, Type.symbol, Type.code:
            s = bodySize + 4;
            break;
        case Type.binData:
            s = bodySize + 4 + 1;
            break;
        case Type.dbPointer:
            s = bodySize + 4 + 12;
            break;
        case Type.regex:
            auto p1 = cast(immutable(char*))data_[1 + rawKeySize..$].ptr;
            size_t length1 = strlen(p1);
            auto p2 = cast(immutable(char*))data_[1 + rawKeySize + length1 + 1..$].ptr;
            size_t length2 = strlen(p2);
            s = length1 + 1 + length2 + 1;
            break;
        }

        return 1 + rawKeySize + s;
    }
    alias size length;

    // D's primitive type accessor like Variant

    @property const /* pure: check is not pure */
    {
        string get(T)() if (is(T == string))
        {
            check(Type.string);
            return str;
        }


        bool get(T)() if (is(T == bool))
        {
            check(Type.boolean);
            return _boolean();
        }


        int get(T)() if (is(T == int))
        {
            check(Type.int32);
            return _int32();
        }


        long get(T)() if (is(T == long))
        {
            check(Type.int64);
            return _int64();
        }


        double get(T)() if (is(T == double))
        {
            check(Type.numberDouble);
            return _double();
        }


        Date get(T)() if (is(T == Date))
        {
            check(Type.date);
            return cast(Date)SysTime(_int64());
        }


        DateTime get(T)() if (is(T == DateTime))
        {
            check(Type.timestamp);
            return cast(DateTime)SysTime(_int64());
        }


        ObjectId get(T)() if (is(T == ObjectId))
        {
            check(Type.oid);
            return ObjectId(value);
        }


        /**
         * Returns an embedded document.
         */
        Document get(T)() if (is(T == Document))
        {
            check(Type.embedded);
            return Document();
        }
    }


    @property @trusted const pure nothrow
    {
        int as(T)() if (is(T == int))
        {
            switch (type) {
            case Type.int32:
                return _int32();
            case Type.int64:
                return cast(int)_int64();
            case Type.numberDouble:
                return cast(int)_double();
            default:
                return 0;
            }
        }


        long as(T)() if (is(T == long))
        {
            switch (type) {
            case Type.int32:
                return _int32();
            case Type.int64:
                return _int64();
            case Type.numberDouble:
                return cast(long)_double();
            default:
                return 0;
            }
        }


        double as(T)() if (is(T == double))
        {
            switch (type) {
            case Type.int32:
                return cast(double)_int32();
            case Type.int64:
                return cast(double)_int64();
            case Type.numberDouble:
                return _double();
            default:
                return 0;
            }
        }        
    }

    // TODO: Add more BSON specified type accessors, e.g.  binData

    @property @trusted const nothrow
    {
        Tuple!(string, string) regex() pure
        {
            immutable start1  = 1 + rawKeySize;
            immutable pattern = cast(string)data_[start1..$];
            immutable length1 = strlen(pattern.ptr);
            immutable start2  = start1 + length1 + 1;
            immutable flags   = cast(string)data_[start2..$];
            immutable length2 = strlen(flags.ptr);
            return typeof(return)(pattern[start1..start1 + length1],
                                  flags[start2..start2 + length2]);
        }


        string str() pure
        {
            return cast(string)value[4..$ - 1];
        }
        alias str dbPointer;


        Date date()
        {
            return cast(Date)SysTime(_int64());
        }


        DateTime timestamp()
        {
            return cast(DateTime)SysTime(_int64());
        }


        string codeWScope() pure
        {
            return cast(string)value[8..$];
        }


        string codeWScopeData() pure
        {
            immutable code = codeWScope;
            return code[code.length + 1..$];
        }


        immutable(ubyte[]) binData() pure
        {
            return value[5..$];
        }
    }


    @safe
    bool opEquals(ref const Element other) const pure nothrow
    {
        size_t s = size;
        if (s != other.size)
            return false;
        return data_[0..s] == other.data_[0..s];
    }


    @safe
    int opCmp(ref const Element other) const pure nothrow
    {
        int typeDiff = canonicalType - other.canonicalType;
        if (typeDiff < 0)
            return -1;
        else if (typeDiff > 0)
            return 1;
        return compareValue(this, other);
    }


    @safe
    string toString() const
    {
        return toFormatString(true, true);
    }


    @trusted
    string toFormatString(bool includeKey = false, bool full = false) const
    {
        string result;
        if (!isEod)
            result = key ~ ": ";

        final switch (type) {
        case Type.minKey:
            result ~= "MinKey";
            break;
        case Type.maxKey:
            result ~= "MaxKey";
            break;
        case Type.eod:
            result ~= "End of Document";
            break;
        case Type.undefined:
            result ~= "Undefined";
            break;
        case Type.nil:
            result ~= "null";
            break;
        case Type.boolean:
            result ~= to!string(_boolean());
            break;
        case Type.int32:
            result ~= to!string(_int32());
            break;
        case Type.int64: 
            result ~= to!string(_int64());
            break;
        case Type.numberDouble:
            result ~= to!string(_double());
            break;
        case Type.date:
            result ~= "new Date(" ~ date.toString() ~ ")";
            break;
        case Type.timestamp:
            result ~= "Timestamp " ~ timestamp.toString();
            break;
        case Type.oid:
            auto oid = get!ObjectId;
            result ~= "ObjectId(" ~ oid.toString() ~ ")";
            break;
        case Type.embedded:
            //result ~= embedded.toFormatString(false, full);
            break;
        case Type.array:
            //result ~= embedded.toFormatString(true, full);
            break;
        case Type.codeWScope:
            result ~= "codeWScope(" ~ codeWScope ~ ")";
            // TODO: Add codeWScopeObject
            break;
        case Type.string, Type.symbol, Type.code:
            // TODO: Support ... representation with bool = true
            result ~= '"' ~ str ~ '"';
            break;
        case Type.binData:
            result ~= "binData";
            // need content?
            break;
        case Type.dbPointer:
            result ~= "DBRef(" ~ str ~ ")";
            break;
        case Type.regex:
            immutable re = regex;
            result ~= "/" ~ re.field[0] ~ "/" ~ re.field[1];
            break;
        }

        return result;
    }


  private:
    @trusted
    void check(Type t) const /* pure */
    {
        if (t != type) {
            string typeName = to!string(t); // why this to! is not pure?
            string message;
            if (isEod)
                message = "Field not found: expected type = " ~ typeName;
            else
                message = "Wrong type for field: " ~ key ~ " != " ~ typeName;

            throw new BSONException(message);
        }
    }


    @trusted const pure nothrow
    {
        bool _boolean()
        {
            return value[0] == 0 ? false : true;
        }


        int _int32()
        {
            return *cast(int*)(value.ptr);
        }


        long _int64()
        {
            return *cast(long*)(value.ptr);
        }


        double _double()
        {
            return *cast(double*)(value.ptr);
        }
    }


    @property const pure nothrow
    {
        @safe
        size_t rawKeySize()
        {
            return key.length + 1;  // including null character termination
        }

        @trusted
        uint bodySize()
        {
            return *cast(uint*)(data_[1 + rawKeySize..$].ptr);
        }
    }
}


unittest
{
    struct ETest
    {
        ubyte[] data;
        Type    type;
        string  key;
        ubyte[] value;
        bool    isTrue;
        bool    isNumber;
        bool    isSimple;
    }

    Element test(ref const ETest set, string msg)
    {
        auto amsg = "Assertion failure(" ~ msg ~ " type unittest)";
        auto elem = Element(set.data.idup);

        assert(elem.type      == set.type,         amsg);
        assert(elem.key       == set.key,          amsg);
        assert(elem.keySize   == set.key.length,   amsg);
        assert(elem.value     == set.value,        amsg);
        assert(elem.valueSize == set.value.length, amsg);
        assert(elem.isTrue    == set.isTrue,       amsg);
        assert(elem.isNumber  == set.isNumber,     amsg);
        assert(elem.isSimple  == set.isSimple,     amsg);

        return elem;
    }

    { // EOD element
        ubyte[] data = [];
        ETest   set  = ETest(data, Type.eod, null, null, false, false, false);

        assert(test(set, "EOD").isEod);
    }
    { // {"hello": "world"} elemement
        ubyte[] data = [0x02, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x00, 0x06, 0x00, 0x00, 0x00, 0x77, 0x6F, 0x72, 0x6C, 0x64, 0x00, 0x00, 0x1f];
        auto    set  = ETest(data, Type.string, "hello", data[7..$ - 2], true, false, true);
        auto    elem = test(set, "UTF8 string");

        assert(elem.str  == "world");
        assert(elem.size == data.length - 2);  // not including extra space
    }

    immutable size_t keyOffset = 3;

    { // {"k": false} elemement
        ubyte[] data = [0x08, 0x6b, 0x00, 0x00];
        ETest   set  = ETest(data, Type.boolean, "k", data[keyOffset..$], false, false, true);

        assert(!test(set, "Boolean false").get!bool);
    }
    { // {"k": true} elemement
        ubyte[] data = [0x08, 0x6b, 0x00, 0x01];
        ETest   set  = ETest(data, Type.boolean, "k", data[keyOffset..$], true, false, true);

        assert(test(set, "Boolean true").get!bool);
    }
    { // {"k": int.max} elemement
        { // true
            ubyte[] data = [0x10, 0x6b, 0x00, 0xff, 0xff, 0xff, 0x7f];
            ETest   set  = ETest(data, Type.int32, "k", data[keyOffset..$], true, true, true);

            assert(test(set, "32bit integer").get!int == int.max);
        }
        { // false
            ubyte[] data = [0x10, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00];
            ETest   set  = ETest(data, Type.int32, "k", data[keyOffset..$], false, true, true);

            assert(test(set, "32bit integer").get!int == 0);
        }
    }
    { // {"k": long.min} elemement
        { // true
            ubyte[] data = [0x12, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80];
            ETest   set  = ETest(data, Type.int64, "k", data[keyOffset..$], true, true, true);

            assert(test(set, "64bit integer").get!long == long.min);
        }
        { // false
            ubyte[] data = [0x12, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
            ETest   set  = ETest(data, Type.int64, "k", data[keyOffset..$], false, true, true);

            assert(test(set, "64bit integer").get!long == 0);
        }
    }
    { // {"k": 10000.0} elemement
        { // true
            ubyte[] data = [0x01, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0xc3, 0x40];
            ETest   set  = ETest(data, Type.numberDouble, "k", data[keyOffset..$], true, true, true);

            assert(test(set, "Floating point").get!double == 10000.0f);
        }
        { // false
            ubyte[] data = [0x01, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
            ETest   set  = ETest(data, Type.numberDouble, "k", data[keyOffset..$], false, true, true);

            assert(test(set, "Floating point").get!double == 0.0f);
        }
    }
    { // {"k": Date or DateTime(2011/09/26...)} elemement
        immutable time = 1316968892700L;
        {
            ubyte[] data = [0x09, 0x6b, 0x00, 0x1c, 0x89, 0x76, 0xa1, 0x32, 0x01, 0x00, 0x00];
            ETest   set  = ETest(data, Type.date, "k", data[keyOffset..$], true, false, true);

            assert(test(set, "Date").get!Date == cast(Date)SysTime(time));
        }
        {
            ubyte[] data = [0x11, 0x6b, 0x00, 0x1c, 0x89, 0x76, 0xa1, 0x32, 0x01, 0x00, 0x00];
            ETest   set  = ETest(data, Type.timestamp, "k", data[keyOffset..$], true, false, false);

            assert(test(set, "Timestamp").get!DateTime == cast(DateTime)SysTime(time));
        }
    }
    { // {"k": ObjectId(...)} elemement
        ubyte[]  data = [0x07, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0xff];
        ETest    set  = ETest(data, Type.oid, "k", data[keyOffset..$], true, false, true);

        assert(test(set, "ObjectId").get!ObjectId == ObjectId(long.min, uint.max));
    }
    { // No content elemements, null, MinKey, MaxKey
        foreach (i, type; [Type.nil, Type.minKey, Type.maxKey]) {
            ubyte[] data = [type, 0x6b, 0x00];
            ETest   set  = ETest(data, type, "k", data[keyOffset..$], i > 0);

            test(set, to!string(type));
        }
    }

    // TODO: Add other type tests
}


@trusted
int wellOrderedCompare(ref const Element lhs, ref const Element rhs, bool considerKey = true) pure nothrow
{
    int r = lhs.canonicalType - rhs.canonicalType;
    if (r != 0 && (!lhs.isNumber() || !rhs.isNumber()))
        return r;

    if (considerKey) { 
        r = strcmp(lhs.key.ptr, rhs.key.ptr);
        if (r != 0)
            return r;
    }

    return compareValue(lhs, rhs);
}


@trusted
int compareValue(ref const Element lhs, ref const Element rhs) pure nothrow
{
    final switch (lhs.type) {
    case Type.minKey, Type.maxKey, Type.eod, Type.undefined,  Type.nil:
        auto r = lhs.canonicalType - rhs.canonicalType;
        if (r < 0)
            return -1;
        return r == 0 ? 0 : 1;
    case Type.numberDouble:
    Ldouble:
        import std.math;

        double l = lhs.as!double;
        double r = rhs.as!double;

        if (l < r)
            return -1;
        if (l == r)
            return 0;
        if (isNaN(l))
            return isNaN(r) ? 0 : -1;
        return 1;
    case Type.int32:
        if (rhs.type == Type.int32) {
            immutable l = lhs.as!int;
            immutable r = rhs.as!int;

            if (l < r)
                return -1;
            return l == r ? 0 : 1;
        }
        goto Ldouble;
    case Type.int64:
        if (rhs.type == Type.int64) {
            immutable l = lhs.as!long;
            immutable r = rhs.as!long;

            if (l < r)
                return -1;
            return l == r ? 0 : 1;
        }
        goto Ldouble;
    case Type.string, Type.symbol, Type.code:
        import std.algorithm;

        immutable ls = lhs.bodySize;
        immutable rs = rhs.bodySize;
        immutable r  = memcmp(lhs.str.ptr, rhs.str.ptr, min(ls, rs));

        if (r != 0)
            return r;
        if (ls < rs)
            return -1;
        return ls == rs ? 0 : 1;
    case Type.embedded,  Type.array:
        // TODO
        return 0;
    case Type.binData:
        immutable ls = lhs.bodySize;
        immutable rs = rhs.bodySize;

        if ((ls - rs) != 0)
            return ls - rs < 0 ? -1 : 1;
        return memcmp(lhs.value[4..$].ptr, rhs.value[4..$].ptr, ls + 1);  // +1 for subtype
    case Type.oid:
        return memcmp(lhs.value.ptr, rhs.value.ptr, 12);
    case Type.boolean:
        return lhs.value[0] - rhs.value[0];
    case Type.date, Type.timestamp:
        // TODO: Fix for correct comparison
        // Following comparison avoids non-pure function call.
        immutable l = lhs._int64();
        immutable r = rhs._int64();

        if (l < r)
            return -1;
        return l == r ? 0 : 1;
    case Type.regex:
        immutable re1 = lhs.regex;
        immutable re2 = rhs.regex;

        immutable r = strcmp(re1.field[0].ptr, re2.field[0].ptr);
        if (r != 0)
            return r;
        return strcmp(re1.field[1].ptr, re2.field[1].ptr);
    case Type.dbPointer:
        immutable ls = lhs.valueSize;
        immutable rs = rhs.valueSize;

        if ((ls - rs) != 0)
            return ls - rs < 0 ? -1 : 1;
        return memcmp(lhs.str.ptr, rhs.str.ptr, ls);
    case Type.codeWScope:
        auto r = lhs.canonicalType - rhs.canonicalType;
        if (r != 0)
            return r;
        r = strcmp(lhs.codeWScope.ptr, rhs.codeWScope.ptr);
        if (r != 0)
            return r;
        r = strcmp(lhs.codeWScopeData.ptr, rhs.codeWScopeData.ptr);
        if (r != 0)
            return r;        
        return 0;
    }
}


unittest
{
    auto oidElem   = Element(cast(immutable(ubyte[]))[0x07, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0xff]);
    auto strElem   = Element(cast(immutable(ubyte[]))[0x02, 0x6b, 0x00, 0x06, 0x00, 0x00, 0x00, 0x77, 0x6F, 0x72, 0x6C, 0x64, 0x00]);  // world
    auto intElem   = Element(cast(immutable(ubyte[]))[0x10, 0x6b, 0x00, 0xff, 0xff, 0xff, 0x7f]);  // int.max
    auto longElem  = Element(cast(immutable(ubyte[]))[0x12, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);  // 0
    auto trueElem  = Element(cast(immutable(ubyte[]))[0x08, 0x6b, 0x00, 0x01]);
    auto dateElem  = Element(cast(immutable(ubyte[]))[0x09, 0x6b, 0x00, 0x1c, 0x89, 0x76, 0xa1, 0x32, 0x01, 0x00, 0x00]);
    auto someElems = [longElem, strElem, oidElem, trueElem, dateElem];  // canonicalType order

    { // MinKey
        auto minKeyElem = Element(cast(immutable(ubyte[]))[Type.minKey, 0x6b, 0x00]);

        assert(minKeyElem == Element(cast(immutable(ubyte[]))[Type.minKey, 0x6b, 0x00]));
        foreach (ref elem; someElems)
            assert(minKeyElem < elem);

        assert(!(minKeyElem < Element(cast(immutable(ubyte[]))[Type.minKey, 0x6b, 0x00])));
        assert(!(minKeyElem < Element(cast(immutable(ubyte[]))[Type.minKey, 0x6a, 0x00])));  // not consider key
        assert(wellOrderedCompare(minKeyElem, Element(cast(immutable(ubyte[]))[Type.minKey, 0x6c, 0x00])) < 0);
        assert(wellOrderedCompare(minKeyElem, Element(cast(immutable(ubyte[]))[Type.minKey, 0x6c, 0x00]), false) == 0);
    }
    { // str
        foreach (ref elem; someElems[0..1])
            assert(strElem > elem);
        foreach (ref elem; someElems[2..$])
            assert(strElem < elem);

        auto strElem2 = Element(cast(immutable(ubyte[]))[0x02, 0x6b, 0x00, 0x05, 0x00, 0x00, 0x00, 0x62, 0x73, 0x6f, 0x6e, 0x00]);  // bson
        auto strElem3 = Element(cast(immutable(ubyte[]))[0x02, 0x6c, 0x00, 0x05, 0x00, 0x00, 0x00, 0x62, 0x73, 0x6f, 0x6e, 0x00]);  // bson

        assert(strElem > strElem2);
        assert(strElem > strElem3);
        assert(wellOrderedCompare(strElem, strElem3) < 0);
        assert(wellOrderedCompare(strElem, strElem3, false) > 0);
    }
    { // int
        foreach (ref elem; someElems[1..$])
            assert(intElem < elem);

        auto intElem2 = Element(cast(immutable(ubyte[]))[0x10, 0x6c, 0x00, 0x00, 0x00, 0x00, 0x00]);  // 0

        assert(intElem > intElem2);
        assert(intElem > longElem);
        assert(wellOrderedCompare(intElem, intElem2) < 0);
    }
    { // long
        foreach (ref elem; someElems[1..$])
            assert(longElem < elem);

        auto longElem2 = Element(cast(immutable(ubyte[]))[0x12, 0x6a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80]);  // long.min

        assert(intElem  > longElem2);
        assert(longElem > longElem2);
        assert(wellOrderedCompare(longElem, longElem2) > 0);
    }
    { // boolean
        foreach (ref elem; someElems[0..2])
            assert(trueElem > elem);
        foreach (ref elem; someElems[4..$])
            assert(trueElem < elem);

        auto falseElem = Element(cast(immutable(ubyte[]))[0x08, 0x6c, 0x00, 0x00]);

        assert(falseElem < trueElem);
        assert(wellOrderedCompare(falseElem, trueElem) > 0);
        assert(wellOrderedCompare(falseElem, trueElem, false) < 0);
    }
    { // MaxKey
        auto maxKeyElem = Element(cast(immutable(ubyte[]))[Type.maxKey, 0x6b, 0x00]);

        assert(maxKeyElem == Element(cast(immutable(ubyte[]))[Type.maxKey, 0x6b, 0x00]));
        foreach (ref elem; someElems)
            assert(maxKeyElem > elem);

        assert(!(maxKeyElem < Element(cast(immutable(ubyte[]))[Type.maxKey, 0x6b, 0x00])));
        assert(!(maxKeyElem < Element(cast(immutable(ubyte[]))[Type.maxKey, 0x6a, 0x00])));  // not consider key
        assert(wellOrderedCompare(maxKeyElem, Element(cast(immutable(ubyte[]))[Type.maxKey, 0x6c, 0x00])) < 0);
        assert(wellOrderedCompare(maxKeyElem, Element(cast(immutable(ubyte[]))[Type.maxKey, 0x6c, 0x00]), false) == 0);
    }

    // TODO: Add other type tests
}


/**
 * Exception type used by mongo.bson module
 */
class BSONException : Exception
{
    this(string msg)
    {
        super(msg);
    }
}


/**
 * The BSON ObjectId Datatype
 *
 * See_Also: 
 *  $(LINK2 http://www.mongodb.org/display/DOCS/Object+IDs, Object IDs)
 */
struct ObjectId
{
  private:
    // ObjectId is 12 bytes
    union
    {
        ubyte[12] data;

        struct
        {
            long a;
            uint b;
        }

        struct
        {
            ubyte[4] time;
            ubyte[3] machine;
            ushort   pid;
            ubyte[3] inc;
        }
    }


    // ourMachine shoulde be immutable
    // immutable static ubyte[3] ourMachine;
    // See: http://dusers.dip.jp/modules/forum/index.php?topic_id=104#post_id399
    __gshared static ubyte[3] ourMachine;


    @trusted
    shared static this()
    {
        import std.md5;  // TODO: Will be replaced with std.digest
        import std.socket;

        ubyte[16] digest;

        sum(digest, Socket.hostName());
        ourMachine[] = digest[0..3];
    }


    unittest
    {
        ObjectId oid;
        oid.initialize();

        assert(oid.machine == ourMachine);
    }


  public:
    @property
    static uint machineID() nothrow
    {
        static union MachineToID
        {
            ubyte[4] machine;
            uint     id;
        }

        MachineToID temp;
        temp.machine[0..3] = ourMachine;
        return temp.id;
    }


    @safe pure nothrow
    {
        this(in ubyte[] bytes)
        in
        {
            assert(bytes.length == 12, "The length of bytes must be 12");
        }
        body
        {
            data[] = bytes;
        }


        this(long a, uint b)
        {
            this.a = a;
            this.b = b;
        }


        this(in string hex)
        in
        {
            assert(hex.length == 24, "The length of hex string must be 24");
        }
        body
        {
            data[] = fromHex(hex);
        }
    }


    @trusted
    void initialize()
    {
        import std.process;

        { // time
            uint   t = cast(uint)Clock.currTime().toUnixTime();
            ubyte* p = cast(ubyte*)&t;
            time[0]  = p[3];
            time[1]  = p[2];
            time[2]  = p[1];
            time[3]  = p[0];
        }

        // machine
        machine = ourMachine;

        // pid(or thread id)
        pid = cast(ushort)getpid();

        { // inc
            //See: http://d.puremagic.com/issues/show_bug.cgi?id = 6670
            //import core.atomic;
            /* shared */ __gshared static uint counter;
            //atomicOp!"+="(counter, 1u);
            uint   i = counter++;
            ubyte* p = cast(ubyte*)&i;
            inc[0]   = p[2];
            inc[1]   = p[1];
            inc[2]   = p[0];
        }
    }


    @safe
    bool opEquals(ref const ObjectId other) const pure nothrow
    {
        return data == other.data;
    }


    @safe
    string toString() const pure nothrow
    {
        return data.toHex();
    }
}


unittest
{
    { // ==
        string hex = "ffffffffffffff7fffffffff";

        auto oid1 = ObjectId(long.max, uint.max);
        auto oid2 = ObjectId(hex);
        assert(oid1 == oid2);
        assert(oid1.toString() == hex);
        assert(oid2.toString() == hex);

        ObjectId oid;
        oid.initialize();
        assert(oid.machineID > 0);
    }
    { // !=
        auto oid1 = ObjectId(long.max, uint.max);
        auto oid2 = ObjectId(long.max,  int.max);

        assert(oid1 != oid2);
    }
    { // internal data
        ObjectId oid = ObjectId("000102030405060708090a0b");

        assert(oid.data == [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b]);
    }
}


private:


// Phobos does not have 0-filled hex conversion functions?


@trusted
string toHex(in ubyte[] nums) pure nothrow
{
    immutable static lowerHexDigits = "0123456789abcdef";

    char[] result = new char[](nums.length * 2);
    foreach (i, num; nums) {
        immutable index = i * 2;
        result[index]     = lowerHexDigits[(num & 0xf0) >> 4];
        result[index + 1] = lowerHexDigits[num & 0x0f];
    }

    return assumeUnique(result);
}


@safe
ubyte[] fromHex(in string hex) pure nothrow
{
    static ubyte toNum(in char c) pure nothrow
    {
        if ('0' <= c && c <= '9')
            return cast(ubyte)(c - '0');
        if ('a' <= c && c <= 'f')
            return cast(ubyte)(c - 'a' + 10);
        assert(false, "Out of hex: " ~ c);
    }

    ubyte[] result = new ubyte[](hex.length / 2);

    foreach (i, ref num; result) {
        immutable index = i * 2;
        num = cast(ubyte)((toNum(hex[index]) << 4) | toNum(hex[index + 1]));
    }

    return result;
}


unittest
{
    static struct Test
    {
        ubyte[] source;
        string  answer;
    }

    Test[] tests = [
        Test([0x00], "00"), Test([0xff, 0xff], "ffff"),
        Test([0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde], "123456789abcde")
    ];

    foreach (ref test; tests)
        assert(test.source.toHex() == test.answer);
    foreach (ref test; tests)
        assert(fromHex(test.answer) == test.source);
}
