namespace cpp thrudoc
namespace php Thrudoc
namespace perl Thrudoc
namespace java org.thrudb.thrudoc
namespace ruby Thrudoc


exception ThrudocException{}
exception InvalidKeyException{}
exception InvalidBucketException{}
exception InvalidParametersException{}


service Thrudoc
{
    set<string> getBuckets()                                         throws(1:ThrudocException ex1),

    i32          incr(1:string bucket, 2:string key, 3:i32 amount=1) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),
    i32          decr(1:string bucket, 2:string key, 3:i32 amount=1) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),

    #map operations
    void         put(1:string bucket, 2:string key, 3:binary value)   throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),
    binary       get(1:string bucket, 2:string key)                   throws(1:ThrudocException ex1, 2:InvalidBucketException ex2, 3:InvalidKeyException ex3),
    void         remove(1:string bucket, 2:string key)                throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),

    #list operations
    void         push_front(1:string bucket, 2:string key, 3:binary value) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),
    void         push_back(1:string bucket, 2:string key,  3:binary value) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),

    binary       pop_front(1:string bucket, 2:string key) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),
    binary       pop_back(1:string bucket, 2:string key) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),

    binary       remove_at(1:string bucket, 2:string key, 3:i32 pos) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),
    void         insert_at(1:string bucket, 2:string key,  3:binary value, 4:i32 pos) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),
    void         replace_at(1:string bucket, 2:string key,  3:binary value, 4:i32 pos) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),
    binary       retrieve_at(1:string bucket, 2:string key, 4:i32 pos) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),

    list<binary> range(1:string bucket, 2:string key, 3:i32 start, 4:i32 end) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),
    i32          length(1:string bucket, 2:string key) throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),


    # scan can be used to walk over all of the entities in a workspace in an
    # undefined order. it is also only guaranteed to pick up the entities that
    # exist at the time of the first call to scan. new entities _may_ be picked
    # up. a return of entities less than count means you've hit the end, this
    # includes 0 entities
    list<string> scan(1:string bucket, 2:string seed, 3:i32 limit)    throws(1:ThrudocException ex1, 2:InvalidBucketException ex2),


    # the following is protected api, it us only to be used by administrative
    # programs and people who really know what they're doing.
    string admin(1:string op, 2:string data)                          throws(1:ThrudocException e)
}
