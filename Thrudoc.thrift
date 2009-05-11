namespace cpp thrudoc
namespace php Thrudoc
namespace perl Thrudoc
namespace java org.thrudb.thrudoc
namespace ruby Thrudoc

enum ExceptionType {
     UNKNOWN     = 1,
     NO_SUCH_KEY = 2
}

exception ThrudocException
{
    1: string        what
    2: ExceptionType type = UNKNOWN
}

struct Entity
{
    1: string bucket,
    2: string key,
    3: optional binary value
}

struct ScanResponse
{
    1: list<Entity>  entities,
    2: string        seed
}

struct ListResponse
{
    1: Entity           entity,
    2: ThrudocException ex
}

service Thrudoc
{
    list<string> getBuckets()                                        throws(ThrudocException e),

        i32          incr(1:string bucket, 2:string key, 3:i32 amount=1);
        i32          decr(1:string bucket, 2:string key, 3:i32 amount=1);

        #map operations
    void         put(1:string bucket, 2:string key, 3:binary value)   throws(ThrudocException e),
    string       get(1:string bucket, 2:string key)                   throws(ThrudocException e),
    void         remove(1:string bucket, 2:string key)                throws(ThrudocException e),

        #list operations
        void         push_front(1:string bucket, 2:string key, 3:binary value) throws(ThrudocException e),
    void                 push_back(1:string bucket, 2:string key,  3:binary value) throws(ThrudocException e),
    void         insert_at(1:string bucket, 2:string key,  3:binary value) throws(ThrudocException e),
    binary       pop_front(1:string bucket, 2:string key),
    binary       pop_back(1:string bucket, 2:string key),
    binary       remove_at(1:string bucket, 2:string key, 3:i32 pos),
    ListResponse range(1:string bucket, 2:string key, 3:i32 start, 4:i32 end),
    i32          length(1:string bucket, 2:string key),


    # scan can be used to walk over all of the entities in a workspace in an
    # undefined order. it is also only guaranteed to pick up the entities that
    # exist at the time of the first call to scan. new entities _may_ be picked
    # up. a return of entities less than count means you've hit the end, this
    # includes 0 entities
    ScanResponse scan(1:string bucket, 2:string seed, 3:i32 count)    throws(ThrudocException e),


    # the following is protected api, it us only to be used by administrative
    # programs and people who really know what they're doing.
    string admin(1:string op, 2:string data)                          throws(ThrudocException e)
}
