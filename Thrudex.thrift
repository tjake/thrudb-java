namespace cpp thrudex
namespace java org.thrudb.thrudex
namespace php Thrudex
namespace rb Thrudex
namespace perl Thrudex



exception ThrudexException
{
        1: string what
}

enum FieldType
{
        KEYWORD  = 1,  #Fixed string, not analyzed
        TEXT     = 2,  #Analyzed text, Stored
        UNSTORED = 3   #Analyzed text
}

struct Field
{
        1: string    key,
        2: string    value,

        3: FieldType type       = TEXT,
        4: i32       weight     = 1,
        5: bool      sortable   = 0
}

struct Document
{
        1: string      index
        2: string      key,
        3: list<Field> fields,
        4: string      payload,
        5: i32         weight = 1
}

struct Element
{
        1:string index,
        2:string key,
        3:string payload
}

struct SearchQuery
{
        1: string  index,

        2: string  query,
        3: string  sortby,

        4: i32     limit     = 10,
        5: i32     offset    = 0,

        6: bool    desc      = 0,
        7: bool    randomize = 0,
        8: bool    payload   = 0,
        9: list<string> keyword_fields;   #flag fields that should not be tokenized
}

struct SearchResponse
{
        1: i32              total = -1,   #total across the entire index
        2: list<Element>    elements,
        3: ThrudexException ex
}

service Thrudex
{
        void           ping(),
        list<string>   getIndices(),

        void           put   (1:Document d)       throws(1:ThrudexException ex),
        void           remove(1:Element  e)       throws(1:ThrudexException ex),
        SearchResponse search(1:SearchQuery s)    throws(1:ThrudexException ex),

        list<ThrudexException>  putList   (1:list<Document> documents) throws(1:ThrudexException ex),
        list<ThrudexException>  removeList(1:list<Element>  elements)  throws(1:ThrudexException ex),
        list<SearchResponse>    searchList(1:list<SearchQuery>     q)  throws(1:ThrudexException ex)


        # the following is protected api, it us only to be used by administrative
        # programs and people who really know what they're doing.
        string         admin(1:string op, 2:string data)          throws(1:ThrudexException ex)
}
