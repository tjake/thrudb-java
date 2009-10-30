namespace cpp thrudex
namespace java org.thrudb.thrudex
namespace php Thrudex
namespace rb Thrudex
namespace perl Thrudex



exception ThrudexException
{
        1: string what
}

enum Analyzer
{
        STANDARD = 1,  # org.apache.lucene.analysis.standard.StandardAnalyzer
        KEYWORD = 2,   # org.apache.lucene.analysis.KeywordAnalyzer
        SIMPLE = 3,    # org.apache.lucene.analysis.SimpleAnalyzer
        STOP = 4,      # org.apache.lucene.analysis.StopAnalyzer
        WHITESPACE = 5 # org.apache.lucene.analysis.WhitespaceAnalyzer
}

struct Field
{
        1: string    key,
        2: string    value,

        3: bool      store      = 1,
        4: i32       weight     = 1,
        5: bool      sortable   = 0,
        6: Analyzer  analyzer = STANDARD
}

struct Document
{
        1: string      index
        2: string      key,
        3: optional list<Field> fields,
        4: optional string      payload,
        5: optional i32         weight = 1
}

struct Element
{
        1:string index,
        2:string key,
        3:optional string payload
}

struct SearchQuery
{
        1: string                 index,

        2: string                 query,
        3: string                 sortby,

        4: i32                    limit     = 10,
        5: i32                    offset    = 0,

        6: bool                   desc      = 0,
        7: bool                   randomize = 0,
        8: bool                   payload   = 0,
        9: Analyzer               defaultAnalyzer = STANDARD,
        10: map<string, Analyzer> fieldAnalyzers = {}
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

        string         getPayload(1:Element  e)   throws(1:ThrudexException ex),
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
