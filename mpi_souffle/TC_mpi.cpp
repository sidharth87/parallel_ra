#include "souffle/CompiledSouffle.h"
#include <mpi.h>
namespace souffle {
using namespace ram;
class Sf_TC : public SouffleProgram {
private:
    static inline bool regex_wrapper(const char *pattern, const char *text) {
        bool result = false;
        try { result = std::regex_match(text, std::regex(pattern)); } catch(...) {
            std::cerr << "warning: wrong pattern provided for match(\"" << pattern << "\",\"" << text << "\")\n";
        }
        return result;
    }
    static inline std::string substr_wrapper(const char *str, size_t idx, size_t len) {
        std::string sub_str, result;
        try { result = std::string(str).substr(idx,len); } catch(...) {
            std::cerr << "warning: wrong index position provided by substr(\"";
            std::cerr << str << "\"," << idx << "," << len << ") functor.\n";
        } return result;
    }
public:
    SymbolTable symTable;
    // -- Table: link
    ram::Relation<Auto,2, ram::index<0>>* rel_1_link;
    souffle::RelationWrapper<0,ram::Relation<Auto,2, ram::index<0>>,Tuple<RamDomain,2>,2,true,false> wrapper_rel_1_link;
    // -- Table: reach
    ram::Relation<Auto,2, ram::index<0,1>>* rel_2_reach;
    souffle::RelationWrapper<1,ram::Relation<Auto,2, ram::index<0,1>>,Tuple<RamDomain,2>,2,false,true> wrapper_rel_2_reach;
    // -- Table: @delta_reach
    ram::Relation<Auto,2>* rel_3_delta_reach;
    // -- Table: @new_reach
    ram::Relation<Auto,2>* rel_4_new_reach;
public:
    Sf_TC() :
        rel_1_link(new ram::Relation<Auto,2, ram::index<0>>()),
        wrapper_rel_1_link(*rel_1_link,symTable,"link",std::array<const char *,2>{"s:symbol","s:symbol"},std::array<const char *,2>{"x","y"}),
        rel_2_reach(new ram::Relation<Auto,2, ram::index<0,1>>()),
        wrapper_rel_2_reach(*rel_2_reach,symTable,"reach",std::array<const char *,2>{"s:symbol","s:symbol"},std::array<const char *,2>{"x","y"}),
        rel_3_delta_reach(new ram::Relation<Auto,2>()),
        rel_4_new_reach(new ram::Relation<Auto,2>()){
        addRelation("link",&wrapper_rel_1_link,1,0);
        addRelation("reach",&wrapper_rel_2_reach,0,1);
    }
    ~Sf_TC() {
        delete rel_1_link;
        delete rel_2_reach;
        delete rel_3_delta_reach;
        delete rel_4_new_reach;
    }
    void run() {
        // -- initialize counter --
        std::atomic<RamDomain> ctr(0);

#if defined(__EMBEDDED_SOUFFLE__) && defined(_OPENMP)
        omp_set_num_threads(1);
#endif

        // -- query evaluation --
        SignalHandler::instance()->setMsg(R"_(reach(x,y) :-
                                          link(x,y).
                                          in file /home/sidharth/souffle/TC/TC.souffle [6:1-6:25])_");
                                                                                                  if (!rel_1_link->empty()) {
            auto part = rel_1_link->partition();
            PARALLEL_START;
            CREATE_OP_CONTEXT(rel_1_link_op_ctxt,rel_1_link->createContext());
            CREATE_OP_CONTEXT(rel_2_reach_op_ctxt,rel_2_reach->createContext());
            pfor(auto it = part.begin(); it<part.end(); ++it)
                    try{for(const auto& env0 : *it) {
                    Tuple<RamDomain,2> tuple({(RamDomain)(env0[0]),(RamDomain)(env0[1])});
                    rel_2_reach->insert(tuple,READ_OP_CONTEXT(rel_2_reach_op_ctxt));
                }
                       } catch(std::exception &e) { SignalHandler::instance()->error(e.what());}
            PARALLEL_END;
        }
        rel_3_delta_reach->insertAll(*rel_2_reach);
        for(;;) {
            SignalHandler::instance()->setMsg(R"_(reach(x,y) :-
                                              reach(x,z),
                                              link(z,y).
                                              in file /home/sidharth/souffle/TC/TC.souffle [7:1-7:37])_");
                                                                                                      if (!rel_3_delta_reach->empty()&&!rel_1_link->empty()) {
                auto part = rel_3_delta_reach->partition();
                PARALLEL_START;
                CREATE_OP_CONTEXT(rel_3_delta_reach_op_ctxt,rel_3_delta_reach->createContext());
                CREATE_OP_CONTEXT(rel_4_new_reach_op_ctxt,rel_4_new_reach->createContext());
                CREATE_OP_CONTEXT(rel_1_link_op_ctxt,rel_1_link->createContext());
                CREATE_OP_CONTEXT(rel_2_reach_op_ctxt,rel_2_reach->createContext());
                pfor(auto it = part.begin(); it<part.end(); ++it)
                        try{for(const auto& env0 : *it) {
                        const Tuple<RamDomain,2> key({env0[1],0});
                        auto range = rel_1_link->equalRange<0>(key,READ_OP_CONTEXT(rel_1_link_op_ctxt));
                        for(const auto& env1 : range) {
                            if( !rel_2_reach->contains(Tuple<RamDomain,2>({env0[0],env1[1]}),READ_OP_CONTEXT(rel_2_reach_op_ctxt))) {
                                Tuple<RamDomain,2> tuple({(RamDomain)(env0[0]),(RamDomain)(env1[1])});
                                rel_4_new_reach->insert(tuple,READ_OP_CONTEXT(rel_4_new_reach_op_ctxt));
                            }
                        }
                    }
                           } catch(std::exception &e) { SignalHandler::instance()->error(e.what());}
                PARALLEL_END;
            }
            if(rel_4_new_reach->empty()) break;
            rel_2_reach->insertAll(*rel_4_new_reach);
            {
                auto rel_0 = rel_3_delta_reach;
                rel_3_delta_reach = rel_4_new_reach;
                rel_4_new_reach = rel_0;
            }
            rel_4_new_reach->purge();
        }
        rel_3_delta_reach->purge();
        rel_4_new_reach->purge();
    }
public:
    void printAll(std::string dirname) {
        try {std::map<std::string, std::string> directiveMap({{"IO","file"},{"filename","./reach.csv"},{"name","reach"}});
            if (!dirname.empty() && directiveMap["IO"] == "file" && directiveMap["filename"].front() != '/') {directiveMap["filename"] = dirname + "/" + directiveMap["filename"];}
            IODirectives ioDirectives(directiveMap);
            IOSystem::getInstance().getWriter(SymbolMask({1, 1}), symTable, ioDirectives)->writeAll(*rel_2_reach);
        } catch (std::exception& e) {std::cerr << e.what();exit(1);}
    }
public:
    void loadAll(std::string dirname) {
        try {
            //std::cout << "dirname: " << dirname << "\n";
            std::map<std::string, std::string> directiveMap({{"IO","file"},{"filename","./link.facts"},{"name","link"}});
            if (!dirname.empty() && directiveMap["IO"] == "file" && directiveMap["filename"].front() != '/') {directiveMap["filename"] = dirname + "/" + directiveMap["filename"];}
            IODirectives ioDirectives(directiveMap);
            IOSystem::getInstance().getReader(SymbolMask({1, 1}), symTable, ioDirectives)->readAll(*rel_1_link);
        } catch (std::exception& e) {std::cerr << e.what();exit(1);}
    }
public:
    void dumpInputs(std::ostream& out = std::cout) {
        try {IODirectives ioDirectives;
            ioDirectives.setIOType("stdout");
            ioDirectives.setRelationName("rel_1_link");
            IOSystem::getInstance().getWriter(SymbolMask({1, 1}), symTable, ioDirectives)->writeAll(*rel_1_link);
        } catch (std::exception& e) {std::cerr << e.what();exit(1);}
    }
public:
    void dumpOutputs(std::ostream& out = std::cout) {
        try {IODirectives ioDirectives;
            ioDirectives.setIOType("stdout");
            ioDirectives.setRelationName("rel_2_reach");
            IOSystem::getInstance().getWriter(SymbolMask({1, 1}), symTable, ioDirectives)->writeAll(*rel_2_reach);
        } catch (std::exception& e) {std::cerr << e.what();exit(1);}
    }
public:
    const SymbolTable &getSymbolTable() const {
        return symTable;
    }
};
SouffleProgram *newInstance_TC(){return new Sf_TC;}
SymbolTable *getST_TC(SouffleProgram *p){return &reinterpret_cast<Sf_TC*>(p)->symTable;}
#ifdef __EMBEDDED_SOUFFLE__
class factory_Sf_TC: public souffle::ProgramFactory {
    SouffleProgram *newInstance() {
        return new Sf_TC();
    };
public:
    factory_Sf_TC() : ProgramFactory("TC"){}
};
static factory_Sf_TC __factory_Sf_TC_instance;
}
#else
}
int main(int argc, char** argv)
{
    int nprocs, rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    try{
        souffle::CmdOptions opt(R"(TC.souffle)",
                                R"(.)",
                                R"(.)",
                                false,
                                R"()",
                                1
                                );
        if (!opt.parse(argc,argv)) return 1;
#if defined(_OPENMP) 
        omp_set_nested(true);
#endif
        souffle::Sf_TC obj;
        obj.loadAll(opt.getInputFileDir());
        obj.run();
        obj.printAll(opt.getOutputFileDir());
        return 0;
    } catch(std::exception &e) { souffle::SignalHandler::instance()->error(e.what());}
    MPI_Finalize();
}
#endif
