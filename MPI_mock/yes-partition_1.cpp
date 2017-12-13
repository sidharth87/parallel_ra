#include "souffle/CompiledSouffle.h"

namespace souffle {
using namespace ram;
class Sf_yes_partition_1 : public SouffleProgram {
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
            std::cerr << str << "\"," << (int32_t)idx << "," << (int32_t)len << ") functor.\n";
        } return result;
    }
public:
    SymbolTable symTable;
    // -- Table: A
    ram::Relation<Auto,1>* rel_1_A;
    souffle::RelationWrapper<0,ram::Relation<Auto,1>,Tuple<RamDomain,1>,1,true,false> wrapper_rel_1_A;
    // -- Table: B1
    ram::Relation<Auto,1>* rel_2_B1;
public:
    Sf_yes_partition_1() : rel_1_A(new ram::Relation<Auto,1>()),
        wrapper_rel_1_A(*rel_1_A,symTable,"A",std::array<const char *,1>{"i:number"},std::array<const char *,1>{"x"}),
        rel_2_B1(new ram::Relation<Auto,1>()){
        addRelation("A",&wrapper_rel_1_A,1,0);
    }
    ~Sf_yes_partition_1() {
        delete rel_1_A;
        delete rel_2_B1;
    }
private:
    template <bool performIO> void runFunction(std::string inputDirectory = ".", std::string outputDirectory = ".") {
        SignalHandler::instance()->set();
        // -- initialize counter --
        std::atomic<RamDomain> ctr(0);

#if defined(__EMBEDDED_SOUFFLE__) && defined(_OPENMP)
        omp_set_num_threads(1);
#endif

        // -- query evaluation --
        /* BEGIN visitSequence @RamSynthesiser.cpp:478 */
        /* BEGIN visitCreate @RamSynthesiser.cpp:276 */
        /* END visitCreate @RamSynthesiser.cpp:277 */
        /* BEGIN visitLoad @RamSynthesiser.cpp:288 */
        if (performIO) {
            try {std::map<std::string, std::string> directiveMap({{"IO","file"},{"delimiter","\t"},{"filename","./A.facts"},{"intermediate","true"},{"name","A"}});
                if (!inputDirectory.empty() && directiveMap["IO"] == "file" && directiveMap["filename"].front() != '/') {directiveMap["filename"] = inputDirectory + "/" + directiveMap["filename"];}
                IODirectives ioDirectives(directiveMap);
                IOSystem::getInstance().getReader(SymbolMask({0}), symTable, ioDirectives, 0)->readAll(*rel_1_A);
            } catch (std::exception& e) {std::cerr << e.what();exit(1);}
        }
        /* END visitLoad @RamSynthesiser.cpp:307 */
        /* BEGIN visitCreate @RamSynthesiser.cpp:276 */
        /* END visitCreate @RamSynthesiser.cpp:277 */
        /* BEGIN visitDebugInfo @RamSynthesiser.cpp:562 */
        SignalHandler::instance()->setMsg(R"_(B1(x) :-
                                          A(x),
                                          x < 50.
                                          in file /home/sidharth/souffle/example/yes-partition.dl [13:1-13:23])_");
                                                                                                               /* BEGIN visitInsert @RamSynthesiser.cpp:333 */
                                                                                                               if (!rel_1_A->empty()) {
            auto part = rel_1_A->partition();
            PARALLEL_START;
            CREATE_OP_CONTEXT(rel_1_A_op_ctxt,rel_1_A->createContext());
            CREATE_OP_CONTEXT(rel_2_B1_op_ctxt,rel_2_B1->createContext());
            /* BEGIN visitScan @RamSynthesiser.cpp:589 */
            pfor(auto it = part.begin(); it<part.end(); ++it)
                    try{for(const auto& env0 : *it) {
                    /* BEGIN visitSearch @RamSynthesiser.cpp:575 */
                    if( /* BEGIN visitBinaryRelation @RamSynthesiser.cpp:896 */
                            ((/* BEGIN visitElementAccess @RamSynthesiser.cpp:1006 */
                              env0[0]/* END visitElementAccess @RamSynthesiser.cpp:1008 */
                              ) < (/* BEGIN visitNumber @RamSynthesiser.cpp:1000 */
                                   50/* END visitNumber @RamSynthesiser.cpp:1002 */
                                   ))/* END visitBinaryRelation @RamSynthesiser.cpp:955 */
                            ) {
                        /* BEGIN visitProject @RamSynthesiser.cpp:829 */
                        Tuple<RamDomain,1> tuple({(RamDomain)(/* BEGIN visitElementAccess @RamSynthesiser.cpp:1006 */
                                                  env0[0]/* END visitElementAccess @RamSynthesiser.cpp:1008 */
                                                  )});
                        rel_2_B1->insert(tuple,READ_OP_CONTEXT(rel_2_B1_op_ctxt));
                        /* END visitProject @RamSynthesiser.cpp:884 */
                    }
                    /* END visitSearch @RamSynthesiser.cpp:585 */
                }
                       } catch(std::exception &e) { SignalHandler::instance()->error(e.what());}
            PARALLEL_END;
        }
        /* END visitInsert @RamSynthesiser.cpp:424 */
        /* END visitDebugInfo @RamSynthesiser.cpp:569 */
        /* BEGIN visitDrop @RamSynthesiser.cpp:443 */
        if (!isHintsProfilingEnabled() && (performIO || 0)) rel_1_A->purge();
        /* END visitDrop @RamSynthesiser.cpp:447 */
        /* BEGIN visitStore @RamSynthesiser.cpp:311 */
        if (performIO) {
            try {std::map<std::string, std::string> directiveMap({{"IO","file"},{"attributeNames","x"},{"filename","./B1.facts"},{"name","B1"}});
                if (!outputDirectory.empty() && directiveMap["IO"] == "file" && directiveMap["filename"].front() != '/') {directiveMap["filename"] = outputDirectory + "/" + directiveMap["filename"];}
                IODirectives ioDirectives(directiveMap);
                IOSystem::getInstance().getWriter(SymbolMask({0}), symTable, ioDirectives, 0)->writeAll(*rel_2_B1);
            } catch (std::exception& e) {std::cerr << e.what();exit(1);}
        }
        /* END visitStore @RamSynthesiser.cpp:329 */
        /* BEGIN visitDrop @RamSynthesiser.cpp:443 */
        if (!isHintsProfilingEnabled() && (performIO || 0)) rel_2_B1->purge();
        /* END visitDrop @RamSynthesiser.cpp:447 */
        /* END visitSequence @RamSynthesiser.cpp:482 */

        // -- relation hint statistics --
        if(isHintsProfilingEnabled()) {
            std::cout << " -- Operation Hint Statistics --\n";
            std::cout << "Relation rel_1_A:\n";
            rel_1_A->printHintStatistics(std::cout,"  ");
            std::cout << "\n";
            std::cout << "Relation rel_2_B1:\n";
            rel_2_B1->printHintStatistics(std::cout,"  ");
            std::cout << "\n";
        }
        SignalHandler::instance()->reset();
    }
public:
    void run() { runFunction<false>(); }
public:
    void runAll(std::string inputDirectory = ".", std::string outputDirectory = ".") { runFunction<true>(inputDirectory, outputDirectory); }
public:
    void printAll(std::string outputDirectory = ".") {
        try {std::map<std::string, std::string> directiveMap({{"IO","file"},{"attributeNames","x"},{"filename","./B1.facts"},{"name","B1"}});
            if (!outputDirectory.empty() && directiveMap["IO"] == "file" && directiveMap["filename"].front() != '/') {directiveMap["filename"] = outputDirectory + "/" + directiveMap["filename"];}
            IODirectives ioDirectives(directiveMap);
            IOSystem::getInstance().getWriter(SymbolMask({0}), symTable, ioDirectives, 0)->writeAll(*rel_2_B1);
        } catch (std::exception& e) {std::cerr << e.what();exit(1);}
    }
public:
    void loadAll(std::string inputDirectory = ".") {
        try {std::map<std::string, std::string> directiveMap({{"IO","file"},{"delimiter","\t"},{"filename","./A.facts"},{"intermediate","true"},{"name","A"}});
            if (!inputDirectory.empty() && directiveMap["IO"] == "file" && directiveMap["filename"].front() != '/') {directiveMap["filename"] = inputDirectory + "/" + directiveMap["filename"];}
            IODirectives ioDirectives(directiveMap);
            IOSystem::getInstance().getReader(SymbolMask({0}), symTable, ioDirectives, 0)->readAll(*rel_1_A);
        } catch (std::exception& e) {std::cerr << e.what();exit(1);}
    }
public:
    void dumpInputs(std::ostream& out = std::cout) {
        try {IODirectives ioDirectives;
            ioDirectives.setIOType("stdout");
            ioDirectives.setRelationName("rel_1_A");
            IOSystem::getInstance().getWriter(SymbolMask({0}), symTable, ioDirectives, 0)->writeAll(*rel_1_A);
        } catch (std::exception& e) {std::cerr << e.what();exit(1);}
    }
public:
    void dumpOutputs(std::ostream& out = std::cout) {
        try {IODirectives ioDirectives;
            ioDirectives.setIOType("stdout");
            ioDirectives.setRelationName("rel_2_B1");
            IOSystem::getInstance().getWriter(SymbolMask({0}), symTable, ioDirectives, 0)->writeAll(*rel_2_B1);
        } catch (std::exception& e) {std::cerr << e.what();exit(1);}
    }
public:
    const SymbolTable &getSymbolTable() const {
        return symTable;
    }
};
//SouffleProgram *newInstance_yes_partition(){return new Sf_yes_partition_1;}
//SymbolTable *getST_yes_partition(SouffleProgram *p){return &reinterpret_cast<Sf_yes_partition_1*>(p)->symTable;}
#ifdef __EMBEDDED_SOUFFLE__
class factory_Sf_yes_partition_1: public souffle::ProgramFactory {
    SouffleProgram *newInstance() {
        return new Sf_yes_partition_1();
    };
public:
    factory_Sf_yes_partition_1() : ProgramFactory("yes_partition"){}
};
static factory_Sf_yes_partition_1 __factory_Sf_yes_partition_1_instance;
}
#else
}
#if 0
int main(int argc, char** argv)
{
    try{
        souffle::CmdOptions opt(R"(yes-partition.dl)",
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
        souffle::Sf_yes_partition_1 obj;
        obj.runAll(opt.getInputFileDir(), opt.getOutputFileDir());
        return 0;
    } catch(std::exception &e) { souffle::SignalHandler::instance()->error(e.what());}
}
#endif
#endif
