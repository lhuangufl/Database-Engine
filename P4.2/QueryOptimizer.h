#ifndef QUERY_OPTIMIZER_H
#define QUERY_OPTIMIZER_H

#include <algorithm>
#include <stdint.h>
#include <iostream>
#include <map>
//#include <set>
#include <string>
#include <vector>
#include <list>
#include <utility>
#include <string.h>
#include "Comparison.h"
#include "DBFile.h"
#include "Defs.h"
#include "Function.h"
#include "Pipe.h"
#include "Record.h"
#include "RelOp.h"
#include "Schema.h"
#include "Statistics.h"
#include "Util.h"

#define Undefined -1
#define NOT_FOUND -1

#define SELECT_FILE 0
#define SELECT_PIPE 1
#define PROJECT 2
#define JOIN 3
#define DUP_REMOVAL 4
#define SUM 5
#define GROUP_BY 6
#define WRITE_OUT 7

#define NONE 0
#define SINGLE 1
#define DOUBLE 2

extern FuncOperator* finalFunction;
extern TableList* tables;
extern AndList* boolean;
extern NameList* groupingAtts;
extern NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;
extern struct AttrList* newattrs;
extern char* newtable;
extern char* newfile;
extern char* oldtable;
extern char* deoutput;

class QueryPlan;
class QueryPlanNode;
class SelectNode;
class ProjectNode;
class JoinNode;
class SumNode;
class GroupByNode;
class WriteOutNode;


void printQueryPlanTree(QueryPlanNode* root);
void printInOrderQueryPlan(QueryPlanNode* root);
void deleteQueryPlanTree(QueryPlanNode* root);
void executeQueryPlan(QueryPlanNode* root, const std::map<int, Pipe*>& all_pipes);
void executeQueryPlanExceptJoin(QueryPlanNode* root, const std::map<int, Pipe*>& all_pipes);
void executeQueryPlanOnlyJoin(QueryPlanNode* root, const std::map<int, Pipe*>& all_pipes);
void waitUntilExecuteDone(QueryPlanNode* root);
void printFile(std::string filepath);

int height(QueryPlanNode* node);
void printGivenLevel(QueryPlanNode* root, int level);
void printLevelOrder(QueryPlanNode* root);
void printInOrderQueryPlan_BFS(QueryPlanNode* root);

class QueryPlan {
        friend class QueryPlanNode;
private:

        //static char* catalog_path;
        //static char* dbfile_dir;
        //static char* tpch_dir;

        const static int pipe_size = 1000;

        std::map<std::string, std::string> prefix_relation;
        Statistics* stat;
        AndList* optimal_predicate; // optimal predicate (the part after WHERE) that has the lowest estimated execution cost
        std::vector<AndList*> min_order;
        int number_of_selects;
        int number_of_joins;
        double min_cost;

        QueryPlanNode* root;
        std::vector<QueryPlanNode*> leaves;
        std::map<std::string, QueryPlanNode*> relname_node;
        std::map<QueryPlanNode*, std::vector<std::string> > node_relnames;

        std::map<int, Pipe*> pipes;

        void initPrefixMap();
        double estimateTotalCost(std::vector<AndList*>& whole_pred);
        void getRelsForStat(char* relNames[], int& numToJoin, AndList* single_pred, Statistics* tmp_stat);
        double estimateAndApplySinglePred(AndList* single_pred, Statistics* tmp_stat, char* tmpFile);

public:
        //QueryPlan();
        QueryPlan(Statistics* st);
        ~QueryPlan();
        void createPlanTree();
        void createLoadDataNodes();
        void updateRelnameNodeMap(QueryPlanNode* new_root, QueryPlanNode* left_child, QueryPlanNode* right_child);
        QueryPlanNode* createPredicateNodes();
        void createProjectNodes();
        void createDupRemovalNodes();
        void createSumNodes();
        void createGroupByNodes();
        void createWriteOutNodes(std::string out_filepath);

        void optimizePredicate();
        bool isValidPred(std::vector<AndList*>& predicates);
        bool isJoin(AndList* single_pred);
        bool isSelfJoin(AndList* single_pred);
        void removeSelfJoinPred(std::vector<AndList*>& predicates);
        std::vector<std::string> getRelNames(AndList* single_pred);
        void printPredicate();
        int height(QueryPlanNode* node);
        void printGivenLevel(QueryPlanNode* root, int level);

        const QueryPlanNode* getRoot();
        const Pipe* getOutPipe();
        const Schema* getOutSchema();

        void execute();

        void printAnswers();
};


class QueryPlanNode {
public:
        std::string name;
        int type;
        int num_children;
        // If only have one child, it will be the left child and right pointer would be NULL
        QueryPlanNode* left;
        QueryPlanNode* right;
        int left_pipe_id, right_pipe_id; // IDs of left and right input pipes corresponding to left and right children
        int file_id;
        int out_pipe_id; // ID of output pipe

        int intermediate_out_pipe_id; // only used in JoinNode
        std::string intermediate_file;
        std::string intermediate_catalog;
        std::string intermediate_rel_name;
        SelectNode* left_reader_from_join, * right_reader_from_join;
        // if any pipe is non-exsiting, its ID would be -1.
        // if there is only one pipe / one child, it would be the left pipe while the right pipe id would be -1
        Schema* output_schema;

        RelationalOp* executor;

        FILE* output_file;

        std::string CNF_string;

        static int next_assign_pipeID;
        static int next_assign_fileID;

        QueryPlanNode();
        virtual ~QueryPlanNode() {};
        virtual void printSpecInfo() {};
        virtual void execute(const std::map<int, Pipe*>& pipes) {};
        void processJoinChild(QueryPlanNode* me);
        void saveSchema(std::string save_to, std::string relname);
        int getNewPipeID();
        int getType();
        //void print(int arg_type, int arg_left_pipe_id, int arg_right_pipe_id, int arg_out_pipe_id, int arg_file_id);
        void print();
};

// SelectFile and SelectPipe
class SelectNode : public QueryPlanNode {
private:
        CNF* cnf;
        Record* literal;
        //int type;
        //int file_id;
        bool load_all_data;
        //std::string from_relation;
        //std::string filename;
        std::string by_catalog;
        std::string relation_name;
        bool is_intermediate;
        DBFile* db;

public:
        //static int next_assign_fileID;
        SelectNode();
        // if select from file, there is no input pipe ID, but a DBFile ID pointing to a DBFile object.
        // at this time, there would be no child node. So child = NULL.
        // select_conditions is parseTree that only contains one OrList, like parseTree of "(ps_suppkey < 10 OR ps_partkey < 5)"
        // That is, one SelectNode corresponds to one selection clause / OrList.
        // For example, "(ps_suppkey < 10 OR ps_partkey < 5) AND (ps_suppkey > 2)" will be split into two SelectNodes,
        // one for "(ps_suppkey < 10 OR ps_partkey < 5)", another for "(ps_suppkey > 2)"
        SelectNode(int select_where, AndList* select_conditions, QueryPlanNode* child, bool is_intermediate,
                const char* catalog_filepath, const char* rel_name, const char* alias = "", int specific_out_pipe_id = -1);
        ~SelectNode();
        void printSpecInfo();
        void execute(const std::map<int, Pipe*>& pipes);
};

/*
class SelectPipeNode : protected QueryPlanNode {
    private:
        CNF *cnf;CNF *selOp;
                        Record *literal;

    public:
        SelectPipCNF *selOp;
                        Record *literal;
        SelectPipCNF *selOp;
                        Record *literal;d, int in_pipe_id, int out_pipe_id, CNF *cnf, char *catalog_path, char *rel_name, char *alias = "");
        ~SelectPipeNode();
        void printSpecInfo();
};

class SelectFileNode : protected QueryPlanNode {
    private:
        CNF *cnf;

    public:
        SelectFileNode();
        SelectFileNode(QueryPlanNode *child, int in_pipe_id, int out_pipe_id, CNF *cnf, char *catalog_path, char *rel_name, char *alias = "");
        ~SelectFileNode();
        void printSpecInfo();
};
*/

class ProjectNode : public QueryPlanNode {
private:
        struct Attribute* keep_atts;
        int* keepMe;
        int numAttsInput, numAttsOutput;

public:
        ProjectNode();
        ProjectNode(NameList* atts, QueryPlanNode* child);
        //ProjectNode(int *keepMe, int numAttsInput, int numAttsOutput, QueryPlanNode *child);
        ~ProjectNode();
        void printSpecInfo();
        void execute(const std::map<int, Pipe*>& pipes);
};

class JoinNode : public QueryPlanNode {
private:
        CNF* cnf;
        Record* literal;
        int numAttsLeft;
        int numAttsRight;
        int* attsToKeep;
        int numAttsToKeep;
        int startOfRight;
        Attribute* output_atts;

        WriteOutNode* writer;

public:
        JoinNode();
        // join_statement is parseTree that only contains one equality OrList, like "(ps_suppkey = s_suppkey)"
        // If there are more than one equality clause in query, they will be split into several JoinNode, one JoinNode for one OrList / clause.
        JoinNode(AndList* join_statement, QueryPlanNode* left_child, QueryPlanNode* right_child);
        ~JoinNode();
        void printSpecInfo();
        void execute(const std::map<int, Pipe*>& pipes);
};

class SumNode : public QueryPlanNode {
private:
        Function* func;
        //Attribute *output_atts;

public:
        SumNode();
        SumNode(FuncOperator* agg_func, QueryPlanNode* child);
        ~SumNode();
        void printSpecInfo();
        void execute(const std::map<int, Pipe*>& pipes);
};

class GroupByNode : public QueryPlanNode {
private:
        OrderMaker* order;
        Function* func;

public:
        GroupByNode();
        GroupByNode(FuncOperator* agg_func, NameList* group_atts, QueryPlanNode* child);
        ~GroupByNode();
        void printSpecInfo();
        void execute(const std::map<int, Pipe*>& pipes);
};

class DupRemovalNode : public QueryPlanNode {
private:
        //OrderMaker *order;

public:
        DupRemovalNode();
        DupRemovalNode(QueryPlanNode* child);
        ~DupRemovalNode();
        void printSpecInfo();
        void execute(const std::map<int, Pipe*>& pipes);
};

class WriteOutNode : public QueryPlanNode {
public:
        std::string output_filepath;
        //FILE *output_file;

        WriteOutNode();
        WriteOutNode(std::string out_filename, QueryPlanNode* child);
        ~WriteOutNode();
        void printSpecInfo();
        void execute(const std::map<int, Pipe*>& pipes);
};

#endif
