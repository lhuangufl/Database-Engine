//
// Created by Lin Huang on 03/16/2022//

#ifndef DBFILEGENERIC_H
#define DBFILEGENERIC_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

typedef enum { heap, sorted, tree } fType;
typedef struct { OrderMaker* orderMaker; int runlen; } SortedInfo;
class DBFileGeneric {
public:
        virtual int Create(char* fpath, fType f_type, void* startup) = 0;
        virtual int Open(char* fpath) = 0;
        virtual int Close() = 0;

        virtual void Load(Schema& myschema, char* loadpath) = 0;

        virtual void MoveFirst() = 0;
        virtual void Add(Record& addme) = 0;
        virtual int GetNext(Record& fetchme) = 0;
        virtual int GetNext(Record& fetchme, CNF& cnf, Record& literal) = 0;
};
//
//
//
#endif //DBFILEGENERIC_H
