#ifndef BIGQ_H
#define BIGQ_H

#include <algorithm> 
#include <iostream>
#include <pthread.h>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>
#include <unistd.h>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "Pipe.h"
#include "Schema.h"
#include "Util.h"

using namespace std;

enum SortOrder {Ascending, Descending};

class Block {
    private:
        long long m_blockSize;
        long long m_nextLoadPageIdx;
        long long m_runEndPageIdx;
        File m_inputFile;
        vector<Page*> m_pages; 
        
    public:
        Block();
        Block(long long size, pair<long long, long long> runStartEndPageIdx ,File &inputFile);
        
        bool noMorePages(); 
        bool isFull();
        bool isEmpty();
        int loadPage();
        int getFrontRecord(Record& front);
        int popFrontRecord();
};

class BigQ {
private:
    
    Pipe *m_inputPipe; 
    Pipe *m_outputPipe;
    static SortOrder m_sortMode;
    static OrderMaker m_attOrder;
    long long m_runLength; 
    long long m_numRuns; 
    string m_sortTmpFilePath;

    vector< pair<long long, long long> > m_runStartEndLoc; 
    
    static bool compare4Sort(Record *left, Record *right);

    struct compare4PQ {
        bool operator() (pair<long long, Record*>& left, pair<long long, Record*>& right) {
            return compare4Sort(left.second, right.second);
        }
    };
    
    void sortRecords(vector<Record*> &recs, const OrderMaker &order, SortOrder mode);
    void readFromPipe(File &outputFile);

public:  
    priority_queue< pair<long long, Record*>, vector< pair<long long, Record*> >, compare4PQ > m_heap;

    void safeHeapPush(long long idx, Record* pushMe);

    int nextPopBlock(vector<Block>& blocks);

    void mergeBlocks(vector<Block>& blocks);

    void writeToPipe(File &inputFile);
    
    void externalSort();

    BigQ() {}

    BigQ (Pipe &inputPipe, Pipe &outputPipe, OrderMaker &order, int runLength);
    
    ~BigQ();
};

#endif