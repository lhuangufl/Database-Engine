#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>

#include <iostream>
#include <queue>

#include "File.h"
#include "Pipe.h"
#include "Record.h"

using namespace std;

typedef struct {
  Pipe *in;
  Pipe *out;
  OrderMaker *order;
  int runlen;
} WorkerArgs;

enum sortmode { DESC, ASC };

void *WorkerMain(void *arg);

class Run {
 private:
  File *fileBase;
  Page bufferPage;
  int startPageIdx;
  int curPageIdx;
  int runlen;

 public:
  Record *topRecord;
  Run(File *file, int startPageIdx, int runlen);
  int UpdateTopRecord();
};
class RecordComparator {
 private:
  OrderMaker *order;

 public:
  bool operator()(Record *left, Record *right);
  RecordComparator(OrderMaker *order);
};

class RunComparator {
 private:
  OrderMaker *order;

 public:
  bool operator()(Run *left, Run *right);
  RunComparator(OrderMaker *order);
};

void *RecordQueueToRun(
    priority_queue<Record *, vector<Record *>, RecordComparator> &recordQ,
    priority_queue<Run *, vector<Run *>, RunComparator> &runQ, File &file,
    Page &bufferPage, int &pageIndex);
class BigQ {
 public:
  BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ();
};

#endif
