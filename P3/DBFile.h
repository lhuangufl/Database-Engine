#ifndef DBFILE_H
#define DBFILE_H

#include <fstream>
#include "BigQ.h"
#include "DBFBase.h"
#include "Defs.h"
#include "Pipe.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Util.h"
 
void twoWayMerge(Pipe* pipe, DBFBase* sorted_file, DBFBase* out, OrderMaker* order);

void emptyFile(string fpath);

struct MetaInfo {
	fType fileType;
	int runLength;
	OrderMaker order; 
};

struct SortInfo { 
	OrderMaker *myOrder; 
	int runLength;
};

struct BigQSuite {
	BigQ* sorter;
	Pipe* inPipe;
	Pipe* outPipe;
};


class GenericDBFile {
	protected:
		fType m_fileType;
		DBFBase* m_file;
		DBFBase* m_cache;
		string m_file_path;
		string m_cache_path;

	public:
		virtual ~GenericDBFile() {}
		virtual int Create (const char *fpath, fType file_type, void *startup) {}
		virtual int Close () {}
		virtual void Load (Schema &myschema, const char *loadpath) {}
		virtual void MoveFirst () {}
		virtual void Add (Record &addme) {}
		virtual int GetNext (Record &fetchme) {}
		virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) {} 
		int GetNextNoFlush (Record &fetchme);

		fType getFileType();
		
};

class Heap : public virtual GenericDBFile {
	public:
		Heap(DBFBase* file, DBFBase* cache, string fpath, string cache_path);
		~Heap() {}
		int Create (const char *fpath, fType file_type, void *startup);
		int Close ();
		void Load (Schema &myschema, const char *loadpath);
		void MoveFirst ();
		void Add (Record &addme);
		int GetNext (Record &fetchme);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);
}; 

class Sorted : public virtual GenericDBFile {
	private:
		OrderMaker m_order;
		int m_runLength;
		fMode m_mode;
		BigQSuite m_sorter;
	
		OrderMaker queryOrder;
		bool continuousGetNext;

		void createSorter() {
    		m_sorter.inPipe = new Pipe(BigQBuffSize);
			m_sorter.outPipe = new Pipe(BigQBuffSize);
    		m_sorter.sorter = new BigQ(*(m_sorter.inPipe), *(m_sorter.outPipe), m_order, m_runLength);
  		}

  		void deleteSorter() {
    		delete m_sorter.inPipe; delete m_sorter.outPipe; delete m_sorter.sorter;
    		m_sorter.sorter = NULL; m_sorter.inPipe = m_sorter.outPipe = NULL;
		}
		
		void startReading() {
			if (m_mode == writing) {
				m_mode = reading;
				m_sorter.inPipe->ShutDown();
				m_cache->Close();
				emptyFile(m_cache_path);
				m_cache->Open(m_cache_path.c_str());
				m_cache->MoveFirst();
				twoWayMerge(m_sorter.outPipe, m_file, m_cache, &m_order);
				deleteSorter();
			}
		}

		void startWriting() {
			if (m_mode == reading) {
				m_mode = writing;
				createSorter();
			}
		}
		
		void moveCacheToFile() {
			if (m_cache->file->GetLength() <= 1) {
				return;
			}
			m_file->Close();
			emptyFile(m_file_path);
			m_file->Open(m_file_path.c_str());	
			m_file->MoveFirst();
			m_cache->MoveFirst();
			Record record;
			while (m_cache->GetNext(record)) {
				m_file->Add(record);
			}
			m_file->MoveFirst();
			m_cache->Close();
			emptyFile(m_cache_path);
			m_cache->Open(m_cache_path.c_str());
			m_cache->MoveFirst();
		}
		
	public:
		Sorted() {}
		Sorted(OrderMaker myOrder, int runLength, DBFBase* file, DBFBase* cache,  string fpath, string cache_path);
		~Sorted() {}
		int Create (const char *fpath, fType file_type, void *startup);
		int Close ();
		void Load (Schema &myschema, const char *loadpath);
		void MoveFirst ();
		void Add (Record &addme);
		int GetNext (Record &fetchme);
		
		int GetNextByBinarySearch (Record &fetchme, CNF &cnf, Record &literal);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);
		OrderMaker getOrder();
		
}; 

class DBFile {

public:
	DBFBase* m_file; 
	DBFBase* m_cache;
	string m_metaPath;
	MetaInfo m_metaInfo;

	DBFile (); 
	
	string metaInfoToStr(MetaInfo meta_info);
	MetaInfo readMetaInfo(string& meta_file);
	void writeMetaInfo(string& meta_file, MetaInfo& meta_info);

	int Open (const char *fpath);
	
	int Create (const char *fpath, fType file_type, void *startup);
	
	int Close ();
	
	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	
private: 
	string m_file_path;
	string m_cache_path;
	GenericDBFile* m_instance;
	
};
#endif
