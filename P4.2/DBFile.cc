#include <iostream>
#include <fstream>
#include <string>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

using namespace std;

void twoWayMerge(Pipe* pipe, DBFBase* sorted_file, DBFBase* out, OrderMaker* order) {
    cout << "Two way merge" << endl;
    int cnt1 = 0, cnt2 = 0, cntt = 0;

    sorted_file->MoveFirst();
    out->MoveFirst();

    Record rec1;
    Record rec2;
    bool is_rec1_remained = false, is_rec2_remained = false;
    int read_rec1, read_rec2;
    Record recOut;
    ComparisonEngine comp;

    while (true) {
        if ( !is_rec1_remained ) {
            read_rec1 = pipe->Remove(&rec1);
        }
        else {
            read_rec1 = 1;
        }
        if ( !is_rec2_remained ) {
            read_rec2 = sorted_file->GetNext(rec2);
        }
        else {
            read_rec2 = 1;
        }
        if (read_rec1 == 1 && read_rec2 == 1) {
            if (comp.Compare(&rec1, &rec2, order) > 0) {
                recOut.Consume(&rec2);
                is_rec1_remained = true;
                is_rec2_remained = false;
                cnt2++;
            }
            else {
                recOut.Consume(&rec1);
                is_rec1_remained = false;
                is_rec2_remained = true;
                cnt1++;
            }
        }
        else if (read_rec1) {
            recOut.Consume(&rec1);
            is_rec1_remained = false;
            cnt1++;
        }
        else if (read_rec2) {
            recOut.Consume(&rec2);
            is_rec2_remained = false;
            cnt2++;
        }
        else {
            break;
        }

        out->Add(recOut);
        cntt++;
    }
    out->MoveFirst();
    cout << "Count of records from pipe: " << cnt1 << endl;
    cout << "Count of records from file: " << cnt2 << endl;
    cout << "Total added records: " << cntt << endl;
}

void emptyFile(string fpath) {
    DBFBase db;
	db.file->Open(0, (char*)(fpath.c_str()));
	db.Close();
}

fType GenericDBFile::getFileType() {
    return m_fileType;
}


Sorted::Sorted(OrderMaker myOrder, int runLength, DBFBase* file, DBFBase* cache, string fpath, string cache_path) {
    m_fileType = sorted;
    m_file = file;
    m_order = myOrder;
    m_runLength = runLength;
    m_mode = reading;
    m_cache = cache;
    m_file_path = fpath;
    m_cache_path = cache_path;
    continuousGetNext = false;
}

int Sorted::Create(const char *fpath, fType file_type, void *startup) {
    m_file_path = string(fpath);
    m_cache_path = string(fpath) + ".cache";
    m_file->Create(fpath, file_type, startup);
    m_cache->Create((char*)(m_cache_path.c_str()), file_type, startup);
    m_order = *((*((SortInfo*)startup)).myOrder);
    m_runLength = (*((SortInfo*)startup)).runLength;
    return 1;
}

int Sorted::Close () {
    try{  
        continuousGetNext = false;
        startReading();
        moveCacheToFile();

        int state = m_file->Close();
        m_cache->Close();
        
        emptyFile(m_cache_path);
        m_file_path = "";
        m_cache_path = "";
        
        return state;
    }
    catch (exception e){
        cerr << "[Error] In function DBFile::Close (): " << e.what() << endl;
        return 0;
    }
}


void Sorted::Load (Schema &f_schema, const char *loadpath) {
    m_file->Load(f_schema, loadpath);
}

void Sorted::Add (Record &addme) {
    continuousGetNext = false;
    startWriting();
    m_sorter.inPipe->Insert(&addme);
}

void Sorted::MoveFirst () {
    continuousGetNext = false;
    startReading();
    moveCacheToFile();

    if (m_file->file->GetLength() > 1) {
        m_file->file->GetPage(&m_file->currentPage, 0);
        m_file->currentPage.MoveToFirst();
        m_file->currentDataPageIdx = 0;
    }
    else{
        m_file->currentDataPageIdx = -1;
        cerr << "[Error] In funciton DBFile::MoveFirst (): No data in file" << endl;
    }
}

int Sorted::GetNext (Record &fetchme) {
    return m_file->GetNext(fetchme);
}

int Sorted::GetNextByBinarySearch (Record &fetchme, CNF &cnf, Record &literal) { // Notice: file may be empty.
    off_t lpage_idx = m_file->currentDataPageIdx;
    off_t rpage_idx = m_file->lastDataPageIdx();

    cout << lpage_idx << " " << rpage_idx << endl;

    if (rpage_idx < 0) {
        return 0;
    }
    
    return m_file->GetNext(fetchme, cnf, literal);
    
    ComparisonEngine comp_engin; 
    
    while (lpage_idx <= rpage_idx) {
        
        off_t mid = (lpage_idx + rpage_idx) / 2;
        m_file->file->GetPage(&(m_file->currentPage), mid);    
        m_file->currentDataPageIdx = mid;
        m_file->currentPage.MoveToFirst();

        if (!m_file->GetNext(fetchme)){
            cerr << "[Error] Empty page found in middle of file" << endl;
            exit(1);
        }
        int res = comp_engin.Compare(&fetchme, &literal, &queryOrder);
        if (res < 0) {
            lpage_idx = mid + 1;
        }
        else if (res >= 0) {
            rpage_idx = mid - 1;
        }
    }

    lpage_idx = max(lpage_idx, (off_t)0);
    rpage_idx = max(rpage_idx, (off_t)0);
    off_t first_possible_page = min(lpage_idx, rpage_idx);
    m_file->file->GetPage(&(m_file->currentPage), first_possible_page);    
    m_file->currentDataPageIdx = first_possible_page;
    m_file->currentPage.MoveToFirst();

    return m_file->GetNext(fetchme, cnf, literal);

}

int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if (continuousGetNext) {
        return m_file->GetNext(fetchme, cnf, literal);
    }
    else{
        continuousGetNext = true;
        //cout << "Bin search" << endl;
        return GetNextByBinarySearch(fetchme, cnf, literal);
    }
}

DBFile::DBFile () {
    m_file = new DBFBase();
    m_cache = new DBFBase();
    m_instance = NULL;
}

string DBFile::metaInfoToStr(MetaInfo meta_info) {
    string res = "";
    res += fTypeStr[meta_info.fileType];
    if (meta_info.fileType == sorted) {
        res += "\n" + Util::toString(meta_info.runLength) + "\n";
        res += meta_info.order.toString();
    }
    return res;
}

MetaInfo DBFile::readMetaInfo(string& meta_file) {
    string file_type_str;
    fType file_type;
    int runLength = -1;
    OrderMaker order;    
    
    ifstream meta;
    meta.open((char*)meta_file.c_str());

    meta >> file_type_str;

    if (file_type_str == fTypeStr[heap]) {
        file_type = heap;
    }
    else if (file_type_str == fTypeStr[sorted]) {
        string numAttsStr, whichAttsStr, whichTypesStr;
        file_type = sorted;
        meta >> runLength;
        meta.ignore();
        getline(meta, numAttsStr);
        getline(meta, whichAttsStr);
        getline(meta, whichTypesStr);
        order = OrderMaker(numAttsStr, whichAttsStr, whichTypesStr);
    }
    meta.close();

    return MetaInfo {
        file_type,
        runLength,
        order
    };

}

void DBFile::writeMetaInfo(string& meta_file, MetaInfo& meta_info) {
    
    ofstream meta;
    meta.open((char*)meta_file.c_str());
    
    string meta_str = metaInfoToStr(meta_info);
    meta << meta_str;
    meta.close();
}

int DBFile::Open(const char *fpath) {
    m_file_path = string(fpath);
    m_cache_path = string(fpath) + ".cache";
    m_file->Open(fpath);
    m_cache->Open((char*)(m_cache_path.c_str()));
    
    m_metaPath = string(fpath) + ".meta";
    
    m_metaInfo = readMetaInfo(m_metaPath);

    
    if (m_instance != NULL) {
        delete m_instance;
        m_instance = NULL;
    }
    
    if (m_metaInfo.fileType == heap) {
        m_instance = new Heap(m_file, m_cache, m_file_path, m_cache_path);
    }
    else if (m_metaInfo.fileType == sorted) {
        m_instance = new Sorted(m_metaInfo.order, m_metaInfo.runLength, m_file, m_cache, m_file_path, m_cache_path);
    }
    else {
        cerr << "[Error] In DBFile::Open (const char *f_path): Invalid file type '" << fTypeStr[m_metaInfo.fileType] << "'" << endl;
        return 0;
    }
    return 1;
}


int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    m_file_path = string(f_path);
    m_cache_path = string(f_path) + ".cache";

    if (m_instance != NULL) {
        delete m_instance;
        m_instance = NULL;
    }

    if (f_type == heap) {
        m_instance = new Heap(m_file, m_cache, m_file_path, m_cache_path);
        m_metaInfo = MetaInfo {
            heap, 
            -1,
            OrderMaker()
        };
    }
    else if (f_type == sorted) {
        m_instance = new Sorted(*(((SortInfo*)startup)->myOrder), ((SortInfo*)startup)->runLength, m_file, m_cache, m_file_path, m_cache_path);
        m_metaInfo = MetaInfo {
            sorted, 
            ((SortInfo*)startup)->runLength,
            *(((SortInfo*)startup)->myOrder)
        };
    }
    
    if (m_instance == NULL) {
        cerr << "[Error] In DBFile::Create (const char *f_path, fType f_type, void *startup): No proper file created" << endl;
        return 0;
    }
    
    m_instance->Create(f_path, f_type, startup);
    m_metaPath = string(f_path) + ".meta";

    return 1;
}


int DBFile::Close () {
    try{  
        writeMetaInfo(m_metaPath, m_metaInfo);
        int state = m_instance->Close();
        delete m_instance;
        m_instance = NULL;    
        return state;
        
    }
    catch (exception e){
        cerr << "[Error] In function DBFile::Close (): " << e.what() << endl;
        return 0;
    }
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    m_instance->Load(f_schema, loadpath);
}

void DBFile::Add (Record &rec) {
    m_instance->Add(rec);
}

void DBFile::MoveFirst () {
    m_instance->MoveFirst();
}

int DBFile::GetNext (Record &fetchme) {
    return m_instance->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return m_instance->GetNext(fetchme, cnf, literal);    
}

Heap::Heap(DBFBase* file, DBFBase* cache,  string fpath, string cache_path) {
    m_fileType = heap;
    m_file = file;
    m_cache = cache;
    m_file_path = fpath;
    m_cache_path = cache_path;
}

int Heap::Create(const char *fpath, fType file_type, void *startup) {
    m_file_path = string(fpath);
    m_cache_path = string(fpath) + ".cache";
    m_file->Create(fpath, file_type, startup);
    m_cache->Create((char*)(m_cache_path.c_str()), file_type, startup);
    return 1;
}

int Heap::Close () {
    try{  
        m_file->Close();
        m_cache->Close();
        emptyFile(m_cache_path);
        m_cache_path = "";
        m_file_path = "";
    }
    catch (exception e){
        cerr << "[Error] In function DBFile::Close (): " << e.what() << endl;
        return 0;
    }
    return 1;
}
		
void Heap::Load (Schema &f_schema, const char *loadpath) {
    m_file->Load(f_schema, loadpath);
}

void Heap::Add (Record &addme) {
    off_t cur_len = m_file->file->GetLength();
    off_t last_data_page_idx = cur_len - 2;
    
    Page last_data_page;
    
    if (last_data_page_idx >= 0){ 
        m_file->file->GetPage(&last_data_page, last_data_page_idx);
    }

    if (!last_data_page.Append(&addme)) {
        Page new_page;
        new_page.Append(&addme);
        m_file->file->AddPage(&new_page, last_data_page_idx + 1);
    }
    else {
        if (last_data_page_idx >= 0) {
            m_file->file->AddPage(&last_data_page, last_data_page_idx);
        }
        else {
            m_file->file->AddPage(&last_data_page, 0);
        }
    }
    
}

int Heap::GetNext (Record &fetchme) {
    off_t cur_len = m_file->file->GetLength();
    off_t last_data_page_idx = cur_len - 2;
    if (last_data_page_idx < 0){
        cerr << "[Error] In funciton DBFile::GetNext (Record &fetchme): No data in file" << endl;
        return 0;
    }
    int status = m_file->currentPage.GetNextRecord(fetchme);
    if (status == 2){
        if (m_file->currentDataPageIdx + 1 > last_data_page_idx){
            cout << "[Info] In function DBFile::GetNext (Record &fetchme): Has arrived at the end of current file." << endl;
            return 0;
        }
        m_file->currentDataPageIdx++;
        m_file->file->GetPage(&m_file->currentPage, m_file->currentDataPageIdx);
        m_file->currentPage.GetFirstNoConsume(fetchme);
        return 1;
    }
    return status;
}

int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return m_file->GetNext(fetchme, cnf, literal);
}

void Heap::MoveFirst () {
    if (m_file->file->GetLength() > 1) {
        m_file->file->GetPage(&m_file->currentPage, 0);
        m_file->currentPage.MoveToFirst();
        m_file->currentDataPageIdx = 0;
    }
    else{
        cerr << "[Error] In funciton DBFile::MoveFirst (): No data in file" << endl;
    }
}



