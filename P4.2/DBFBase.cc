#include <iostream>
#include <fstream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFBase.h"
#include "Defs.h"

using namespace std;

DBFBase::DBFBase() {
    file = new File();
    currentDataPageIdx = -1;
}

DBFBase::~DBFBase() {
    if (file != NULL) {
        delete file;
        file = NULL;
    }
}

int DBFBase::Create (const char *f_path, fType f_type, void *startup) {
    file->Open(0, (char*)f_path);
    myType = f_type;
    currentDataPageIdx = -1;
    currentPage.EmptyItOut();
    return 1;
}

void DBFBase::Load (Schema &f_schema, const char *loadpath) {
    FILE* data_file = fopen(loadpath, "r");
    Page new_page;
    Record* new_record = new Record();    
    int cnt = 0;
    while (new_record->SuckNextRecord(&f_schema, data_file)){
        if (!new_page.Append(new_record))
        {
            file->AddPage(&new_page, currentDataPageIdx + 1); 
            currentDataPageIdx++;
            new_page.EmptyItOut();
            new_page.Append(new_record);
        }
        cnt++;
    }
    
    std::cout << "The number of loaded records: " << cnt << std::endl;

    if (!new_page.IsEmpty()) {
        file->AddPage(&new_page, currentDataPageIdx + 1); 
        currentDataPageIdx++;
        new_page.EmptyItOut();
    }

    file->GetPage(&currentPage, currentDataPageIdx);
    this->MoveFirst();
    delete new_record;
    new_record = NULL;

}

int DBFBase::Open (const char *f_path) {
    file->Open(1, (char*)f_path);
#ifdef verbose
    cout << "[Info] In DBFile::Open (const char *f_path): Length of file " << f_path << " : " << file->GetLength() << endl;
#endif

    MoveFirst();

    return 1;
}

void DBFBase::MoveFirst () {
    if (file->GetLength() > 1) {
        file->GetPage(&currentPage, 0); 
        currentPage.MoveToFirst(); 
        currentDataPageIdx = 0;
    }
    else{
#ifdef verbose
        cerr << "[Error] In funciton DBFile::MoveFirst (): No data in file" << endl;
#endif
    }
}

int DBFBase::Close () {
    try{
        int state = file->Close();
        currentDataPageIdx = -1;
        currentPage.EmptyItOut();
        return state;
    }
    catch (exception e){
        cerr << "[Error] In function DBFile::Close (): " << e.what() << endl;
        return 0;
    }
}

void DBFBase::Add (Record &rec) {
    off_t cur_len = file->GetLength();
    off_t last_data_page_idx = cur_len - 2;
    
    Page last_data_page;
    
    if (last_data_page_idx >= 0){
        file->GetPage(&last_data_page, last_data_page_idx);
    }

    if (!last_data_page.Append(&rec)) {
        Page new_page;
        new_page.Append(&rec);
        file->AddPage(&new_page, last_data_page_idx + 1);
    }
    else {
        if (last_data_page_idx >= 0) {
            file->AddPage(&last_data_page, last_data_page_idx);
        }
        else {
            file->AddPage(&last_data_page, 0);
        }
    }

    if (currentDataPageIdx < 0) {
        MoveFirst();
    }
}

int DBFBase::GetNext (Record &fetchme) {
    off_t cur_len = file->GetLength();
    off_t last_data_page_idx = cur_len - 2;
    if (last_data_page_idx < 0){
#ifdef verbose
        cerr << "[Error] In funciton DBFile::GetNext (Record &fetchme): No data in file" << endl;
#endif
        return 0;
    }
    int status = currentPage.GetNextRecord(fetchme);
    if (status == 2){
        if (currentDataPageIdx + 1 > last_data_page_idx){
#ifdef verbose
            cout << "[Info] In function DBFile::GetNext (Record &fetchme): Has arrived at the end of current file." << endl;
#endif
            return 0;
        }
        currentDataPageIdx++;
        file->GetPage(&currentPage, currentDataPageIdx);
        currentPage.GetFirstNoConsume(fetchme);
        return 1;
    }
    return status;
}

int DBFBase::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp_engin; 
    while (GetNext(fetchme)){
        if (comp_engin.Compare(&fetchme, &literal, &cnf) == 1){
            return 1;
        }
    }
    return 0;
}
