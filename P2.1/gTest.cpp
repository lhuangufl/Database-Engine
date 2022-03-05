#include <iostream>
#include "gtest/gtest.h"
#include "DBFile.h"
#include "gTest.h"

#include "test.h"
#include "BigQ.h"
#include <pthread.h>

void* producer(void* arg) {

        Pipe* myPipe = (Pipe*)arg;

        Record temp;
        int counter = 0;

        DBFile dbfile;
        dbfile.Open(rel->path());
        dbfile.MoveFirst();

        while (dbfile.GetNext(temp) == 1) {
                counter += 1;
                if (counter % 100000 == 0) {
                        cerr << " producer: " << counter << endl;
                }
                myPipe->Insert(&temp);
        }
        dbfile.Close();
        myPipe->ShutDown();
        return NULL;
}

void* consumer(void* arg) {

        testutil* t = (testutil*)arg;

        ComparisonEngine ceng;

        DBFile dbfile;
        char outfile [100];

        if (t->write) {
                sprintf(outfile, "%s.bigq", rel->path());
                dbfile.Create(outfile, heap, NULL);
        }

        int err = 0;
        int i = 0;

        Record rec [2];
        Record* last = NULL, * prev = NULL;

        while (t->pipe->Remove(&rec [i % 2])) {
                prev = last;
                last = &rec [i % 2];

                if (prev && last) {
                        if (ceng.Compare(prev, last, t->order) == 1) {
                                err++;
                        }
                        if (t->write) {
                                dbfile.Add(*prev);
                        }
                }
                if (t->print) {
                        // last->Print(rel->schema());
                }
                i++;
        }

        if (t->write) {
                if (last) {
                        dbfile.Add(*last);
                }
                cerr << " consumer: recs removed written out as heap DBFile at " << outfile << endl;
                dbfile.Close();
        }
        if (err) {
                cerr << " consumer: " << err << " recs failed sorted order test \n" << endl;
        }
        return NULL;
}

TEST(BigQTest, CreateTest) {
        //arrange
        //act
        //assert
        setup();
        relation* rel_ptr[] = { n, r, c, p, ps, o, li };
        rel = rel_ptr [0];
        int runlen = 4;

        OrderMaker* sortorder = new OrderMaker(rel->schema());

        int buffsz = 100; // pipe cache size
        Pipe input(buffsz);
        Pipe output(buffsz);

        // thread to dump data into the input pipe (for BigQ's consumption)
        pthread_t thread1;
        pthread_create(&thread1, NULL, producer, (void*)&input);

        // thread to read sorted data from output pipe (dumped by BigQ)
        pthread_t thread2;
        testutil tutil = { &output, sortorder, true, false };

        pthread_create(&thread2, NULL, consumer, (void*)&tutil);

        EXPECT_EQ(input.isRunning(), true);
        EXPECT_EQ(output.isRunning(), true);

        BigQ bq(input, output, *sortorder, runlen);

        EXPECT_EQ(input.isRunning(), false);
        EXPECT_EQ(output.isRunning(), false);

        pthread_join(thread1, NULL);
        pthread_join(thread2, NULL);
}





int main(int argc, char** argv) {

        ::testing::InitGoogleTest(&argc, argv);

        return RUN_ALL_TESTS();
}
