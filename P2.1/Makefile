
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o -lfl -lpthread

test_old.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test_old.o
	$(CC) -o test_old.out build/Record.o build/Comparison.o build/ComparisonEngine.o build/Schema.o build/File.o build/DBFile.o build/y.tab.o build/lex.yy.o build/test_old.o -lfl

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl

test.o: test.cc
	$(CC) -g -c test.cc -o build/test.o

test_old.o: test_old.cc
	$(CC) -g -c test_old.cc -o build/test_old.o

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc -o build/a1-test.o

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc -o build/Comparison.o

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc -o build/ComparisonEngine.o

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc -o build/Pipe.o

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc -o build/BigQ.o

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc -o build/DBFile.o

File.o: File.cc
	$(CC) -g -c File.cc -o build/File.o

Record.o: Record.cc
	$(CC) -g -c Record.cc -o build/Record.o

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc -o build/Schema.o

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c -o build/y.tab.o

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c -o build/lex.yy.o

clean:
	rm -f *.o
	rm -f build/*.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
