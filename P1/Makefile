CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out build/Record.o build/Comparison.o build/ComparisonEngine.o build/Schema.o build/File.o build/DBFile.o build/y.tab.o build/lex.yy.o build/test.o -lfl

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl

test.o: test.cc
	$(CC) -g -c test.cc -o build/test.o

main.o: main.cc
	$(CC) -g -c main.cc -o

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc -o build/Comparison.o

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc -o build/ComparisonEngine.o

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
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
