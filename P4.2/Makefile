
CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

main:   y.tab.o lex.yy.o main.o
	$(CC) -o main y.tab.o lex.yy.o main.o -lfl
	
main.o : main.cc
	$(CC) -g -c main.cc
	
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
