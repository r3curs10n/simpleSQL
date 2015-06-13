all: crapsql

norun: lex.yy.c parse.tab.c parse.tab.h myfiles
	g++ -c condition.o condition_and.o condition_or.o condition_op_const.o condition_op_column.o node.o

crapsql: lex.yy.c parse.tab.c parse.tab.h myfiles
	g++ -c -fpermissive -o ../Space_and_Buffer_Manager/file.o ../Space_and_Buffer_Manager/file.cpp
	g++ -c -fpermissive -o ../File_and_Access_Layer/file_and_access_layer.o ../File_and_Access_Layer/file_and_access_layer.cpp
	g++ -fpermissive -o crapsql lex.yy.c parse.tab.c condition.o condition_and.o condition_or.o condition_op_const.o condition_op_column.o node.o node_join.o node_table.o condition_list.o ../Space_and_Buffer_Manager/file.o ../File_and_Access_Layer/file_and_access_layer.o projection.o

myfiles: condition.o condition_and.o condition_or.o condition_op_const.o condition_op_column.o node.o node_table.o node_join.o condition_list.o projection.o
#	g++ -c condition.cpp condition_and.cpp condition_op_const.cpp node.cpp

l2files: ../Space_and_Buffer_Manager/file.o ../File_and_Access_Layer/file_and_access_layer.o
	g++ -c -fpermissive -o ../Space_and_Buffer_Manager/file.o ../Space_and_Buffer_Manager/file.cpp
	g++ -c -fpermissive -o ../File_and_Access_Layer/file_and_access_layer.o ../File_and_Access_Layer/file_and_access_layer.cpp


lex.yy.c: lex.l parse.tab.h
	flex lex.l

parse.tab.c parse.tab.h: parse.y
	bison -d parse.y

clean:
	rm *.o crapsql parse.tab.c parse.tab.h lex.yy.c ../Space_and_Buffer_Manager/*.o ../File_and_Access_Layer/*.o