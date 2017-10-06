all:
	mpic++ -O3 -std=c++11 TC_mpi.cpp -lpthread -lmpi -o TC
	mpic++ -O3 -std=c++11 partition_input_relations.cpp -lpthread -lmpi -o partition
	g++ data_parser.c -o data_parser

clean:
	rm  TC
