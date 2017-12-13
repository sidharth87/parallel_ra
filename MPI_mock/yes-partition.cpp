#include <mpi.h>
#define NUMBER_OF_TASKS 4
#include "souffle/CompiledSouffle.h"

#include "yes-partition_0.cpp"
#include "yes-partition_1.cpp"
#include "yes-partition_2.cpp"
#include "yes-partition_3.cpp"

int main(int argc, char** argv)
{
    int nprocs, rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    assert(nprocs == NUMBER_OF_TASKS);

    int recv_process_count;
    int send_process_count;
    int *recv_rank_list;
    int *send_rank_list;
    int *recv_rank_flag;
    //MPI_Request *request;
    //MPI_Request *status;
    
    // Parse json file
    if (rank == 0)
    {
      recv_process_count = 0;
      
      send_process_count = 2;
      send_rank_list = new int[send_process_count];
      send_rank_list[0] = 1;
      send_rank_list[1] = 2;

      recv_rank_flag = new int[recv_process_count];
    }
    else if (rank == 1)
    {
      recv_process_count = 1;
      recv_rank_list = new int[recv_process_count];
      recv_rank_list[0] = 0;
      
      send_process_count = 1;
      send_rank_list = new int[send_process_count];
      send_rank_list[0] = 3;

      recv_rank_flag = new int[recv_process_count];
      recv_rank_flag[0] = 0;
    }
    else if (rank == 2)
    {
      recv_process_count = 1;
      recv_rank_list = new int[recv_process_count];
      recv_rank_list[0] = 0;
      
      send_process_count = 1;
      send_rank_list = new int[send_process_count];
      send_rank_list[0] = 3;

      recv_rank_flag = new int[recv_process_count];
      recv_rank_flag[0] = 0;
    }
    else if (rank == 3)
    {
      send_process_count = 0;

      recv_process_count = 2;
      recv_rank_list = new int[recv_process_count];
      recv_rank_list[0] = 1;
      recv_rank_list[1] = 2;

      recv_rank_flag = new int[recv_process_count];
      recv_rank_flag[0] = 0;
      recv_rank_flag[1] = 0;
    }

    //MPI_Request *request = new MPI_Request[recv_process_count + send_process_count];
    //MPI_Status *status = new MPI_Status[recv_process_count + send_process_count];

    MPI_Status status;
    for (int r = 0; r < recv_process_count; r++)
    {
      MPI_Recv (&recv_rank_flag[r], 1, MPI_INT, recv_rank_list[r], 1234, MPI_COMM_WORLD, &status);
      //printf("[rank %d] rrf %d rrl %d\n", rank, recv_rank_flag[r], recv_rank_list[r]);
    }

    int count = 0;
    for (int r = 0; r < recv_process_count; r++)
    {
      if (recv_rank_flag[r] == 1)
        count++;
    }

    if (count == recv_process_count)
    {
#if 1
      try{
          souffle::CmdOptions opt(R"(yes-partition.dl)",
                                  R"(.)",
                                  R"(.)",
                                  false,
                                  R"()",
                                  1
                                  );
          if (!opt.parse(argc,argv)) return 1;
#if defined(_OPENMP)
          omp_set_nested(true);
#endif
          if (rank == 0)
          {
            souffle::Sf_yes_partition_0 obj;
            obj.runAll(opt.getInputFileDir(), opt.getOutputFileDir());
          }
          else if (rank == 1)
          {
            souffle::Sf_yes_partition_1 obj;
            obj.runAll(opt.getInputFileDir(), opt.getOutputFileDir());
          }
          else if (rank == 2)
          {
            souffle::Sf_yes_partition_2 obj;
            obj.runAll(opt.getInputFileDir(), opt.getOutputFileDir());
          }
          else if (rank == 3)
          {
            souffle::Sf_yes_partition_3 obj;
            obj.runAll(opt.getInputFileDir(), opt.getOutputFileDir());
          }

      } catch(std::exception &e) { souffle::SignalHandler::instance()->error(e.what());}
#endif
      printf("Rank ordering %d\n", rank);
    }

    int send_buffer = 1;
    for (int r = 0; r < send_process_count; r++)
    {
      MPI_Send (&send_buffer, 1, MPI_INT, send_rank_list[r], 1234, MPI_COMM_WORLD);
      //printf("S [rank %d] ssf %d ssl %d\n", rank, send_buffer, send_rank_list[r]);
    }

    MPI_Finalize();

}
