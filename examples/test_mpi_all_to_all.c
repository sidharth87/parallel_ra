/*

  // Send buffer
  P0: 0,0, 1,1, 2,2, 3,3, 4,4
  P1: 1,1, 2,2, 3,3, 4,4, 5,5
  P2: 2,2, 3,3, 4,4, 5,5, 6,6
  P3: 3,3, 4,4, 5,5, 6,6, 7,7
  P4: 4,4, 5,5, 6,6, 7,7, 8,8

  COMMUNICATION

  // Receive buffer
  P0: 0,0, 1,1, 2,2, 3,3, 4,4
  P1: 1,1, 2,2, 3,3, 4,4, 5,5
  P2: 2,2, 3,3, 4,4, 5,5, 6,6
  P3: 3,3, 4,4, 5,5, 6,6, 7,7
  P4: 4,4, 5,5, 6,6, 7,7, 8,8


*/



#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <limits.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdint.h>
#include <ctype.h>
#include <mpi.h>

#undef DEBUG_OUTPUT
#define LOOP_COUNT 100

static int mode = 0;
static int count = 1;
static int rank;
static int nprocs;

static void parse_args(int argc, char **argv);
static void terminate_with_error_msg(const char *format, ...);
static void verify_result(int* rec_buf);

static char *usage = "Parallel Usage: mpirun -n 8 ./all_to_all -m 0 -c 1\n"
                     "  -m: mode [0, 1, 2]\n"
                     "  -c: count\n";


int main(int argc, char **argv)
{
  // Initializing MPI
  MPI_Init(&argc, &argv);

  int *send_buffer;
  int *recieve_buffer;

  int ret;
  int i = 0, j = 0, lc = 0;

  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  // Parse input arguments and initialize
  parse_args(argc, argv);

  recieve_buffer = malloc(nprocs * count * sizeof(*recieve_buffer));
  memset(recieve_buffer, 0, (nprocs * count * sizeof(*recieve_buffer)));

  send_buffer = malloc(nprocs * count * sizeof(*send_buffer));
  memset(send_buffer, 0, (nprocs * count * sizeof(*send_buffer)));

  for (i = 0; i < nprocs; i++)
    for (j = 0; j < count; j++)
      send_buffer[i * count + j] = rank + i;

  double start_time = 0;
  double end_time = 0;
  double total_time[LOOP_COUNT];

  for (lc = 0; lc < LOOP_COUNT; lc++)
  {
    start_time = MPI_Wtime();

    // Communication using collective all to all call
    if (mode == 0)
    {
      ret = MPI_Alltoall(send_buffer, count, MPI_INT, recieve_buffer, count, MPI_INT, MPI_COMM_WORLD);
      if (ret != MPI_SUCCESS)
        terminate_with_error_msg("MPI_Alltoall Failed\n");
    }

    // point to point communication
    else if (mode == 1)
    {
      MPI_Request *req = malloc(sizeof(*req) * nprocs * 2);
      MPI_Status *status = malloc(sizeof(*status) * nprocs * 2);

      for (i = 0; i < nprocs; i++)
        MPI_Irecv(recieve_buffer + (i * count), count, MPI_INT, i, 123, MPI_COMM_WORLD, &req[i]);

      for (i = 0; i < nprocs; i++)
        MPI_Isend(send_buffer + (i * count), count, MPI_INT, i, 123, MPI_COMM_WORLD, &req[i + nprocs]);

      ret = MPI_Waitall(nprocs * 2, req, status);
      if (ret != MPI_SUCCESS)
        terminate_with_error_msg("MPI_Waitall Failed\n");
    }

    // one sided communication
    else if (mode == 2)
    {
      MPI_Win win;
      MPI_Win_create(recieve_buffer, nprocs * count * sizeof(int), sizeof(int), MPI_INFO_NULL, MPI_COMM_WORLD, &win);

      MPI_Win_fence(0, win);

      for (i = 0; i < nprocs; i++)
      {
        ret = MPI_Put(send_buffer + (i * count), count, MPI_INT, i, rank * count, count, MPI_INT, win);
        if (ret != MPI_SUCCESS)
          terminate_with_error_msg("MPI_Waitall Failed\n");
      }

      MPI_Win_fence(0, win);
      MPI_Win_free(&win);
    }

    end_time = MPI_Wtime();
    total_time[lc] = end_time - start_time;

#if DEBUG_OUTPUT
    verify_result(recieve_buffer);
#endif

  }

  free(recieve_buffer);
  free(send_buffer);

  double max_time;
  double total_loop_time = 0;
  for (lc = 0; lc < LOOP_COUNT; lc++)
  {
    MPI_Allreduce(&total_time[lc], &max_time, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
    total_loop_time = total_loop_time + total_time[lc];

    if (rank == 0)
      printf("Time taken at loop %d = %f [%f]\n", lc, max_time, total_loop_time);
  }

  double total_max_loop_time = 0;
  MPI_Allreduce(&total_loop_time, &total_max_loop_time, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
  if (rank == 0)
  {
    if (mode == 0)
      printf("[All to All] Bytes moved from every process %d [chunk %d x int size %d x nprocs %d] Total time %f\n", (int) (count * sizeof(int) * nprocs), count, (int) sizeof(int), nprocs, total_max_loop_time / LOOP_COUNT);

    if (mode == 1)
      printf("[P2P] Bytes moved from every process %d [chunk %d x int size %d x nprocs %d] Total time %f\n", (int) (count * sizeof(int) * nprocs), count, (int) sizeof(int), nprocs, total_max_loop_time / LOOP_COUNT);

    if (mode == 2)
      printf("[One Sided] Bytes moved from every process %d [chunk %d x int size %d x nprocs %d] Total time %f\n", (int) (count * sizeof(int) * nprocs), count, (int) sizeof(int), nprocs, total_max_loop_time / LOOP_COUNT);
  }

  MPI_Finalize();
  return 0;
}



static void parse_args(int argc, char **argv)
{
  char flags[] = "m:c:";
  int one_opt = 0;

  while ((one_opt = getopt(argc, argv, flags)) != EOF)
  {
    switch (one_opt)
    {
      case('m'): // number of timesteps
        if (sscanf(optarg, "%d", &mode) < 0)
          terminate_with_error_msg("Invalid variable file\n%s", usage);
        break;

      case('c'): // number of timesteps
        if (sscanf(optarg, "%d", &count) < 0)
          terminate_with_error_msg("Invalid variable file\n%s", usage);
        break;

      default:
        terminate_with_error_msg("Wrong arguments\n%s", usage);
    }
  }

  return;
}


//----------------------------------------------------------------
static void terminate_with_error_msg(const char *format, ...)
{
  va_list arg_ptr;
  va_start(arg_ptr, format);
  vfprintf(stderr, format, arg_ptr);
  va_end(arg_ptr);

  MPI_Abort(MPI_COMM_WORLD, -1);
}


static void verify_result(int *recieve_buffer)
{
#if DEBUG_OUTPUT
  int i = 0, j = 0;

  if (rank == 1)
    printf("Mode %d count %d: ", mode, count);

  for (i = 0; i < nprocs; i++)
  {
    for (j = 0; j < count; j++)
    {
      if (recieve_buffer[i * count + j] != rank + i)
      {
        printf("RB %d\n", recieve_buffer[i * count + j]);
        terminate_with_error_msg("Verification Failed", recieve_buffer[i * count + j]);
      }

      if (rank == 1)
        printf("%d\t", recieve_buffer[i * count + j]);
    }
  }

  if (rank == 1)
    printf("\n");
#endif
  return;
}
