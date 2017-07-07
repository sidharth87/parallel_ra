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

#include "../data_struct/tuple.h"
#include "../ra/ra_operations.h"

int main(int argc, char **argv)
{
    // Initializing MPI
    MPI_Init(&argc, &argv);

    tuple t;
    t.set_number_of_column(2);

    MPI_Finalize();
    return 0;
}
