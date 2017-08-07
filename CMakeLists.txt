cmake_minimum_required (VERSION 2.6)
project (parallel_RA)


SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11")


FIND_PACKAGE(MPI REQUIRED)

#ADD_SUBDIRECTORY(hash)
ADD_SUBDIRECTORY(core)
ADD_SUBDIRECTORY(ra)
ADD_SUBDIRECTORY(data_struct)
ADD_SUBDIRECTORY(examples)

#INCLUDE_DIRECTORIES(${MPI_INCLUDE_PATH})
#add_executable(all_to_all test_mpi_all_to_all.c)
#target_link_libraries(all_to_all ${MPI_LIBRARIES})
