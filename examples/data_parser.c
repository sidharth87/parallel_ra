#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <strings.h>
#include <stdlib.h>
#include <stdio.h>


int main(int argc, char **argv)
{
    if (argc != 3)
    {
        printf("usage: data_parser input_data.txt output_data");
        exit(0);
    }

    mkdir(argv[2], S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    char data_filename[1024];
    char meta_data_filename[1024];

    sprintf(data_filename, "%s/data.raw", argv[2]);
    printf("Data file name %s\n", data_filename);

    sprintf(meta_data_filename, "%s/meta_data.txt", argv[2]);
    printf("Meta data file name %s\n", meta_data_filename);

    int fp_out;// = open(data_filename, O_CREAT | O_WRONLY);

    int row_count = 0;
    int element1;
    int element2;
    FILE *fp_in;
    char * line = NULL;
    fp_in = fopen(argv[1], "r");
    off_t offset = 0;
    size_t len = 0;
    while ((getline(&line, &len, fp_in)) != -1)
    {
        char *pch;
        pch = strtok(line, "\t");
        element1 = atoi(pch);

        pch = strtok(NULL , "\t");
        element2 = atoi(pch);

        //printf("%d %d\n", element1, element2);

        pwrite(fp_out, &element1, sizeof(int), offset);
        offset = offset + sizeof(int);

        pwrite(fp_out, &element2, sizeof(int), offset);
        offset = offset + sizeof(int);

        row_count++;
    }

    fclose(fp_in);
    close(fp_out);

    FILE *fp_out1;
    fp_out1 = fopen(meta_data_filename, "w");
    fprintf (fp_out1, "(row count)\n%d\n(col count)\n2", row_count);
    fclose(fp_out1);

    return 0;
}
