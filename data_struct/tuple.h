class tuple{

private:
    int number_of_columns;

public:
    int get_number_of_column();
    void set_number_of_column(int col_count);

};

int tuple::get_number_of_column()
{
    return number_of_columns;
}

void tuple::set_number_of_column(int n)
{
    number_of_columns = n;
    return;
}
