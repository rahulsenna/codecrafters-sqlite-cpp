#include <cstring>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <cassert>
#include <unordered_map>
#include <map>
#include <ranges>

// Returns number of bytes consumed
int parse_varint(const unsigned char *p, uint64_t *value)
{
    *value = 0;
    for (unsigned int i = 0; i < 8; i++)
    {
        *value = (*value << 7) | (p[i] & 0x7F);
        if ((p[i] & 0x80) == 0)
            return i + 1;
    }
    // Handle last byte specially
    *value = (*value << 8) | p[8];
    return 9;
}

struct Table
{
    std::string name;
    std::string sql;
    int rootpage;
    uint64_t row_count = 0;
    std::vector<std::vector<std::string>> rows;
};

// Parse a cell in a leaf table page to extract table names
Table get_tbl_info(const unsigned char *cell)
{
    int offset = 0;
    uint64_t payload_size;
    offset += parse_varint(cell + offset, &payload_size);
    
    uint64_t rowid;
    offset += parse_varint(cell + offset, &rowid);
    
    const unsigned char *payload = cell + offset;
    uint8_t header_size = payload[0];
    
    int header_offset = 1;  // Skip header size byte
    int column_types[5];    // sqlite_master has 5 columns
    int column_count = 0;
    
    while (header_offset < header_size && column_count < 5) 
    {
        uint64_t serial_type;
        header_offset += parse_varint(payload + header_offset, &serial_type);
        column_types[column_count++] = serial_type;
    }
    
    // Process column data
    int data_offset = header_size;
    
    // columns: 0 type | 1 name | 2 tbl_name | 3 rootpage | 4 sql
    Table res = {};
    int root_page = -1;
    for (int i = 0; i < 5; i++)
    {
        uint64_t serial_type = column_types[i];
        
        // Handle TEXT columns (odd numbers >= 13)
        if (serial_type >= 13 && serial_type % 2 == 1)
        {
            uint64_t str_len = (serial_type - 13) / 2;
            if (i == 2)// sqlite_schema.tbl_name
                res.name = std::string((char *) (payload + data_offset), str_len);
            if (i == 4)// sqlite_schema.sql
                res.sql = std::string((char *) (payload + data_offset), str_len);

            data_offset += str_len;
        } else if (i == 3)// sqlite_schema.rootpage
        {
            if (serial_type == 1)
            {
                root_page = (int) payload[data_offset];
            } else if (serial_type == 2)
            {
                root_page = ((int) payload[data_offset] << 8) | payload[data_offset + 1];
            } else if (serial_type == 3)
            {
                root_page = ((int) payload[data_offset] << 16) |
                            ((int) payload[data_offset + 1] << 8) |
                            payload[data_offset + 2];
            } else if (serial_type == 4)
            {
                root_page = ((int) payload[data_offset] << 24) |
                            ((int) payload[data_offset + 1] << 16) |
                            ((int) payload[data_offset + 2] << 8) |
                            payload[data_offset + 3];
            }
            res.rootpage = root_page;
            data_offset += serial_type;
        }
    }
    return res;
}

int get_page_size(std::ifstream &database_file)
{
    database_file.seekg(16); // Skip the first 16 bytes of the header
    char pg_sz_buffer[2];
    database_file.read(pg_sz_buffer, 2);
    unsigned short page_size = (unsigned short)pg_sz_buffer[1] | ((unsigned short)pg_sz_buffer[0] << 8);
    return page_size;
}

void parse_cell_data(Table &table, const unsigned char *cell)
{
    int offset = 0;
    uint64_t payload_size;
    offset += parse_varint(cell + offset, &payload_size);
    
    uint64_t rowid;
    offset += parse_varint(cell + offset, &rowid);
    
    const unsigned char *payload = cell + offset;
    uint8_t header_size = payload[0];
    
    int header_offset = 1;  // Skip header size byte
    int column_count = 0;
    std::vector<std::string> row_of_str;
    while (header_offset < header_size) 
    {
        uint64_t serial_type;
        header_offset += parse_varint(payload + header_offset, &serial_type);
        column_count++;
        if (serial_type > 13)
        {
            size_t str_size = (serial_type-13)/2;
            row_of_str.push_back(std::string(str_size,'*'));
        }
        else
            row_of_str.push_back(std::string());
    }
    
    payload += header_size;
    for (auto &str: row_of_str)
    {
    	str = std::string((char*)payload, str.length());
        payload += str.length();
    }
    row_of_str[0] = std::to_string(rowid);
    table.rows.push_back(row_of_str);
}

void process_table_data_rec(Table &table, const unsigned char *page_data, int page_size, int page_number)
{
    if (page_number <= 0)
        return;
    const unsigned char *curr_page = page_data + (page_number - 1) * page_size;

    // Determine page type and header size
    // For pages after the first page, header starts at byte 0
    int page_header_size = 0;
    uint8_t page_type = curr_page[page_header_size];
    
    if (page_type == 0x0D) // PAGE_TYPE_BTREE_LEAF
    {
        // Get cell count (2 bytes at offset 3 in b-tree header)
        int cell_count = (curr_page[3] << 8) | curr_page[4];
        table.row_count += cell_count;

        int cell_ptr_offset = 8;
        for (int i = 0; i < cell_count; ++i)
        {
            unsigned short cell_content_offset = curr_page[cell_ptr_offset + i * 2] << 8 |
                                                 (unsigned char) curr_page[cell_ptr_offset + i * 2 + 1];

            parse_cell_data(table, curr_page+cell_content_offset);
        }
    }
    else if (page_type == 0x02 or page_type == 0x05) // PAGE_TYPE_BTREE_INTERIOR
    {
        // Interior page - follow all child pointers
        int cell_count = (curr_page[page_header_size + 3] << 8) | curr_page[page_header_size + 4];
        
        // First follow the rightmost pointer
        int rightmost_child = (curr_page[page_header_size + 8] << 24) |
                              (curr_page[page_header_size + 9] << 16) |
                              (curr_page[page_header_size + 10] << 8) |
                              curr_page[page_header_size + 11];
        
        process_table_data_rec(table, page_data, page_size, rightmost_child);
        
        // Cell pointer array starts at offset 12 in interior page header
        int cell_pointer_array = page_header_size + 12;
        
        // Follow each interior cell pointer
        for (int i = 0; i < cell_count; i++) {
            int cell_offset = (curr_page[cell_pointer_array + i*2] << 8) | 
                              curr_page[cell_pointer_array + i*2 + 1];
            
            // First 4 bytes of an interior cell is the left child pointer
            int child_page = (curr_page[cell_offset] << 24) |
                             (curr_page[cell_offset + 1] << 16) |
                             (curr_page[cell_offset + 2] << 8) |
                             curr_page[cell_offset + 3];

            process_table_data_rec(table, page_data, page_size, child_page);
        }
    }
    else
    {
        printf("Unknown page type: %d\n", page_type);
    }
    
}


std::map<std::string, Table> get_tables(std::ifstream &database_file)
{
    unsigned short page_size = get_page_size(database_file);
    unsigned char *buffer = (unsigned char *) malloc(4096*1000);
    // Get file size
    database_file.seekg(0, std::ios::end);
    size_t size = database_file.tellg();
    database_file.seekg(0, std::ios::beg);
    assert(size < 4096*1000);
    
    database_file.read((char *)buffer,size);

    database_file.seekg(103);  // cell count
    char cell_cnt_buf[2];
    database_file.read(cell_cnt_buf, 2);
    unsigned short cell_count = cell_cnt_buf[0] << 8 | (unsigned char)cell_cnt_buf[1];

    int cell_ptr_offset = 108; // end of database(b-tree) header
    std::map<std::string, Table> res;
    for (int i = 0; i < cell_count; ++i)
    {
        unsigned short cell_content_offset = buffer[cell_ptr_offset + i * 2] << 8 | (unsigned char) buffer[cell_ptr_offset + i * 2 + 1];
        auto table = get_tbl_info(buffer+cell_content_offset);
        if (not table.name.empty())
        {
            if (table.rootpage > 0)
            {
                process_table_data_rec(table, buffer, page_size, table.rootpage);
            }
            res[table.name] = table;
        }
            
    }
    free(buffer);
    return(res);
}

auto trim_white_space = [](auto &&range)
{
    std::string s(range.begin(), range.end());
    int beg = s.find_first_not_of(" \n\t");
    auto size = s.find_first_of(" \n\t", beg)-beg;
    return s.substr(beg, size);
};

int main(int argc, char* argv[]) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    std::cerr << "Logs from your program will appear here" << std::endl;

    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    std::string database_file_path = argv[1];
    std::string command = argv[2];
    for (auto &c: command)
    {
        if (c == '\'')
            break;
        c = std::tolower( c);
    }

    std::ifstream database_file(database_file_path, std::ios::binary);
    if (!database_file) 
    {
        std::cerr << "Failed to open the database file" << std::endl;
        return 1;
    }

    if (command == ".dbinfo")
    {
        std::cout << "database page size: " << get_page_size(database_file) << std::endl;
        std::cout << "number of tables: " << get_tables(database_file).size() << '\n';
    }
    else if (command == ".tables")
    {
        auto tables = get_tables(database_file);
        for (auto [name, table]: tables)
        {
        	std::cout << name << ' ';
        }
    }
    else if (command.starts_with("select count(*) from"))
    {
        auto tables = get_tables(database_file);
        auto table_name = command.substr(command.find_last_of(' ') + 1, INT_MAX);
        
        std::cout << tables[table_name].row_count << '\n';
        
    }
    else if (command.starts_with("select "))
    {
        auto table_name_beg = command.find("from ") + strlen("from ");
        auto table_name_end = command.find_first_of(" \n", table_name_beg+1);
        auto table_name = command.substr(table_name_beg, table_name_end-table_name_beg);
        auto table = get_tables(database_file)[table_name];
        assert(not table.name.empty());

        int beg = strlen("select "), str_len = command.find(" from")-beg;
        auto search_cols_str = command.substr(beg, str_len);
        auto search_cols = search_cols_str | std::views::split(',') |
                           std::views::transform(trim_white_space) |
                           std::ranges::to<std::vector<std::string>>();

        auto every_col_str = table.sql.substr(table.sql.find_first_of('(') + 1, INT_MAX);

        auto every_col = every_col_str | std::views::split(',') |
                    std::views::transform(trim_white_space) |
                    std::ranges::to<std::vector<std::string>>();

        std::vector<int> col_indexes;
        for (auto search_col_name : search_cols)
        {
            auto it = std::find(every_col.begin(), every_col.end(), search_col_name);
            if (it != every_col.end())
                col_indexes.push_back(std::distance(every_col.begin(), it));
        }

        bool filter = false;
        int filter_col_idx = 0;
        std::string filter_str;
        auto where = command.find("where ");
        if (where != -1)
        {
        	filter = true;
            int beg = where + strlen("where ");
            int end = command.find_first_of(" ", beg);
            std::string filter_col_name = command.substr(beg, end-beg);
            auto it = std::find(every_col.begin(), every_col.end(), filter_col_name);
            if (it != every_col.end())
                filter_col_idx = std::distance(every_col.begin(), it);

            beg = command.find_first_of("'", end)+1;
            end = command.find_first_of("'", beg+1);
            filter_str = command.substr(beg, end-beg);
        }

        for (auto row: table.rows)
        {
            if (filter and row[filter_col_idx] != filter_str)
                continue;
            int i = 0;
            for (; (i+1) < col_indexes.size(); i++)
            	std::cout << row[col_indexes[i]] << '|';
            
            std::cout << row[col_indexes[i]] << '\n';
        }
    }
    

    return 0;
}
