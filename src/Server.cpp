#include <cstring>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <cassert>

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

struct table_info
{
    std::string name;
    std::string sql;
    int rootpage;
    uint64_t row_count;
};

// Parse a cell in a leaf table page to extract table names
table_info get_tbl_info(const unsigned char *cell)
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
    table_info res = {};
    int root_page = -1;
    for (int i = 0; i < 4; i++)
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
                data_offset += 1;
            } else if (serial_type == 2)
            {
                data_offset += 2;
                root_page = ((int) payload[data_offset] << 8) | payload[data_offset + 1];
            } else if (serial_type == 3)
            {
                data_offset += 3;
                root_page = ((int) payload[data_offset] << 16) |
                            ((int) payload[data_offset + 1] << 8) |
                            payload[data_offset + 2];
            } else if (serial_type == 4)
            {
                data_offset += 4;
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

// Count rows by traversing a B-tree
int count_rows_recursive(const unsigned char *page_data, int page_size, int page_number)
{
    if (page_number <= 0)
        return 0;
    const unsigned char *curr_page = page_data + (page_number - 1) * page_size;

    // Determine page type and header size
    // For pages after the first page, header starts at byte 0
    int page_header_size = 0;
    uint8_t page_type = curr_page[page_header_size];
    
    int row_count = 0;

    if (page_type == 0x0D) // PAGE_TYPE_BTREE_LEAF
    {
        // Get cell count (2 bytes at offset 3 in b-tree header)
        row_count = (curr_page[3] << 8) | curr_page[4];
    }
    else if (page_type == 0x02) // PAGE_TYPE_BTREE_INTERIOR
    {
        // Interior page - follow all child pointers
        int cell_count = (curr_page[page_header_size + 3] << 8) | curr_page[page_header_size + 4];
        
        // First follow the rightmost pointer
        int rightmost_child = (curr_page[page_header_size + 8] << 24) |
                              (curr_page[page_header_size + 9] << 16) |
                              (curr_page[page_header_size + 10] << 8) |
                              curr_page[page_header_size + 11];
        
        row_count += count_rows_recursive(page_data, page_size, rightmost_child);
        
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

            row_count += count_rows_recursive(page_data, page_size, child_page);
        }
    }
    else
    {
        printf("Unknown page type: %d\n", page_type);
    }
    return row_count;
}


std::vector<table_info> get_table_names(std::ifstream &database_file)
{
    unsigned short page_size = get_page_size(database_file);
    assert(page_size< 4096*10);
    unsigned char buffer[4096*10];
    // Get file size
    database_file.seekg(0, std::ios::end);
    size_t size = database_file.tellg();
    database_file.seekg(0, std::ios::beg);
    
    database_file.read((char *)buffer,size);

    database_file.seekg(103);  // cell count
    char cell_cnt_buf[2];
    database_file.read(cell_cnt_buf, 2);
    unsigned short cell_count = cell_cnt_buf[0] << 8 | (unsigned char)cell_cnt_buf[1];

    int cell_ptr_offset = 108; // end of database(b-tree) header
    unsigned char num[2];
    std::vector<table_info> res;
    for (int i = 0; i < cell_count; ++i)
    {
        num[0] = buffer[cell_ptr_offset++];
        num[1] = buffer[cell_ptr_offset++];
    	unsigned short cell_content_offset =  num[0]<<8 | (unsigned char)num[1];
        auto tbl = get_tbl_info(buffer+cell_content_offset);
        if (not tbl.name.empty())
        {
            if (tbl.rootpage > 0)
            {
                tbl.row_count = count_rows_recursive(buffer, page_size, tbl.rootpage);
            }
            res.push_back(tbl);
        }
            
    }
    return(res);
}

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

    std::ifstream database_file(database_file_path, std::ios::binary);
    if (!database_file) 
    {
        std::cerr << "Failed to open the database file" << std::endl;
        return 1;
    }

    if (command == ".dbinfo")
    {
        std::cout << "database page size: " << get_page_size(database_file) << std::endl;
        std::cout << "number of tables: " << get_table_names(database_file).size() << '\n';
    }
    else if (command == ".tables")
    {
        auto tables = get_table_names(database_file);
        for (auto table: tables)
        {
        	std::cout << table.name << ' ';
        }
    }
    else if (command.starts_with("select count(*) from"))
    {
        auto tables = get_table_names(database_file);
        auto table_name = command.substr(command.find_last_of(' ') + 1, INT_MAX);
        for (auto table: tables)
        {
        	if (table.name == table_name)
        	{
        		std::cout << table.row_count << '\n';
        	}
        }
        
    }


    return 0;
}
