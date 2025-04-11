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

// Parse a cell in a leaf table page to extract table names
std::string get_tbl_name(const unsigned char *cell)
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
    // Extract type (column 0) and name (column 1)  tbl_name (column 2)
    for (int i = 0; i < column_count && i < 3; i++)
    {
        uint64_t serial_type = column_types[i];
        
        // Handle TEXT columns (odd numbers >= 13)
        if (serial_type >= 13 && serial_type % 2 == 1)
        {
            uint64_t str_len = (serial_type - 13) / 2;
            if (i == 2) // sqlite_schema.tbl_name
                return std::string((char *)(payload + data_offset), str_len);
             
            data_offset += str_len;
        }
    }
    return "";
}

int get_page_size(std::ifstream &database_file)
{
    database_file.seekg(16); // Skip the first 16 bytes of the header
    char pg_sz_buffer[2];
    database_file.read(pg_sz_buffer, 2);
    unsigned short page_size = (unsigned short)pg_sz_buffer[1] | ((unsigned short)pg_sz_buffer[0] << 8);
    return page_size;
}

std::vector<std::string> get_table_names(std::ifstream &database_file)
{
    database_file.seekg(103);  // cell count
    char cell_cnt_buf[2];
    database_file.read(cell_cnt_buf, 2);
    unsigned short cell_count = cell_cnt_buf[0] << 8 | (unsigned char)cell_cnt_buf[1];

    unsigned short page_size = get_page_size(database_file);
    assert(page_size< 4096*10);
    unsigned char buffer[4096*10];
    database_file.seekg(0);
    database_file.read((char *)buffer, page_size);

    int offset = 108; // end of database(b-tree) header
    unsigned char num[2];
    std::vector<std::string> res;
    for (int i = 0; i < cell_count; ++i)
    {
        num[0] = buffer[offset++];
        num[1] = buffer[offset++];
    	unsigned short offset =  num[0]<<8 | (unsigned char)num[1];
        auto tbl_name = get_tbl_name(buffer+offset);
        if (not tbl_name.empty())
            res.push_back(tbl_name);
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
        for (auto &table: tables)
        {
        	std::cout << table << ' ';
        }
        int a = 3;
    }


    return 0;
}
