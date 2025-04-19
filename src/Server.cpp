#include <cstring>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <cassert>
#include <map>
#include <ranges>
#include <string_view>

#define INTERIOR_INDEX 0x02
#define INTERIOR_TABLE 0x05
#define LEAF_INDEX 0x0a
#define LEAF_TABLE 0x0D

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

struct Schema_Table_Info
{
    std::string type;
    std::string name;
    std::string tbl_name;
    std::string sql;
    int rootpage{};
    uint64_t row_count = 0;
    std::vector<uint64_t> cells;
    std::vector<int> index_rowids;
};

std::pair<int,int> parse_int_and_consumed_bytes(uint64_t serial_type, const unsigned char *payload)
{
    if (serial_type == 0 or serial_type == 8 or serial_type == 9 or serial_type == 12 or serial_type == 13)
        return {0, 0};

    int res = -1;

    if (serial_type == 1)
    {
        res = (int)payload[0];
    }
    else if (serial_type == 2)
    {
        res = ((int)payload[0] << 8) | payload[0 + 1];
    }
    else if (serial_type == 3)
    {
        res = ((int)payload[0] << 16) |
              ((int)payload[0 + 1] << 8) |
              payload[0 + 2];
    }
    else if (serial_type == 4)
    {
        res = ((int)payload[0] << 24) |
              ((int)payload[0 + 1] << 16) |
              ((int)payload[0 + 2] << 8) |
              payload[0 + 3];
    }
    return {res, serial_type};
}

// Parse a cell in a leaf table page to extract table names
Schema_Table_Info get_tbl_info(const unsigned char *cell)
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

    while (header_offset < header_size)
    {
        uint64_t serial_type;
        header_offset += parse_varint(payload + header_offset, &serial_type);
        column_types[column_count++] = serial_type;
    }

    // Process column data
    uint64_t data_offset = header_size;

    // columns: 0 type | 1 name | 2 tbl_name | 3 rootpage | 4 sql
    Schema_Table_Info res = {};
    for (int i = 0; i < 5; i++)
    {
        uint64_t serial_type = column_types[i];

        // Handle TEXT columns (odd numbers >= 13)
        if (serial_type >= 13 && serial_type % 2 == 1)
        {
            const uint64_t str_len = (serial_type - 13) / 2;
            std::string str = std::string((char *)(payload + data_offset), str_len);
            for (char &c : str) c = std::tolower(c);
            if (i == 0)// sqlite_schema.type
                res.type = str;
            if (i == 1)// sqlite_schema.name
                res.name = str;
            if (i == 2)// sqlite_schema.tbl_name
                res.tbl_name = str;
            if (i == 4)// sqlite_schema.sql
                res.sql = str;

            data_offset += str_len;
        } else if (i == 3)// sqlite_schema.rootpage
        {
            auto [value, bytes] = parse_int_and_consumed_bytes(serial_type, payload+data_offset);
            res.rootpage = value;
            data_offset += bytes;
        }
    }
    return res;
}

inline int get_page_size(const unsigned char *buffer)
{
    return (buffer[16] << 8 | (unsigned short)buffer[17]);
}

void print_row(std::ifstream &file, uint64_t ptr, const std::vector<int>& col_indexes,
const bool filter, const int filter_col_idx, const std::string_view filter_str)
{
    unsigned char temp_buf[9];
    file.seekg(ptr);
    file.read((char*)temp_buf, 9);

    int offset = 0;
    uint64_t payload_size;
    offset += parse_varint(temp_buf, &payload_size);

    file.seekg(ptr+offset);
    file.read((char*)temp_buf, 9);

    uint64_t rowid;
    offset += parse_varint(temp_buf, &rowid);

    file.seekg(ptr+offset);
    unsigned char payload[1024];
    file.read((char*)payload, payload_size);
    const uint8_t header_size = payload[0];

    int header_offset = 1;  // Skip header size byte
    std::vector<std::string_view> row_of_str;
    int str_offset = 0;
    const unsigned char *content =  payload+header_size;
    while (header_offset < header_size)
    {
        uint64_t serial_type;
        header_offset += parse_varint(payload + header_offset, &serial_type);
        if (serial_type > 13)
        {
            size_t str_size = (serial_type-13)/2;
            row_of_str.emplace_back((char*)(content+str_offset), str_size);
            str_offset += str_size;
        } else
            row_of_str.emplace_back();
    }

    if (filter and row_of_str[filter_col_idx] != filter_str)
        return;

    const std::string rowid_str = std::to_string(rowid);
    row_of_str[0] = std::string_view(rowid_str);

    int i = 0;
    for (; (i + 1) < col_indexes.size(); i++)
        std::cout << row_of_str[col_indexes[i]] << '|';

    std::cout << row_of_str[col_indexes[i]] << '\n';
}


std::pair<std::string_view, int> parse_index_cell (const unsigned char *cell)
{
    uint64_t offset = 0, payload_size;
    offset += parse_varint(cell + offset, &payload_size);

    const unsigned char *payload = cell + offset;
    uint8_t header_size = payload[0];

    std::vector<int> column_types;
    int header_offset = 1;
    while (header_offset < header_size)
    {
        uint64_t serial_type;
        header_offset += parse_varint(payload + header_offset, &serial_type);
        column_types.push_back(serial_type);
    }
    payload += header_size;

    std::string_view country;
    int row_id = -1;

    for (int serial_type : column_types)
    {
        if (serial_type > 13)
        {
            // String type
            const uint64_t str_len = (serial_type - 13) / 2;
            country = std::string_view((const char *)payload, str_len);
            payload += str_len;
        }
        else if (serial_type < 12)
        {
            // Integer type
            auto [num, consumed] = parse_int_and_consumed_bytes(serial_type, payload);
            payload += consumed;
            row_id = num;
        }
    }

    uint64_t content_size = payload - (cell + offset);
    if (content_size < payload_size)
    {
        assert(0);
        int overflow_page = (payload[0] << 24) |
                            (payload[1] << 16) | (payload[2] << 8) | payload[3];

        if (overflow_page > 0)
        {
            // todo: Process overflow data
        }
    }

    return {country, row_id};
}

void scan_index_rec(Schema_Table_Info &table, std::ifstream &file,
                    const int page_size, const int page_number, const std::string_view search_country)
{
    if (page_number <= 0)
        return;

    unsigned char curr_page[4096];
    file.seekg((page_number - 1) * page_size);
    file.read((char*)(curr_page), 4096);

    uint8_t page_type = curr_page[0];

    if (page_type == LEAF_INDEX)
    {
        const int cell_count = (curr_page[3] << 8) | curr_page[4];
        constexpr int cell_ptr_offset = 8;

        for (int i = 0; i < cell_count; ++i)
        {
            const unsigned short cell_offset =
                (curr_page[cell_ptr_offset + i * 2] << 8) |
                (unsigned char)curr_page[cell_ptr_offset + i * 2 + 1];

            auto [country, row_id] = parse_index_cell(curr_page + cell_offset);

            if (search_country == country)
            {
                table.index_rowids.emplace_back(row_id);
            }
        }
    }
    else if (page_type == INTERIOR_INDEX)
    {
        int cell_count = (curr_page[3] << 8) | curr_page[4];
        int cell_pointer_array = 12;

        for (int i = 0; i < cell_count; i++)
        {
            int cell_offset = (curr_page[cell_pointer_array + i * 2] << 8) |
                              curr_page[cell_pointer_array + i * 2 + 1];

            int child_page = (curr_page[cell_offset] << 24) | (curr_page[cell_offset + 1] << 16) |
                             (curr_page[cell_offset + 2] << 8) | curr_page[cell_offset + 3];

            auto [country, row_id] = parse_index_cell(curr_page + cell_offset + 4);

            int comparison = search_country.compare(country);

            if (comparison < 0)
            {
                scan_index_rec(table, file, page_size, child_page, search_country);
                return;
            }
            else if (comparison == 0)
            {
                scan_index_rec(table, file, page_size, child_page, search_country);
                table.index_rowids.emplace_back(row_id);
            }
        }

        // If key is larger than everything in current page, follow rightmost child
        int rightmost_child = (curr_page[8] << 24) | (curr_page[9] << 16) |
                              (curr_page[10] << 8) | curr_page[11];
        scan_index_rec(table, file, page_size, rightmost_child, search_country);
    }
    else
    {
        printf("Unknown page type: %d\n", page_type);
    }
}



void scan_table_rec(Schema_Table_Info &table, std::ifstream &file,
    const int page_size, const int page_number, int search_row_id = 0)
{
    if (page_number <= 0)
        return;

    unsigned char curr_page[4096];
    file.seekg((page_number - 1) * page_size);
    file.read((char*)curr_page, 4096);

    uint8_t page_type = curr_page[0];

    if (page_type == LEAF_TABLE)
    {
        int cell_count = (curr_page[3] << 8) | curr_page[4];
        table.row_count += cell_count;

        for (int i = 0; i < cell_count; ++i)
        {
            unsigned short cell_offset =
                (curr_page[8 + i * 2] << 8) |
                (unsigned char)curr_page[8 + i * 2 + 1];

            const unsigned char *cell = curr_page + cell_offset;

            if (!search_row_id)
            {
                table.cells.emplace_back((page_number - 1) * page_size + cell_offset);
                continue;
            }

            int offset = 0;
            uint64_t payload_size;
            offset += parse_varint(cell + offset, &payload_size);

            uint64_t rowid;
            parse_varint(cell + offset, &rowid);

            if (rowid == search_row_id)
            {
                table.cells.emplace_back((page_number - 1) * page_size + cell_offset);
            }
        }
    }
    else if (page_type == INTERIOR_TABLE)
    {
        int cell_count = (curr_page[3] << 8) | curr_page[4];

        for (int i = 0; i < cell_count; i++)
        {
            int cell_offset = (curr_page[12 + i * 2] << 8) |
                              curr_page[12 + i * 2 + 1];

            int child_page = (curr_page[cell_offset] << 24) |
                             (curr_page[cell_offset + 1] << 16) |
                             (curr_page[cell_offset + 2] << 8) |
                             curr_page[cell_offset + 3];

            if (!search_row_id)
            {
                scan_table_rec(table, file, page_size, child_page, 0);
                continue;
            }

            uint64_t key;
            parse_varint(curr_page + cell_offset + 4, &key);

            if (search_row_id <= key)
            {
                scan_table_rec(table, file, page_size, child_page, search_row_id);
                return;
            }
        }

        int rightmost_child = (curr_page[8] << 24) |
                              (curr_page[9] << 16) |
                              (curr_page[10] << 8) |
                              curr_page[11];
        scan_table_rec(table, file, page_size, rightmost_child, search_row_id);
    }
    else
    {
        printf("Unknown page type: %d\n", page_type);
    }
}

std::map<std::string, Schema_Table_Info> get_tables(const unsigned char *buffer)
{
    int page_size = get_page_size(buffer);
    unsigned short cell_count = buffer[103] << 8 | (unsigned char)buffer[104];
    std::map<std::string, Schema_Table_Info> res;
    for (int i = 0; i < cell_count; ++i)
    {
        constexpr int cell_ptr_offset = 108;
        const unsigned short cell_content_offset =
            buffer[cell_ptr_offset + i * 2] << 8 |
            (unsigned char)buffer[cell_ptr_offset + i * 2 + 1];
        auto table = get_tbl_info(buffer+cell_content_offset);
        res[table.name] = table;
    }
    return(res);
}

auto trim_white_space = [](auto &&range)
{
    std::string s(range.begin(), range.end());
    int beg = s.find_first_not_of(" \n\t");
    auto size = s.find_first_of(" \n\t", beg)-beg;
    return s.substr(beg, size);
};

int main(int argc, char* argv[])
{
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

    unsigned char page[2];
    database_file.seekg(16);
    database_file.read((char*)page, 2);
    int page_size = (page[0] << 8 | (unsigned short) page[1]);
    database_file.seekg(0);
    auto *buffer = (unsigned char *) malloc(page_size);
    database_file.read((char *)buffer,page_size);


    if (command == ".dbinfo")
    {
        std::cout << "database page size: " << page_size << std::endl;
        std::cout << "number of tables: " << get_tables(buffer).size() << '\n';
    }
    else if (command == ".tables")
    {
        auto tables = get_tables(buffer);
        for (const auto &name: tables | std::views::keys)
        {
        	std::cout << name << ' ';
        }
    }
    else if (command.starts_with("select count(*) from"))
    {
        auto tables = get_tables(buffer);
        auto table_name = command.substr(command.find_last_of(' ') + 1, INT_MAX);
        auto table = tables[table_name];
        scan_table_rec(table, database_file, page_size, table.rootpage);
        std::cout << table.row_count << '\n';

    }
    else if (command.starts_with("select "))
    {
        // Parse table name
        auto table_name_beg = command.find("from ") + 5;
        auto table_name_end = command.find_first_of(" \n", table_name_beg);
        auto table_name = command.substr(table_name_beg, table_name_end - table_name_beg);

        // Get table info
        auto tables_and_indexes = get_tables(buffer);
        auto &table = tables_and_indexes[table_name];
        assert(!table.name.empty());

        // Parse selected columns
        auto search_cols_str = command.substr(7, command.find(" from") - 7);
        auto search_cols = search_cols_str | std::views::split(',') |
                           std::views::transform(trim_white_space) |
                           std::ranges::to<std::vector<std::string>>();

        // Parse table schema
        auto every_col_str = table.sql.substr(table.sql.find_first_of('(') + 1);
        auto every_col = every_col_str | std::views::split(',') |
                         std::views::transform(trim_white_space) |
                         std::ranges::to<std::vector<std::string>>();

        // Map selected columns to indexes
        std::vector<int> col_indexes;
        for (const auto &search_col_name : search_cols)
        {
            auto it = std::ranges::find(every_col, search_col_name);
            if (it != every_col.end())
                col_indexes.push_back(std::distance(every_col.begin(), it));
        }

        // Parse WHERE clause if present
        bool filter = false;
        int filter_col_idx = 0;
        std::string filter_str;
        if (auto where_pos = command.find("where "); where_pos != std::string::npos)
        {
            filter = true;

            // Extract filter column
            auto col_beg = where_pos + 6;
            auto col_end = command.find_first_of(' ', col_beg);
            auto filter_col_name = command.substr(col_beg, col_end - col_beg);

            auto it = std::ranges::find(every_col, filter_col_name);
            if (it != every_col.end())
                filter_col_idx = std::distance(every_col.begin(), it);

            // Extract filter value
            auto val_beg = command.find_first_of('\'', col_end) + 1;
            auto val_end = command.find_first_of('\'', val_beg);
            filter_str = command.substr(val_beg, val_end - val_beg);
        }

        // Find and use index if available
        Schema_Table_Info index = {};
        for (auto &maybe_index : tables_and_indexes | std::views::values)
        {
            if (maybe_index.type == "index" && maybe_index.tbl_name == table_name)
            {
                index = maybe_index;
                scan_index_rec(index, database_file, page_size, index.rootpage, filter_str);
                break;
            }
        }

        // Scan table based on index or full scan
        if (index.name.empty())
        {
            scan_table_rec(table, database_file, page_size, table.rootpage);
        }
        else
        {
            for (auto row_id : index.index_rowids)
            {
                scan_table_rec(table, database_file, page_size, table.rootpage, row_id);
            }
        }

        // Print results
        for (auto cell : table.cells)
        {
            print_row(database_file, cell, col_indexes, filter, filter_col_idx, filter_str);
        }
    }

    return 0;
}
