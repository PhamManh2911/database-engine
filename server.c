#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>

#define TABLE_MAX_PAGES 100
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNISED,
} MetaCommandResult;

typedef enum
{
    STATEMENT_SELECT,
    STATEMENT_INSERT,
} StatementType;

const uint32_t USERNAME_LENGTH = 32;
const uint32_t EMAIL_LENGTH = 255;

typedef struct
{
    int id;
    char username[USERNAME_LENGTH + 1];
    char email[EMAIL_LENGTH + 1];
} Record;

typedef struct
{
    StatementType type;
    Record record;
} Statement;

typedef struct
{
    // page's supposed to be big (maximum 4kb) so using pointer help reduce memory usage by allocating incrementally
    // using multiple pages instead so that we can allocate page incrementally in chunk, not the whole database in once?
    void *pages[TABLE_MAX_PAGES];
    u_int32_t file_length;
    int file_descriptor;
} Pager;

typedef struct
{
    int numberOfRecords;
    Pager *pager;
} Table;

typedef struct
{
    Table *table;
    uint32_t record_num;
    bool end_of_table;
} Cursor;

typedef enum
{
    EXTRACT_STATEMENT_SUCCESS,
    EXTRACT_STATEMENT_SYNTAX_ERROR,
    EXTRACT_STATEMENT_STRING_TOO_LONG,
    EXTRACT_STATEMENT_ID_IS_NEGATIVE,
    EXTRACT_STATEMENT_UNRECOGNISED,
} ExtractStatementStatus;

typedef enum
{
    EXECUTE_STATEMENT_SUCCESS,
    EXECUTE_STATEMENT_FULL_ROWS,
} ExecuteStatementResult;

const uint32_t PAGE_SIZE = 4096; // 4kb per page

const uint32_t RECORD_ID_SIZE = sizeof(int);
const uint32_t RECORD_ID_OFFSET = 0;

const uint32_t RECORD_USERNAME_SIZE = size_of_attribute(Record, username);
const uint32_t RECORD_USERNAME_OFFSET = RECORD_ID_SIZE;

const uint32_t RECORD_EMAIL_SIZE = size_of_attribute(Record, email);
const uint32_t RECORD_EMAIL_OFFSET = RECORD_ID_SIZE + RECORD_USERNAME_SIZE;

const uint32_t RECORD_SIZE = RECORD_ID_SIZE + RECORD_USERNAME_SIZE + RECORD_EMAIL_SIZE;
const uint32_t RECORD_PER_PAGE = PAGE_SIZE / RECORD_SIZE;
const uint32_t TABLE_MAX_RECORDS = TABLE_MAX_PAGES * RECORD_PER_PAGE;

Pager *open_pager(const char *database_file)
{

    int database_fd = open(database_file, O_RDWR | O_CREAT, 0644);

    if (database_fd == -1)
    {
        perror("Failure on opening database file");
        exit(EXIT_FAILURE);
    }
    Pager *pager = (Pager *)calloc(1, sizeof(Pager));
    off_t file_length = lseek(database_fd, 0, SEEK_END);

    if (file_length == -1)
    {
        perror("Failure on getting database file length");
        exit(EXIT_FAILURE);
    }
    pager->file_descriptor = database_fd;
    pager->file_length = file_length;

    return pager;
}

Table *open_database(const char *database_file)
{
    Table *table = (Table *)calloc(1, sizeof(Table));
    Pager *pager = open_pager(database_file);

    table->pager = pager;
    table->numberOfRecords = pager->file_length / RECORD_SIZE;

    return table;
}

Cursor *start_cursor(Table *table)
{
    Cursor *cursor = (Cursor *)calloc(1, sizeof(Cursor));

    cursor->table = table;
    cursor->record_num = 0;
    cursor->end_of_table = (table->numberOfRecords == 0);
    return cursor;
}

Cursor *end_cursor(Table *table)
{
    Cursor *cursor = (Cursor *)calloc(1, sizeof(Cursor));

    cursor->table = table;
    cursor->record_num = table->numberOfRecords;
    cursor->end_of_table = true;
    return cursor;
}

void cursor_advance(Cursor *cursor)
{
    cursor->record_num++;
    if (cursor->record_num == cursor->table->numberOfRecords)
    {
        cursor->end_of_table = true;
    }
}

void clear_input_buffer(char *buffer)
{
    free(buffer);
    buffer = NULL;
}

char *get_input()
{
    size_t baseCapacity = 10;
    char *inputBuffer = malloc(baseCapacity * sizeof(int));

    if (!inputBuffer)
    {
        perror("Malloc failed");
        exit(EXIT_FAILURE);
    }
    // c for capture user input each type user type.
    // how about when user delete
    int c;
    // current size of user input
    int currentSize = 0;

    while ((c = getchar()) != '\n' && c != EOF)
    {
        // leave at least one character for ending char (\n) checking.
        if (c + 1 >= baseCapacity)
        {
            baseCapacity *= 2;
            char *newBuffer = realloc(inputBuffer, baseCapacity * sizeof(int));

            if (!newBuffer)
            {
                perror("Realloc failed");
                clear_input_buffer(inputBuffer);
                exit(EXIT_FAILURE);
            }
            inputBuffer = newBuffer;
        }
        *(inputBuffer + currentSize) = (char)c;
        currentSize++;
    }
    *(inputBuffer + currentSize) = '\0';

    return inputBuffer;
}

void clear_table(Table *table)
{
    Pager *pager = table->pager;
    int numberOfPages = table->numberOfRecords / RECORD_PER_PAGE;

    if (table->numberOfRecords % RECORD_PER_PAGE > 0)
    {
        numberOfPages += 1;
    }
    // write to database file
    for (int i = 0; i < numberOfPages; i++)
    {
        if (pager->pages[i] == NULL)
        {
            continue;
        }
        uint32_t current_page_size;
        if (i == numberOfPages - 1 && table->numberOfRecords % RECORD_PER_PAGE > 0)
        {
            current_page_size = RECORD_SIZE * (table->numberOfRecords % RECORD_PER_PAGE);
        }
        else
        {
            current_page_size = RECORD_SIZE * RECORD_PER_PAGE;
        }
        lseek(pager->file_descriptor, i * PAGE_SIZE, SEEK_SET);
        ssize_t sizes_write = write(pager->file_descriptor, pager->pages[i], current_page_size);

        if (sizes_write == -1)
        {
            perror("Failed on writing content to database");
            exit(EXIT_FAILURE);
        }
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
    close(pager->file_descriptor);
    free(pager);
    pager = NULL;
    free(table);
    table = NULL;
}

MetaCommandResult check_meta_command(char *command)
{
    if (strcmp(command, ".exit") == 0)
    {
        return META_COMMAND_SUCCESS;
    }
    else
    {
        printf("Unrecognised meta command: '%s'\n", command);
        return META_COMMAND_UNRECOGNISED;
    }
}

ExtractStatementStatus parse_command(char *command, Statement *statement)
{
    if (strncmp(command, "insert", 6) == 0)
    {
        statement->type = STATEMENT_INSERT;
        char *keyword = strtok(command, " ");
        char *recordId = strtok(NULL, " ");
        char *recordUsername = strtok(NULL, " ");
        char *recordEmail = strtok(NULL, " ");

        if (recordId == NULL || recordUsername == NULL || recordEmail == NULL)
        {
            return EXTRACT_STATEMENT_UNRECOGNISED;
        }
        if (atoi(recordId) < 1)
        {
            return EXTRACT_STATEMENT_ID_IS_NEGATIVE;
        }

        if (strlen(recordUsername) > USERNAME_LENGTH || strlen(recordEmail) > EMAIL_LENGTH)
        {
            return EXTRACT_STATEMENT_STRING_TOO_LONG;
        }

        statement->record.id = atoi(recordId);
        strcpy(statement->record.username, recordUsername);
        strcpy(statement->record.email, recordEmail);
        return EXTRACT_STATEMENT_SUCCESS;
    }
    else if (strncmp(command, "select", 6) == 0)
    {
        statement->type = STATEMENT_SELECT;
        return EXTRACT_STATEMENT_SUCCESS;
    }
    return EXTRACT_STATEMENT_UNRECOGNISED;
}

void serialize_record(Record *record, void *position)
{
    memcpy((uint8_t *)position + RECORD_ID_OFFSET, &(record->id), RECORD_ID_SIZE);
    memcpy((uint8_t *)position + RECORD_USERNAME_OFFSET, record->username, RECORD_USERNAME_SIZE);
    memcpy((uint8_t *)position + RECORD_EMAIL_OFFSET, record->email, RECORD_EMAIL_SIZE);
}

void deserialize_record(Record *record, void *position)
{
    memcpy(&(record->id), (uint8_t *)position + RECORD_ID_OFFSET, RECORD_ID_SIZE);
    memcpy(record->username, (uint8_t *)position + RECORD_USERNAME_OFFSET, RECORD_USERNAME_SIZE);
    memcpy(record->email, (uint8_t *)position + RECORD_EMAIL_OFFSET, RECORD_EMAIL_SIZE);
}

void *get_page(Pager *pager, int num_page)
{
    if (pager->pages[num_page] == NULL)
    {
        pager->pages[num_page] = calloc(1, PAGE_SIZE);
    }
    // move the pointer to page position
    off_t offset = lseek(pager->file_descriptor, num_page * PAGE_SIZE, SEEK_SET);

    if (offset == -1)
    {
        perror("Failed on moving the pointer");
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_read = read(pager->file_descriptor, pager->pages[num_page], PAGE_SIZE);

    if (bytes_read == -1)
    {
        perror("Failed on reading content from database");
        exit(EXIT_FAILURE);
    }

    return pager->pages[num_page];
}

void *get_record_position(Cursor *cursor)
{
    u_int32_t record_num = cursor->record_num;
    uint32_t currentPage = record_num / RECORD_PER_PAGE;
    void *page = get_page(cursor->table->pager, currentPage);
    uint32_t offset = record_num % RECORD_PER_PAGE;

    return (uint8_t *)page + offset * RECORD_SIZE;
}

ExecuteStatementResult execute_insert(Table *table, Statement *statement)
{
    if (table->numberOfRecords >= TABLE_MAX_RECORDS)
    {
        printf("Error: Maximum number of rows's exceeded!\n");
        return EXECUTE_STATEMENT_FULL_ROWS;
    }

    Cursor *cursor = end_cursor(table);
    // copy record from statement to database
    // remove padding as well
    serialize_record(&(statement->record), get_record_position(cursor));

    table->numberOfRecords++;
    free(cursor);
    return EXECUTE_STATEMENT_SUCCESS;
}

ExecuteStatementResult execute_select(Table *table, Statement *statement)
{
    Cursor *cursor = start_cursor(table);
    Record record;
    while (!(cursor->end_of_table))
    {
        deserialize_record(&record, get_record_position(cursor));
        printf("(%i, %s, %s)\n", record.id, record.username, record.email);
        cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_STATEMENT_SUCCESS;
}

ExecuteStatementResult execute_statement(Table *table, Statement *statement)
{
    switch (statement->type)
    {
    case STATEMENT_INSERT:
        return execute_insert(table, statement);
    case STATEMENT_SELECT:
        execute_select(table, statement);
        return EXECUTE_STATEMENT_SUCCESS;
    }
}

int main(int argc, char const *argv[])
{
    const char *database_file = argv[1];
    Table *table = open_database(database_file);
    while (true)
    {
        // read data from input and load to input buffer
        printf("db > ");

        // Layer 1: Interface
        char *userInput = get_input();

        // processing input data
        // Check for meta commands that start with '.'
        if (userInput[0] == '.')
        {
            switch (check_meta_command(userInput))
            {
            case META_COMMAND_SUCCESS:
                clear_input_buffer(userInput);
                clear_table(table);
                exit(EXIT_SUCCESS);
            case META_COMMAND_UNRECOGNISED:
                clear_input_buffer(userInput);
                continue;
            }
        }
        // Layer 2: Command processor
        Statement statement;
        switch (parse_command(userInput, &statement))
        {
        case EXTRACT_STATEMENT_SUCCESS:
            break;
        case EXTRACT_STATEMENT_SYNTAX_ERROR:
            printf("Syntax error. Could not parse statement: '%s'\n", userInput);
            clear_input_buffer(userInput);
            continue;
        case EXTRACT_STATEMENT_UNRECOGNISED:
            printf("Unrecognized keyword at start of '%s'.\n", userInput);
            clear_input_buffer(userInput);
            continue;
        case EXTRACT_STATEMENT_ID_IS_NEGATIVE:
            printf("ID must be positive.\n");
            clear_input_buffer(userInput);
            continue;
        case EXTRACT_STATEMENT_STRING_TOO_LONG:
            printf("String is too long.\n");
            clear_input_buffer(userInput);
            continue;
        }
        // Layer 3: Virtual Machine
        switch (execute_statement(table, &statement))
        {
        case EXECUTE_STATEMENT_SUCCESS:
            printf("Executed.\n");
            break;
        case EXECUTE_STATEMENT_FULL_ROWS:
            // free memory address after using it
            clear_input_buffer(userInput);
            break;
        }
    }

    exit(EXIT_SUCCESS);
}
