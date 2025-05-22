#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define TABLE_MAX_PAGES 100

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

typedef struct
{
    int id;
    char username[30];
    char email[100];
} Record;

typedef struct
{
    StatementType type;
    Record record;
} Statement;

typedef struct
{
    int numberOfRecords;
    // page's supposed to be big (maximum 4kb) so using pointer help reduce memory usage by allocating incrementally
    void *pages[TABLE_MAX_PAGES];
} Table;

typedef enum
{
    EXTRACT_STATEMENT_SUCCESS,
    EXTRACT_STATEMENT_SYNTAX_ERROR,
    EXTRACT_STATEMENT_UNRECOGNISED,
} ExtractStatementStatus;

typedef enum
{
    EXECUTE_STATEMENT_SUCCESS,
    EXECUTE_STATEMENT_FULL_ROWS,
} ExecuteStatementResult;

const uint32_t PAGE_SIZE = 4096; // 4kb per page
const uint32_t RECORD_SIZE = sizeof(Record);
const uint32_t RECORD_PER_PAGE = PAGE_SIZE / RECORD_SIZE;
const uint32_t TABLE_MAX_RECORDS = TABLE_MAX_PAGES * RECORD_PER_PAGE;
const uint32_t RECORD_ID_SIZE = sizeof(int);
const uint32_t RECORD_ID_OFFSET = 0;
const uint32_t RECORD_USERNAME_SIZE = sizeof(char[30]);
const uint32_t RECORD_USERNAME_OFFSET = RECORD_ID_SIZE;
const uint32_t RECORD_EMAIL_SIZE = sizeof(char[100]);
const uint32_t RECORD_EMAIL_OFFSET = RECORD_ID_SIZE + RECORD_USERNAME_SIZE;

Table *get_table()
{
    Table *table = (Table *)calloc(1, sizeof(Table));

    return table;
}

char *get_input()
{
    size_t baseCapacity = 10;
    char *inputBuffer = malloc(baseCapacity * sizeof(int));

    if (!inputBuffer)
    {
        perror("Malloc failed!");
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
                perror("Realloc failed!");
                free(inputBuffer);
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

void clear_input_buffer(char *buffer)
{
    free(buffer);
    buffer = NULL;
}

void clear_table(Table *table)
{
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        free(table->pages[i]);
        table->pages[i] = NULL;
    }
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

ExtractStatementStatus extract_statement_from_command(char *command, Statement *statement)
{
    if (strncmp(command, "insert", 6) == 0)
    {
        statement->type = STATEMENT_INSERT;
        int num_args = sscanf(command, "insert %i %s %s", &(statement->record.id), statement->record.username, statement->record.email);

        if (num_args != 3)
        {
            return EXTRACT_STATEMENT_SYNTAX_ERROR;
        }
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

ExecuteStatementResult execute_insert(Table *table, Statement *statement)
{
    if (table->numberOfRecords >= TABLE_MAX_RECORDS)
    {
        printf("Maximum number of rows's exceeded!");
        return EXECUTE_STATEMENT_FULL_ROWS;
    }

    // copy record from statement to database
    // remove padding as well
    uint32_t currentPage = table->numberOfRecords / RECORD_PER_PAGE;
    uint32_t offset = table->numberOfRecords % RECORD_PER_PAGE;
    if (table->pages[currentPage] == NULL)
    {
        table->pages[currentPage] = calloc(1, PAGE_SIZE);
    }
    serialize_record(&(statement->record), (Record *)(table->pages[currentPage]) + offset);

    table->numberOfRecords++;
    return EXECUTE_STATEMENT_SUCCESS;
}

ExecuteStatementResult execute_select(Table *table, Statement *statement)
{
    for (uint32_t page = 0; page <= table->numberOfRecords / RECORD_PER_PAGE; page++)
    {
        uint32_t maxRecords;
        bool isLastPage = page == table->numberOfRecords / RECORD_PER_PAGE;

        if (isLastPage)
        {
            maxRecords = table->numberOfRecords % RECORD_PER_PAGE;
        }
        else
        {
            maxRecords = RECORD_PER_PAGE;
        }
        for (Record *position = table->pages[page]; position < (Record *)(table->pages[page]) + maxRecords; position++)
        {
            Record record;

            deserialize_record(&record, position);
            printf("(%i, %s, %s)\n", record.id, record.username, record.email);
        }
    }
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
    printf("Server's listening!\n");

    Table *table = get_table();
    while (true)
    {
        // read data from input and load to input buffer
        printf("db > ");

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
        // extract statement tokens
        Statement statement;
        switch (extract_statement_from_command(userInput, &statement))
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
        }
        // some processes are done
        switch (execute_statement(table, &statement))
        {
        case EXECUTE_STATEMENT_SUCCESS:
            printf("Executed!\n");
            break;
        case EXECUTE_STATEMENT_FULL_ROWS:
            // free memory address after using it
            clear_input_buffer(userInput);
            break;
        }
    }

    exit(EXIT_SUCCESS);
}
