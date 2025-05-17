#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNISED,
} MetaCommandResult;

typedef enum {
    STATEMENT_SELECT,
    STATEMENT_INSERT,
    STATEMENT_NOT_FOUND,
} StatementType;

typedef struct {
    StatementType type;
} Statement;

typedef enum {
    SUCCESS_EXTRACT_STATEMENT,
    UNSUCCESS_EXTRACT_STATEMENT,
} ExtractStatementStatus;

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

void clearInputBuffer(char *buffer)
{
    free(buffer);
    buffer = NULL;
}

MetaCommandResult check_meta_command(char *command) {
    if (strcmp(command, ".exit") == 0) {
        return META_COMMAND_SUCCESS;
    } else {
        printf("Unrecognised meta command: '%s'\n", command);
        return META_COMMAND_UNRECOGNISED;
    }
}

ExtractStatementStatus extract_statement_from_command(char *command, Statement *statement) {
    if (strncmp(command, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        return SUCCESS_EXTRACT_STATEMENT;
    } else if (strncmp(command, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return SUCCESS_EXTRACT_STATEMENT;
    }
    return UNSUCCESS_EXTRACT_STATEMENT;
}

void process_statement(Statement *statement) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            printf("Your query is insert something.\n");
            break;
        case STATEMENT_SELECT:
            printf("Your query is select something.\n");
            break;
        case STATEMENT_NOT_FOUND:
            printf("Unrecognised statement.\n");
            break;
    }
}

int main(int argc, char const *argv[])
{
    printf("Server's listening!\n");
    while (true)
    {
        // read data from input and load to input buffer
        printf("db > ");

        char *userInput = get_input();

        // processing input data
        // Check for meta commands that start with '.'
        if (userInput[0] == '.') {
            switch (check_meta_command(userInput)) {
                case META_COMMAND_SUCCESS:
                    clearInputBuffer(userInput);
                    exit(EXIT_SUCCESS);
                case META_COMMAND_UNRECOGNISED:
                    clearInputBuffer(userInput);
                    continue;
            }
        }
        // extract statement tokens
        Statement statement;
        switch (extract_statement_from_command(userInput, &statement)) {
            case SUCCESS_EXTRACT_STATEMENT:
                break;
            case UNSUCCESS_EXTRACT_STATEMENT:
                printf("Unrecognized keyword at start of '%s'.\n", userInput);       
                clearInputBuffer(userInput);
                continue;
        }
        // some processes are done
        process_statement(&statement);

        // free memory address after using it
        clearInputBuffer(userInput);
    }

    exit(EXIT_SUCCESS);
}
