#ifndef LOGGER_H
#define LOGGER_H

#include <fstream>
#include <iostream>
#include <string>
#include <streambuf>

class Logger {
public:
    // Constructor: opens the log file and redirects std::cout
    Logger(const std::string& log_filename) {
        log_stream.open(log_filename);
        if (log_stream.is_open()) {
            original_cout_buf = std::cout.rdbuf(); // Save original cout buffer
            std::cout.rdbuf(log_stream.rdbuf());   // Redirect cout to log_stream
        } else {
            std::cerr << "Error: Could not open log file " << log_filename << std::endl;
            original_cout_buf = nullptr;
        }
    }

    // Destructor: restores std::cout and closes the log file
    ~Logger() {
        if (original_cout_buf) {
            std::cout.rdbuf(original_cout_buf); // Restore original cout buffer
        }
        if (log_stream.is_open()) {
            log_stream.close(); // Close the file stream
        }
    }

    // Delete copy constructor and copy assignment operator to prevent issues
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    // Optionally, allow move constructor and move assignment if needed,
    // but for simple RAII, deleting copy operations is often sufficient.

private:
    std::ofstream log_stream;
    std::streambuf* original_cout_buf;
};

#endif // LOGGER_H