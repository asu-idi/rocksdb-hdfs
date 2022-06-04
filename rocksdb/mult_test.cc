#include <iostream>
#include <string>
#include <thread>

void print_message_function(const std::string& msg);

int main()
{
    std::string message1 = "Thread 1";
    std::string message2 = "Thread 2";

    std::thread thread1(print_message_function, std::cref(message1));
    std::thread thread2(print_message_function, std::cref(message2));

    thread1.join();
    thread2.join();
}

void print_message_function(const std::string& msg)
{
    std::cout << msg << std::endl;
}