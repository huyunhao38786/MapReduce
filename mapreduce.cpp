#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdlib> // For exit() and EXIT_FAILURE
#include <filesystem> // Requires C++17
#include <thread>
#include<bits/stdc++.h>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include<dirent.h>
#include <thread>
#include <sstream>
#include <algorithm>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <filesystem>


namespace fs = std::filesystem;

class Master {
private:
    std::string inputDir;
    std::string outputDir;
    int nWorkers;
    int nReduce;

    std::vector<std::thread> workers;
    std::vector<std::future<void>> mapFutures;

    size_t hashKey(const std::string& key) {
        std::hash<std::string> hash_fn;
        return hash_fn(key) % nReduce;
    }

    std::string processLine(const std::string& line) {
        std::string processed;
        for (char ch : line) {
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == ' ') {
                processed += std::tolower(ch);
            }
        }
        return processed;
    }

    void mapTask(const std::string& filePath, int mapTaskNumber, std::promise<void> promise) {
        std::vector<std::ofstream> outs(nReduce);
        for (int i = 0; i < nReduce; ++i) {
            outs[i].open(outputDir + "/map.part-" + std::to_string(mapTaskNumber) + "-" + std::to_string(i) + ".txt", std::ofstream::out | std::ofstream::app);
        }

        std::ifstream inFile(filePath);
        std::string line;
        while (std::getline(inFile, line)) {
            std::istringstream lineStream(line);
            std::string word;
            while (lineStream >> word) {
                size_t reducerIndex = hashKey(word);
                outs[reducerIndex] << word << ",1\n";
            }
        }

        for (auto& out : outs) {
            out.close();
        }

        promise.set_value();
    }

    void reduceTask(int reduceTaskNumber) {
        std::unordered_map<std::string, int> counts;
        for (int i = 0; i < nReduce; ++i) {
            std::ifstream inFile(outputDir + "/map.part-" + std::to_string(i) + "-" + std::to_string(reduceTaskNumber) + ".txt");
            std::string line;
            while (std::getline(inFile, line)) {
                std::istringstream lineStream(line);
                std::string word;
                int count;
                while (lineStream >> word >> count) {
                    counts[word] += count;
                }
            }
        }

        std::ofstream outFile(outputDir + "/reduce.part-" + std::to_string(reduceTaskNumber) + ".txt");
        for (const auto& [word, count] : counts) {
            outFile << word << "," << count << "\n";
        }
    }

    void mergeOutput() {
        std::ofstream finalOutput(outputDir + "/output.txt");
        for (int i = 0; i < nReduce; ++i) {
            std::ifstream reduceOutput(outputDir + "/reduce.part-" + std::to_string(i) + ".txt");
            finalOutput << reduceOutput.rdbuf();
        }
    }
public:
    Master(const std::string& inputDir, const std::string& outputDir, int nWorkers, int nReduce) : inputDir(inputDir), outputDir(outputDir), nWorkers(nWorkers), nReduce(nReduce) {}

    void run() {
        std::cout << "Initializing MapReduce job..." << std::endl;
        
        // Ensure input and output directories exist or can be created
        if (!fs::exists(inputDir) || !fs::is_directory(inputDir)) {
            std::cerr << "Input directory does not exist." << std::endl;
            return;
        }

        fs::create_directory(outputDir);

       // Schedule and execute map tasks
        int mapTaskNumber = 0;
        for (const auto& entry : fs::directory_iterator(inputDir)) {
            if (entry.is_regular_file()) {
                std::promise<void> mapPromise;
                mapFutures.push_back(mapPromise.get_future());
                std::thread([this, path = entry.path().string(), mapTaskNumber, p = std::move(mapPromise)]() mutable {
                    this->mapTask(path, mapTaskNumber, std::move(p));
                }).detach();
                ++mapTaskNumber;
            }
        }

        // Wait for all map tasks to complete
        for (auto& future : mapFutures) {
            future.get();
        }

        // Execute reduce tasks
        std::vector<std::thread> reduceThreads;
        for (int i = 0; i < nReduce; ++i) {
            reduceThreads.emplace_back([this, i]() {
                this->reduceTask(i);
            });
        }

        // Wait for all reduce tasks to complete
        for (auto& thread : reduceThreads) {
            thread.join();
        }

        mergeOutput();

        std::cout << "MapReduce job completed successfully." << std::endl;
    }
};

// Function to display usage information
void showUsage(const std::string& programName) {
    std::cerr << "Usage: " << programName << " --input <inputdir> --output <outputdir> --nworkers <nWorkers> --nreduce <nReduce>\n";
}

int main(int argc, char** argv) {
    if (argc != 9) { // Expecting 8 arguments plus the program name
        showUsage(argv[0]);
        return 1;
    }

    std::string inputDir, outputDir;
    int nWorkers = 0, nReduce = 0;

    // Basic command-line argument parsing
    for (int i = 1; i < argc; i += 2) {
        std::string arg = argv[i];
        if (arg == "--input") {
            inputDir = argv[i + 1];
        } else if (arg == "--output") {
            outputDir = argv[i + 1];
        } else if (arg == "--nworkers") {
            nWorkers = std::stoi(argv[i + 1]);
        } else if (arg == "--nreduce") {
            nReduce = std::stoi(argv[i + 1]);
        } else {
            showUsage(argv[0]);
            std::exit(EXIT_FAILURE);
        }
    }

    // Ensure all parameters were set
    if (inputDir.empty() || outputDir.empty() || nWorkers <= 0 || nReduce <= 0) {
        showUsage(argv[0]);
        std::exit(EXIT_FAILURE);
    }

    // Initialize and start the MapReduce process
    Master master(inputDir, outputDir, nWorkers, nReduce);
    master.run();

    return 0;
}
