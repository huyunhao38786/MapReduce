#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdlib> // For exit() and EXIT_FAILURE
#include <filesystem> // Requires C++17
#include <thread>
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
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>


namespace fs = std::filesystem;

class Master {
private:
    std::string inputDir;
    std::string outputDir;
    int nWorkers;
    int nMap;
    int nReduce;

    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mtx;
    std::condition_variable condition;
    int nRemaining{0};
    bool stop = false;

    void worker() {
        while(true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(queue_mtx);
                condition.wait(lock, [this]{ return !tasks.empty() || stop; });
                if(stop && tasks.empty()) return;
                task = std::move(tasks.front());
                tasks.pop();
            }
            task();
            {
                std::unique_lock<std::mutex> lock(queue_mtx);
                --nRemaining;
                condition.notify_all();
            }
        }
    }

    size_t hashKey(const std::string& key) {
        std::hash<std::string> hash_fn;
        return hash_fn(key) % nReduce;
    }

    std::string processLine(const std::string& line) {
        std::string processed;
        for (char ch : line) {
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) {
                processed += std::tolower(ch);
            } else {
                if (!processed.empty() && processed.back() != ' ') {
                    processed += ' ';
                }
            }
        }
        if (!processed.empty() && processed.back() == ' ') {
            processed.pop_back();
        }
        std::cout << "processed: " << processed;
        return processed;
    }

    void mapTask(const std::string& filePath, int mapTaskNumber) {
        std::cout << "map task " << mapTaskNumber << " starts" << std::endl;
        std::vector<std::ofstream> outs(nReduce);
        for (int i = 0; i < nReduce; ++i) {
            outs[i].open(outputDir + "/map.part-" + std::to_string(mapTaskNumber) + "-" + std::to_string(i) + ".txt", std::ofstream::out | std::ofstream::app);
        }

        std::ifstream inFile(filePath);
        std::string line;
        while (std::getline(inFile, line)) {
            std::string processedLine = processLine(line);
            std::istringstream lineStream(processedLine);
            std::string word;
            while (lineStream >> word) {
                size_t reducerIndex = hashKey(word);
                outs[reducerIndex] << word << ",1\n";
            }
        }

        for (auto& out : outs) {
            out.close();
        }
        std::cout << "map task " << mapTaskNumber << " ends" << std::endl;
    }

    void reduceTask(int reduceTaskNumber) {
        std::cout << "reduce task " << reduceTaskNumber << " starts" << std::endl;
        std::unordered_map<std::string, int> counts;
        for (int i = 0; i < nMap; ++i) {
            std::ifstream inFile(outputDir + "/map.part-" + std::to_string(i) + "-" + std::to_string(reduceTaskNumber) + ".txt");
            std::string line;
            while (std::getline(inFile, line)) {
                std::istringstream lineStream(line);
                std::string word;
                int count;
                if (std::getline(lineStream, word, ',') && (lineStream >> count)) {
                    counts[word] += count;
                }
            }
        }

        std::ofstream outFile(outputDir + "/reduce.part-" + std::to_string(reduceTaskNumber) + ".txt");
        for (const auto& [word, count] : counts) {
            outFile << word << "," << count << "\n";
        }
        std::cout << "reduce task " << reduceTaskNumber << " ends" << std::endl;
    }

    void mergeOutput() {
        std::cout << "merge the output files" << std::endl;
        std::ofstream finalOutput(outputDir + "/output.txt");
        for (int i = 0; i < nReduce; ++i) {
            std::ifstream reduceOutput(outputDir + "/reduce.part-" + std::to_string(i) + ".txt");
            finalOutput << reduceOutput.rdbuf();
        }
    }
public:
    Master(const std::string& inputDir, const std::string& outputDir, int nWorkers, int nReduce) : inputDir(inputDir), outputDir(outputDir), nWorkers(nWorkers), nReduce(nReduce) {
        workers.reserve(nWorkers);
        for(int i = 0; i < nWorkers; i++){
            workers.emplace_back(&Master::worker, this);
        }
    }
    ~Master(){
        {
            std::unique_lock<std::mutex> lock(queue_mtx);
            stop = true;
        }
        condition.notify_all();
        for(std::thread& worker : workers){
            worker.join();
        }
    }

    void run() {
        std::cout << "Initializing MapReduce job..." << std::endl;
        
        // Ensure input and output directories exist or can be created
        if (!fs::exists(inputDir) || !fs::is_directory(inputDir)) {
            std::cerr << "Input directory does not exist." << std::endl;
            return;
        }

        fs::create_directory(outputDir);

        // Schedule map tasks
        nMap = 0;
        for (const auto& entry: fs::directory_iterator(inputDir)){
            if(entry.is_regular_file()){
                auto path = entry.path().string();
                int map_task_number = nMap;
                auto task = [this, path, map_task_number](){
                    this -> mapTask(path, map_task_number);
                };
                {
                    std::unique_lock<std::mutex> lock(queue_mtx);
                    tasks.push(task);
                    ++nRemaining;
                }
                ++nMap;
            }
        }

        condition.notify_all();
        // Wait for all map tasks to complete
        std::cout << "Start wait map tasks" << std::endl;
        {
            std::unique_lock<std::mutex> lock(queue_mtx);
            condition.wait(lock, [this]{return nRemaining  == 0; });
        }
        std::cout << "Complete wait map tasks" << std::endl;

        // Schedule reduce tasks
        for(int i = 0; i < nReduce; i++){
            auto task = [this, i](){
                this -> reduceTask(i);
            };
            {
                std::unique_lock<std::mutex> lock(queue_mtx);
                tasks.push(task);
                ++nRemaining;
            }
            condition.notify_one();
        }
        

        // Wait for all reduce tasks to complete
        {
            std::unique_lock<std::mutex> lock(queue_mtx);
            condition.wait(lock, [this]{return nRemaining  == 0; });
        }
       
        // Merge the outputs
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
