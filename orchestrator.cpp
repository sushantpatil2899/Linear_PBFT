#include <grpcpp/grpcpp.h>
#include "messages.pb.h"
#include "messages.grpc.pb.h"
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <fstream>
#include <nlohmann/json.hpp>
#include <thread>
#include <unordered_map>
#include <map>
#include <chrono>
#include <atomic>
#include <algorithm>
#include "logger.h"

using json = nlohmann::json;
using logLevel = Logger::Level;
using grpc::CompletionQueue;
using namespace std;

class MainOrchestrator
{
public:
    MainOrchestrator(string id, int port, string resourcesFile, string inputCSVFile, string logFile = "orchestrator_log.txt")
        : port_(port),
          resourcesFile_(move(resourcesFile)), inputCSVFile_(move(inputCSVFile)),
          logger_(logFile, {logLevel::INFO, logLevel::ERROR, logLevel::DEBUG, logLevel::MSG_IN, logLevel::MSG_OUT}, Logger::Output::BOTH),
          id_(move(id)) {}

    void run()
    {
        logger_.log(logLevel::INFO, "Starting orchestrator.");

        json resourcesJson = openJsonFile(resourcesFile_+"/resources.json");
        initResources(resourcesJson);

        loadInputCSV();

        this_thread::sleep_for(chrono::seconds(10));


        for (auto &[setNumber, data] : sets_)
        {
            logger_.log(logLevel::INFO, "Processing Set " + to_string(setNumber));

            string liveNodesStr;
            for (auto &node : data.live_nodes)
                liveNodesStr += node + " ";
            logger_.log(logLevel::DEBUG, "Live Nodes: " + liveNodesStr);
            string byzantineNodesStr;
            for (auto &node : data.byzantine_nodes)
                byzantineNodesStr += node + " ";
            logger_.log(logLevel::DEBUG, "Byzantine Nodes: " + byzantineNodesStr);
            string byzantineAttackStr;
            for (auto &attack : data.byzantine_attacks)
            {
                string attackStr = attack.type + " (";
                for (auto &node : attack.targetNodeIds)
                    attackStr += node + ", ";
                attackStr += ")";
                byzantineAttackStr += attackStr + "; ";
            }
            logger_.log(logLevel::DEBUG, "Byzantine Attack: " + byzantineAttackStr);
            manageNodeSetup(setNumber, data.live_nodes, data.byzantine_nodes, data.byzantine_attacks);
            vector<Transaction> transactionList;
            for (auto &txn : data.transactions)
            {
                logger_.log(logLevel::MSG_OUT, txn.sender + " -> " + txn.receiver + " : " + to_string(txn.unit));
            }
            string confirm;
            cout << "Confirm to proceed (Y/N)? ";
            while (true) {
                cin >> confirm;
                if (confirm == "Y" || confirm == "N") break;
                else cout << endl << "Invalid entry! Retry." << endl;
            }

            if (confirm == "N") continue;          

            broadcastTransactions(setNumber, data.transactions);

            logger_.log(logLevel::INFO, "Prompting user with menu.");
            bool exit = false;
            while (true)
            {
                cout << "MENU: [1] PrintLog  [2] PrintDB  [3] PrintStatus  [4] PrintView  [5] Proceed with next set [6] EXIT\nEnter choice: ";
                int choice;
                cin >> choice;
                if (choice == 1)
                {
                    thread(&MainOrchestrator::callHelperFunctions, this, "PrintLog", 0).detach();
                }
                else if (choice == 2)
                {
                    thread(&MainOrchestrator::callHelperFunctions, this, "PrintDB", 0).detach();
                }
                else if (choice == 3)
                {
                    int seqNo;
                    cout << "Sequence Number ? (ex:1, 2)";
                    cin >> seqNo;
                    thread(&MainOrchestrator::callHelperFunctions, this, "PrintStatus", seqNo).detach();
                }
                else if (choice == 4)
                {
                    thread(&MainOrchestrator::callHelperFunctions, this, "PrintView", 0).detach();
                }
                else if (choice == 5)
                {
                    break;
                }
                else if (choice == 6)
                {
                    exit = true;
                    break;
                }
                else
                {
                    cout << "\nInvalid entry! Retry.\n";
                }
            }

            if (exit)
                break;
        }

        cout << "Do you wish to proceed with the closure of the server? (Y/N)" << "\n";
        string choice;
        while (true) {
            cin >> choice;
            if (choice == "Y") {
                shutdownAllNodesAndClients();
                break;
            }
            else if (choice == "N") {
                break;
            }
            else cout << "Invalid entry! Retry." << "\n";
        }
    }

private:
    struct Node
    {
        string id;
        string host;
        int port;
        unique_ptr<Orchestrator::Stub> orchestratorStub;
        bool status;
    };

    struct Client
    {
        string id;
        string host;
        int port;
        unique_ptr<Orchestrator::Stub> orchestratorStub;
    };

    struct Transaction
    {
        string sender;
        string receiver;
        int unit;
        bool isLeaderFailureCommand;
    };

    struct ByzantineAttack
    {
        string type;
        vector<string> targetNodeIds;
    };

    struct SetData
    {
        vector<Transaction> transactions;
        vector<string> live_nodes;
        vector<string> byzantine_nodes;
        vector<ByzantineAttack> byzantine_attacks;
    };

    string id_;
    int port_;
    string resourcesFile_;
    string inputCSVFile_;
    unordered_map<string, Node> nodes_;
    unordered_map<string, Client> clients_;
    map<int, SetData> sets_;
    Logger logger_;
    CompletionQueue cq_;
    struct AsyncOrchestratorCall {
        Empty reply;
        grpc::ClientContext context;
        grpc::Status status;
        unique_ptr<grpc::ClientAsyncResponseReader<Empty>> response_reader;
    };

    void ProcessCompletions() {
        logger_.log(logLevel::INFO, "[Func: ProcessCompletions] ENTER - Processing async completions.");
        void* tag;
        bool ok;
        while (cq_.Next(&tag, &ok)) {
        auto* call = static_cast<AsyncOrchestratorCall*>(tag);
        delete call;
        }
        logger_.log(logLevel::INFO, "[Func: ProcessCompletions] EXIT - CQ drained.");
    }

    void callHelperFunctions(string request_type, int seqNo = 0)
    {
        HelperFunctionRequest request;
        request.set_request_type(request_type);
        request.set_sequence_number(seqNo);

        
        for (auto &[nodeId, n] : nodes_)
        {
            auto* call = new AsyncOrchestratorCall;
            call->response_reader = n.orchestratorStub->AsyncCallHelperFunction(&call->context, request, &cq_);
            call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        }
    }

    bool shutdownAllNodesAndClients() {
        Empty empty;
        bool result = true;
        for (const auto& [id, node]: nodes_) {
            grpc::ClientContext context;
            grpc::Status status = node.orchestratorStub->SendShutdown(&context, empty, &empty);
            if (!status.ok()) {
                logger_.log(logLevel::INFO, "Unable to kill existing run of node: "+id+".");
                result = false;
            }
        }

        for (const auto& [id, client]: clients_) {
            grpc::ClientContext context;
            grpc::Status status = client.orchestratorStub->SendShutdown(&context, empty, &empty);
            if (!status.ok()) {
                logger_.log(logLevel::INFO, "Unable to kill existing run of client: "+id+".");
                result = false;
            }
        }
        return result;
    }

    inline void launchDetached(const string& cmd, const string& title) {
        string full = "gnome-terminal --title='" + title + "' -- bash -c '" + cmd + ";'";
        system(full.c_str());
    }

    bool launchAllNodesAndClients() {
        for (auto& [id, node] : nodes_) {
            string cmd = resourcesFile_ + "/build/node --id " + id +
                        " --port " + to_string(node.port) +
                        " --stateFile " + resourcesFile_ + "/node_files/" + id + "_state.json";
            launchDetached(cmd, "Node "+ id);
        }

        for (auto& [id, client] : clients_) {
            string cmd = resourcesFile_ + "/build/client --id " + id +
                        " --port " + to_string(client.port) +
                        " --configFile " + resourcesFile_ + "/client_files/client_config.json";
            launchDetached(cmd, "Client "+ id);
        }

        return true;
    }
    
    json openJsonFile(const string &file_path)
    {
        ifstream f(file_path);
        json j;
        if (!f.is_open())
        {
            logger_.log(logLevel::ERROR, "Could not open file " + file_path);
            return j;
        }
        f >> j;
        return j;
    }

    void initResources(const json &resourcesJson)
    {
        logger_.log(logLevel::INFO, "Initializing resources.");
        for (auto &r : resourcesJson)
        {
            if (r["id"] == id_)
                continue;
            auto channel = grpc::CreateChannel(r["host"].get<string>() + ":" + to_string(r["port"]), grpc::InsecureChannelCredentials());
            unique_ptr<Orchestrator::Stub> stub(Orchestrator::NewStub(channel));
            if (r["type"] == "node")
            {
                nodes_[r["id"]] = {r["id"], r["host"], r["port"], move(stub), false};
                logger_.log(logLevel::DEBUG, "Added node: " + r["id"].get<string>());
            }
            else if (r["type"] == "client")
            {
                clients_[r["id"]] = {r["id"], r["host"], r["port"], move(stub)};
                logger_.log(logLevel::DEBUG, "Added client: " + r["id"].get<string>());
            }
        }
    }

    void loadInputCSV()
    {
        ifstream file(inputCSVFile_);
        if (!file.is_open())
        {
            logger_.log(logLevel::ERROR, "Could not open file " + inputCSVFile_);
            return;
        }

        string line;
        getline(file, line);
        int set_no = -1;

        while (getline(file, line))
        {
            string setStr, txnStr, nodesStr, byzantineNodesStr, byzantineAttackStr;
            size_t pos1 = line.find(',');
            if (pos1 == string::npos)
                continue;
            if (pos1 == 0)
            {
                size_t pos2 = line.find('(', pos1 + 1);
                if (pos2 == string::npos)
                    continue;
                size_t pos3 = line.find(')', pos2 + 1);
                if (pos3 == string::npos)
                    continue;
                txnStr = line.substr(pos2 + 1, pos3 - pos2 - 1);
                Transaction txn = parseTransaction(txnStr);
                sets_[set_no].transactions.push_back(txn);
            }
            else
            {
                setStr = line.substr(0, pos1);
                size_t pos2 = line.find('(', pos1 + 1);
                if (pos2 == string::npos)
                    continue;
                size_t pos3 = line.find(')', pos2 + 1);
                if (pos3 == string::npos)
                    continue;
                txnStr = line.substr(pos2 + 1, pos3 - pos2 - 1);
                size_t pos4 = line.find('"', pos3 + 2);
                if (pos4 == string::npos)
                    continue;
                size_t pos5 = line.find('"', pos4 + 1);
                if (pos5 == string::npos)
                    continue;
                nodesStr = line.substr(pos4 + 1, pos5 - pos4 - 1);
                set_no = stoi(trim(setStr));
                Transaction txn = parseTransaction(txnStr);
                vector<string> liveNodes = parseLiveNodes(nodesStr);
                size_t pos6 = line.find('[', pos5 + 1);
                if (pos6 == string::npos)
                    continue;
                size_t pos7 = line.find(']', pos6 + 1);
                if (pos7 == string::npos)
                    continue;
                byzantineNodesStr = line.substr(pos6, pos7 - pos6 + 1);
                size_t pos8 = line.find('[', pos7 + 1);
                if (pos8 == string::npos)
                    continue;
                size_t pos9 = line.find(']', pos8 + 1);
                if (pos9 == string::npos)
                    continue;
                byzantineAttackStr = line.substr(pos8, pos9 - pos8 + 1);
                vector<string> byzantineNodes = parseLiveNodes(byzantineNodesStr);
                vector<ByzantineAttack> byzantineAttacks = parseByzantineAttacks(byzantineAttackStr);
                sets_[set_no].transactions.push_back(txn);
                if (sets_[set_no].live_nodes.empty())
                {
                    sets_[set_no].live_nodes = liveNodes;
                }
                if (sets_[set_no].byzantine_nodes.empty())
                {
                    sets_[set_no].byzantine_nodes = byzantineNodes;
                }
                if (sets_[set_no].byzantine_attacks.empty())
                {
                    sets_[set_no].byzantine_attacks = byzantineAttacks;
                }
            }
        }
        logger_.log(logLevel::INFO, "Loaded input CSV.");
    }

    inline ByzantineAtt ToProto(const ByzantineAttack& b) {
        ByzantineAtt p;
        p.set_type(b.type);
        *p.mutable_target_node_ids() = {b.targetNodeIds.begin(), b.targetNodeIds.end()};
        return p;
    }

    inline void VectorToRepeated(const vector<ByzantineAttack>& v, google::protobuf::RepeatedPtrField<ByzantineAtt>* out) {
        out->Clear();
        for (const auto& b : v) *out->Add() = ToProto(b);
    }

    void manageNodeSetup(const int& setNumber, const vector<string> &live_nodes, const vector<string> &byzantine_nodes, const vector<ByzantineAttack> &byzantine_attacks)
    {
        if (setNumber!=1)
        {
            if (!shutdownAllNodesAndClients()) {
                logger_.log(logLevel::INFO, "Unable to kill existing run if any of nodes/clients. Please don't proceed with the next set.");
                return;
            }
        }

        this_thread::sleep_for(chrono::seconds(2));

        if (!launchAllNodesAndClients()) {
            logger_.log(logLevel::INFO, "Unable to launch/revive nodes/clients. Please don't proceed with the next set.");
            return;
        }

        this_thread::sleep_for(chrono::seconds(10));

        vector<thread> threads;
        atomic<int> counter{0};

        for (auto &[nodeId, n] : nodes_)
        {
            bool activate = (find(live_nodes.begin(), live_nodes.end(), nodeId) != live_nodes.end());
            bool byzantine = (find(byzantine_nodes.begin(), byzantine_nodes.end(), nodeId) != byzantine_nodes.end());
            threads.emplace_back([&, nodeId, activate, byzantine, byzantine_attacks]()
                                 {
                NodeSetupRequest request;
                request.set_activate_node(activate);
                request.set_byzantine_node(byzantine);
                if (byzantine) {
                    VectorToRepeated(byzantine_attacks, request.mutable_attacks());
                }

                Empty response;
                grpc::ClientContext context;
                grpc::Status status = n.orchestratorStub->ToSetupNode(&context, request, &response);
             });
        }

        for (auto &t : threads)
            t.join();
        logger_.log(logLevel::INFO, "All nodes setup successfully.");
    }

    void broadcastTransactions(const int &set_number, const vector<Transaction> &transactions)
    {
        vector<thread> threads;

        for (auto &[clientId, c] : clients_)
        {
            threads.emplace_back([&, clientId]()
                                 {
                TransactionSetRequest request;
                bool make_call = false;
                for (const auto& txn : transactions) {
                    if (txn.sender==clientId) {
                        auto* txnEntry = request.add_transactions();
                        txnEntry->set_sender(txn.sender);
                        txnEntry->set_receiver(txn.receiver);
                        txnEntry->set_unit(txn.unit);
                        make_call = true;
                    }
                }
                if (make_call) {
                    request.set_set_number(set_number);
                    Empty response;
                    grpc::ClientContext context;
                    grpc::Status status = c.orchestratorStub->SendTransactions(&context, request, &response);
                } });
        }

        for (auto &t : threads)
            t.join();
    }

    static string trim(const string &s)
    {
        size_t start = s.find_first_not_of(" \t");
        size_t end = s.find_last_not_of(" \t");
        return (start == string::npos) ? "" : s.substr(start, end - start + 1);
    }

    static vector<string> parseLiveNodes(const string &s)
    {
        vector<string> nodes;
        string cleaned = s;
        cleaned.erase(remove(cleaned.begin(), cleaned.end(), '['), cleaned.end());
        cleaned.erase(remove(cleaned.begin(), cleaned.end(), ']'), cleaned.end());
        cleaned.erase(remove(cleaned.begin(), cleaned.end(), '('), cleaned.end());
        cleaned.erase(remove(cleaned.begin(), cleaned.end(), ')'), cleaned.end());

        stringstream ss(cleaned);
        string node;
        while (getline(ss, node, ','))
        {
            nodes.push_back(trim(node));
        }
        return nodes;
    }

    static vector<ByzantineAttack> parseByzantineAttacks(const string &s)
    {
        vector<ByzantineAttack> ba;
        string cleaned = s;
        cleaned.erase(remove(cleaned.begin(), cleaned.end(), '['), cleaned.end());
        cleaned.erase(remove(cleaned.begin(), cleaned.end(), ']'), cleaned.end());

        stringstream ss(cleaned);
        string attack;
        while (getline(ss, attack, ';'))
        {
            string type;
            vector<string> targetNodeIds;
            attack = trim(attack);
            size_t pos1 = attack.find('(',0);
            if (pos1 == string::npos)
                type = attack;
            else
            {
                type = attack.substr(0, pos1);
                size_t pos2 = attack.find(')', pos1 + 1);
                if (pos2 == string::npos)
                    continue;
                string targetNodesStr = attack.substr(pos1, pos2 - pos1 + 1);
                targetNodeIds = parseLiveNodes(targetNodesStr);
            }
            ba.push_back({type, targetNodeIds});
        }
        return ba;
    }

    static Transaction parseTransaction(const string &s)
    {
        string cleaned = s;
        cleaned.erase(remove(cleaned.begin(), cleaned.end(), '('), cleaned.end());
        cleaned.erase(remove(cleaned.begin(), cleaned.end(), ')'), cleaned.end());

        stringstream ss(cleaned);
        string sender, receiver, unitStr;
        getline(ss, sender, ',');
        getline(ss, receiver, ',');
        getline(ss, unitStr);
        if (unitStr.empty())
        {
            unitStr = "0";
        }
        return {trim(sender), trim(receiver), stoi(trim(unitStr)), false};
    }
};

int main(int argc, char *argv[])
{
    int port = -1;
    string id, resourcesFile, inputCSVFile;

    for (int i = 1; i < argc; i++)
    {
        string arg = argv[i];
        if (arg == "--id" && i + 1 < argc)
            id = argv[++i];
        else if (arg == "--port" && i + 1 < argc)
            port = stoi(argv[++i]);
        else if (arg == "--resources" && i + 1 < argc)
            resourcesFile = argv[++i];
        else if (arg == "--inputCSV" && i + 1 < argc)
            inputCSVFile = argv[++i];
    }

    if (id.empty() || port == -1 || resourcesFile.empty() || inputCSVFile.empty())
    {
        cerr << "Usage: ./orchestrator --id <id> --port <port> --resources <file> --inputCSV <file>\n";
        return 1;
    }

    MainOrchestrator orch(id, port, resourcesFile, inputCSVFile);
    orch.run();

    return 0;
}
