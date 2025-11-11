#include <grpcpp/grpcpp.h>
#include <google/protobuf/repeated_field.h>
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
#include <atomic>
#include <algorithm>
#include <chrono>
#include "logger.h"
#include <future>
#include <random>
#include "bls_helper.h"

using std::atomic;
using std::max;
using std::cerr;
using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::chrono::time_point_cast;
using std::cout;
using std::endl;
using std::ifstream;
using std::lock_guard;
using std::map;
using std::move;
using std::mutex;
using std::string;
using std::thread;
using std::to_string;
using std::unique_ptr;
using std::shared_ptr;
using std::make_shared;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using std::future;
using json = nlohmann::json;
using logLevel = Logger::Level;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::CompletionQueue;
using std::this_thread::sleep_for;
using std::stoi;
using std::future;
using std::launch;
using std::async;
using std::size_t;
using std::hash;

class MainClient {
public:
    MainClient(string id, int port, string configFile, string logFile)
        : port_(port),
          configFile_(move(configFile)),
          logger_(logFile,{logLevel::INFO, logLevel::ERROR, logLevel::DEBUG, logLevel::MSG_IN, logLevel::MSG_OUT}, Logger::Output::BOTH),
          id_(move(id)),
          leaderNode_("n1"),
          cqThread_(&MainClient::ProcessCompletions, this) {} 

    void run() {

        logger_.log(logLevel::INFO, "[Func: run] ENTER - Starting client main execution.");
        json configJson = openJsonFile(configFile_);
        setClientProperties(configJson);
        logger_.log(logLevel::DEBUG, "[Func: run] ACTION - Configuration properties set.");
        setLeader();
        logger_.log(logLevel::DEBUG, "[Func: run] ACTION - Leader identified for current view.");
        runServer();
        logger_.log(logLevel::DEBUG, "[Func: run] ACTION - Server started, loading configuration file: " + configFile_);
        json resourcesJson = openJsonFile(resourcesFile_);
        logger_.log(logLevel::DEBUG, "[Func: run] ACTION - Loading resources file: " + resourcesFile_);
        initResources(resourcesJson);
        logger_.log(logLevel::INFO, "[Func: run] STATE - Resources initialized successfully.");
        clientThread_.join();
        logger_.log(logLevel::INFO, "[Func: run] EXIT - Client thread joined, shutting down.");
    }

    void setClientProperties(const json &clientConfig)
    {
        logger_.log(logLevel::INFO, "[Func: setClientProperties] ENTER - Setting client configuration properties.");
        if (clientConfig.contains("resources_file"))
        {
            resourcesFile_ = clientConfig["resources_file"].get<string>();
            logger_.log(logLevel::DEBUG, "[Func: setClientProperties] STATE - resources_file=" + resourcesFile_);
        }
        if (clientConfig.contains("timeout_ms"))
        {
            timeoutMs_.store(clientConfig["timeout_ms"].get<int>());
            logger_.log(logLevel::DEBUG, "[Func: setClientProperties] STATE - timeout_ms=" + to_string(timeoutMs_.load()));
        }
        if (clientConfig.contains("no_faults"))
        {
            noFaults_.store(clientConfig["no_faults"].get<int>());
            logger_.log(logLevel::DEBUG, "[Func: setClientProperties] STATE - no_faults=" + to_string(noFaults_.load()));
        }
        if (clientConfig.contains("total_nodes"))
        {
            totalNodes_.store(clientConfig["total_nodes"].get<int>());
            logger_.log(logLevel::DEBUG, "[Func: setClientProperties] STATE - total_nodes=" + to_string(totalNodes_.load()));
        }
        logger_.log(logLevel::INFO, "[Func: setClientProperties] EXIT - Configuration applied.");
    }

    ~MainClient() {
        logger_.log(logLevel::INFO, "[Func: ~MainClient] ENTER - Cleaning up client resources.");
        if (server_) {
            logger_.log(logLevel::DEBUG, "[Func: ~MainClient] ACTION - Shutting down gRPC server.");
            server_->Shutdown();
        }
        if (taskFuture_.valid()) {
            logger_.log(logLevel::DEBUG, "[Func: ~MainClient] ACTION - Waiting for active async task.");
            taskFuture_.wait();
        }
        cq_.Shutdown();
        logger_.log(logLevel::DEBUG, "[Func: ~MainClient] ACTION - Completion queue shut down.");
        if (cqThread_.joinable()) {
            logger_.log(logLevel::DEBUG, "[Func: ~MainClient] ACTION - Joining CQ thread.");
            cqThread_.join();
        }
        if (clientThread_.joinable()) {
            logger_.log(logLevel::DEBUG, "[Func: ~MainClient] ACTION - Joining client thread.");
            clientThread_.join();
        }
        logger_.log(logLevel::INFO, "[Func: ~MainClient] EXIT - Client shutdown complete.");
    }

private:
    struct Node {
        string id;
        string host;
        int port;
        unique_ptr<NodeService::Stub> nodeStub;
        string publicKey;
    };

    struct Transaction {
        string sender;
        string receiver;
        int unit;
    };

    struct SetData {
        vector<Transaction> transactions;
        bool succesfully_processed;
    };

    class OrchestratorServiceImpl final : public ::Orchestrator::Service {

    public:
        OrchestratorServiceImpl(Logger& logger, MainClient& orig) : logger_(logger), clientOrig(orig){}
        grpc::Status SendTransactions(grpc::ServerContext* context,
                          const TransactionSetRequest* request,
                          Empty* response) override {
            logger_.log(logLevel::INFO, "[Func: OrchestratorServiceImpl::SendTransactions] ENTER - set_number=" + to_string(request->set_number()));
            logger_.log(logLevel::MSG_IN, "Received SendTransactions request for client "+ clientOrig.id_ + ": " + request->DebugString());
            for (const auto& txn : request->transactions()) {
                clientOrig.setOfTransactions[request->set_number()].transactions.push_back({txn.sender(), txn.receiver(), txn.unit()});
            }
            logger_.log(logLevel::DEBUG, "[Func: OrchestratorServiceImpl::SendTransactions] STATE - Enqueued " + to_string(request->transactions_size()) + " transactions for set " + to_string(request->set_number()));
            thread(&MainClient::handleTransactionsThread, &clientOrig, request->set_number()).detach();
            logger_.log(logLevel::INFO, "[Func: OrchestratorServiceImpl::SendTransactions] EXIT - Handler thread detached for set " + to_string(request->set_number()));
            return grpc::Status::OK;
        }

        grpc::Status SendShutdown(grpc::ServerContext *context,
                                            const Empty *request,
                                            Empty *response) override
        {
            logger_.log(logLevel::INFO, "[Func: SendShutdown] ENTER - Received Client Shutdown from orchestrator.");
            thread([this]() {
                clientOrig.stopServer();
                exit(0);
            }).detach();
            return grpc::Status::OK;
        }

    private:
        Logger& logger_;
        MainClient& clientOrig;
    };

    class NodeServiceImpl final : public ::NodeService::Service {
    public:
        NodeServiceImpl(Logger& logger, MainClient& orig) : logger_(logger), clientOrig(orig){}
        grpc::Status SendReply(grpc::ServerContext* context,
                          const Reply* request,
                          Empty* response) override {
            logger_.log(logLevel::INFO, "[Func: NodeServiceImpl::SendReply] ENTER - from node=" + request->node());
            logger_.log(logLevel::INFO, "timestamp: "+to_string(request->reply_content().timestamp()));
            if (!bls_utils::verify_signature(clientOrig.nodes_[request->node()].publicKey, request->signature(), clientOrig.serializeDeterministic(request->reply_content()))) {
                logger_.log(logLevel::INFO, "Verificaion failed for sender " + request->node());
                return grpc::Status::CANCELLED;
            }
            logger_.log(logLevel::DEBUG, "[Func: NodeServiceImpl::SendReply] STATE - Signature verified for node " + request->node());
            clientOrig.handleReply(*request);
            logger_.log(logLevel::INFO, "[Func: NodeServiceImpl::SendReply] EXIT - Reply handled for ts=" + to_string(request->reply_content().timestamp()));
            return grpc::Status::OK;
        }

    private:
        Logger& logger_;
        MainClient& clientOrig;
    };

    string id_;
    int port_;
    string resourcesFile_, configFile_;
    mutex leaderNodeMx_, taskMutex_, pendingMx_;
    string leaderNode_;
    unordered_map<int, string> viewLeaderMap_{{0, "n7"}, {1, "n1"}, {2, "n2"}, {3, "n3"}, {4, "n4"}, {5, "n5"}, {6, "n6"}};
    atomic<int> timeoutMs_;
    atomic<int> noFaults_;
    atomic<int> totalNodes_;
    string privateKey_;
    string publicKey_;
    unordered_map<string, Node> nodes_;
    Logger logger_;
    future<bool> taskFuture_;
    atomic<int> currentView_{1};
    OrchestratorServiceImpl orchestratorService_{logger_, *this};
    NodeServiceImpl nodeService_{logger_, *this};
    unique_ptr<grpc::Server> server_;
    thread clientThread_;
    unordered_map<int, SetData> setOfTransactions;
    CompletionQueue cq_;
    thread cqThread_;
    struct ResultHash {
        size_t operator()(const Result& r) const noexcept {
            return (hash<bool>()(r.successfully_processed())) ^ (hash<int>()(r.balance_value()) << 1);
        }
    };
    struct ResultEq {
        bool operator()(const Result& a, const Result& b) const noexcept {
            return a.successfully_processed() == b.successfully_processed() &&
                a.balance_value() == b.balance_value();
        }
    };
    struct PendingRequest {
        long long timestamp;
        RequestContent request_content;
        unordered_map<Result, int, ResultHash, ResultEq> resultCounts;
        unordered_set<string> receivedFromNodes;
        unordered_map<string, Result> resultsFromNodes;
        mutex mtx;
        bool completed = false;
        Result decidedResult;
    };
    unordered_map<long long, shared_ptr<PendingRequest>> pending_;;
    struct AsyncClientCall {
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
        auto* call = static_cast<AsyncClientCall*>(tag);
        delete call;
        }
        logger_.log(logLevel::INFO, "[Func: ProcessCompletions] EXIT - CQ drained.");
    }

    void handleTransactionsThread(int setNumber)
    {
        logger_.log(logLevel::INFO, "[Func: handleTransactionsThread] ENTER - setNumber=" + to_string(setNumber));
        lock_guard<mutex> lock(taskMutex_);
        if (taskFuture_.valid()) taskFuture_.wait();
        taskFuture_ = async(launch::async, &MainClient::handleTransactions, this, setNumber);
        logger_.log(logLevel::INFO, "[Func: handleTransactionsThread] EXIT - Async task launched for set " + to_string(setNumber));
    }

    void runServer() {
        string server_address("0.0.0.0:" + to_string(port_));

        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&orchestratorService_);
        builder.RegisterService(&nodeService_);
        server_ = builder.BuildAndStart();
        logger_.log(logLevel::INFO, "Client "+id_+" listening on " + server_address);
        clientThread_ = thread([this]() { server_->Wait(); });
        sleep_for(seconds(5));
        logger_.log(logLevel::DEBUG, "[Func: runServer] STATE - Server started, worker thread detached.");
    }

    void stopServer() {
        logger_.log(logLevel::INFO, "[Func: ~MainClient] ENTER - Cleaning up client resources.");
        if (server_) {
            logger_.log(logLevel::DEBUG, "[Func: ~MainClient] ACTION - Shutting down gRPC server.");
            server_->Shutdown();
        }
        if (taskFuture_.valid()) {
            logger_.log(logLevel::DEBUG, "[Func: ~MainClient] ACTION - Skipping wait, detaching async task.");
            thread([](auto fut) mutable {
                try { fut.wait(); } catch (...) {}
            }, move(taskFuture_)).detach();
        }
        cq_.Shutdown();
        logger_.log(logLevel::DEBUG, "[Func: ~MainClient] ACTION - Completion queue shut down.");
        if (cqThread_.joinable()) {
            logger_.log(logLevel::DEBUG, "[Func: ~MainClient] ACTION - Joining CQ thread.");
            cqThread_.join();
        }
        if (clientThread_.joinable()) {
            logger_.log(logLevel::DEBUG, "[Func: ~MainClient] ACTION - Joining client thread.");
            clientThread_.join();
        }
        logger_.log(logLevel::INFO, "[Func: ~MainClient] EXIT - Client shutdown complete.");
    }

    json openJsonFile(const string& file_path) {
        logger_.log(logLevel::DEBUG, "[Func: openJsonFile] ENTER - path=" + file_path);
        ifstream f(file_path);
        json j;
        if (!f.is_open()) {
            logger_.log(logLevel::ERROR, "Could not open file " + file_path);
            return j;
        }
        f >> j;
        logger_.log(logLevel::DEBUG, "[Func: openJsonFile] EXIT - loaded keys=" + to_string(j.size()));
        return j;
    }

    void initResources(const json& resourcesJson) {

        logger_.log(logLevel::INFO, "[Func: initResources] ENTER - items=" + to_string(resourcesJson.size()));
        for (auto& r : resourcesJson) {
            if (r["id"] == id_)
            {   
                privateKey_ = r["private_key"];
                publicKey_ = r["public_key"];
                logger_.log(logLevel::DEBUG, "[Func: initResources] STATE - Local keys loaded for id=" + id_);
                continue;
            }

            if (r["type"] == "node") {
                auto channel = grpc::CreateChannel(r["host"].get<string>() + ":" + to_string(r["port"]), grpc::InsecureChannelCredentials());
                unique_ptr<NodeService::Stub> stub(NodeService::NewStub(channel));
                nodes_[r["id"]] = {r["id"], r["host"], r["port"], move(stub), r["public_key"]};
                logger_.log(logLevel::DEBUG, "[Func: initResources] ACTION - Added NODE " + string(r["id"]) + " @ " + string(r["host"]) + ":" + to_string(r["port"]));
            }
        }
        logger_.log(logLevel::INFO, "[Func: initResources] EXIT - nodes=" + to_string(nodes_.size()));
    }

    shared_ptr<PendingRequest> submitTransaction(const Transaction& txnMap)
    {
        logger_.log(logLevel::INFO, "[Func: submitTransaction] ENTER - sender=" + txnMap.sender + ", receiver=" + txnMap.receiver + ", unit=" + to_string(txnMap.unit));
        auto req = make_shared<PendingRequest>();
        req->timestamp = getCurrentTimestampMs();
        auto* content = &req->request_content;
        content->set_message_type(REQUEST);
        content->set_timestamp(req->timestamp);
        auto* op = content->mutable_operation();
        op->set_type(txnMap.receiver.empty() ? READ_ONLY : READ_WRITE);
        auto* txn = op->mutable_transaction();
        txn->set_sender(txnMap.sender);
        txn->set_receiver(txnMap.receiver);
        txn->set_unit(txnMap.unit);
        req->completed = false;
        {
            lock_guard<mutex> lock(pendingMx_);
            pending_[req->timestamp] = req;
        }
        if (txnMap.receiver.empty()) {
            logger_.log(logLevel::INFO, "[Submitting Read-Only Transaction] Timestamp: " + to_string(req->timestamp) + ", Sender: " + txnMap.sender);
            BroadcastTransaction(req);
        } else {
            logger_.log(logLevel::INFO, "[Submitting Read-Write Transaction] Timestamp: " + to_string(req->timestamp) + ", Sender: " + txnMap.sender + ", Receiver: " + txnMap.receiver + ", Unit: " + to_string(txnMap.unit));
            SendAsyncToLeader(req);
        }

        StartTimer(req);
        logger_.log(logLevel::DEBUG, "[Func: submitTransaction] EXIT - Timer started for ts=" + to_string(req->timestamp));
        return req;
    }

    void SendAsyncToLeader(shared_ptr<PendingRequest> req) {
        logger_.log(logLevel::INFO, "[Func: SendAsyncToLeader] ENTER - ts=" + to_string(req->timestamp));
        grpc::ClientContext context;
        auto* call = new AsyncClientCall;
        Request request;
        request.set_client(id_);
        *request.mutable_request_content() = req->request_content;
        request.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(req->request_content)));
        {
            lock_guard<mutex> l(leaderNodeMx_);
            call->response_reader = nodes_[leaderNode_].nodeStub->AsyncSendRequest(&call->context, request, &cq_);
        }
        call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        logger_.log(logLevel::DEBUG, "[Func: SendAsyncToLeader] EXIT - Sent to leader " + leaderNode_);
    }

    void StartTimer(shared_ptr<PendingRequest> req) {
        logger_.log(logLevel::DEBUG, "[Func: StartTimer] ENTER - ts=" + to_string(req->timestamp) + ", timeout_ms=" + to_string(timeoutMs_.load()));
        thread([this, req]() {
            sleep_for(milliseconds(timeoutMs_.load()));

            lock_guard<mutex> lock(req->mtx);
            if (!req->completed) {
                logger_.log(logLevel::INFO, "[Timer Expired] Retrying txn " + to_string(req->timestamp));
                if (req->request_content.operation().type() == READ_ONLY) {
                    req->request_content.mutable_operation()->set_type(READ_PROMOTED_TO_READ_WRITE);
                    SendAsyncToLeader(req);
                }
                else {
                    BroadcastTransaction(req);
                }
                StartTimer(req);
            }
        }).detach();
    }

    void BroadcastTransaction(shared_ptr<PendingRequest> req) {
        logger_.log(logLevel::INFO, "[Func: BroadcastTransaction] ENTER - ts=" + to_string(req->timestamp));
        Request request;
        request.set_client(id_);
        *request.mutable_request_content() = req->request_content;
        request.set_signature(bls_utils::sign_data(privateKey_, req->request_content.SerializeAsString()));
        for (auto& [nodeId, n] : nodes_) {
            grpc::ClientContext context;
            auto* call = new AsyncClientCall;
            call->response_reader = n.nodeStub->AsyncSendRequest(&call->context, request, &cq_);
            call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        }
        logger_.log(logLevel::INFO, "[Func: BroadcastTransaction] EXIT - request sent to " + to_string(nodes_.size()) + " nodes.");
    }

    void handleReply(const Reply& reply) {
        logger_.log(logLevel::INFO, "[Func: handleReply] ENTER - from node=" + reply.node() + ", ts=" + to_string(reply.reply_content().timestamp()));
        shared_ptr<PendingRequest> req;
        {
            lock_guard<mutex> lock(pendingMx_);
            auto it = pending_.find(reply.reply_content().timestamp());
            if (it == pending_.end()) return;
            req = it->second;
        }

        lock_guard<mutex> lock(req->mtx);
        if (req->completed) return;
        if (req->receivedFromNodes.find(reply.node()) != req->receivedFromNodes.end()) {
            ResultEq eq;

            const auto& oldResult = req->resultsFromNodes[reply.node()];
            const auto& newResult = reply.reply_content().result();

            if (eq(oldResult, newResult)) {
                return;
            }

            req->resultCounts[oldResult] = max(0, req->resultCounts[oldResult] - 1);
        }

        req->receivedFromNodes.insert(reply.node());
        req->resultsFromNodes[reply.node()] = reply.reply_content().result();
        const auto& result = reply.reply_content().result();

        int count = ++req->resultCounts[result];

        if (count >= (noFaults_.load() + 1)) {
            req->completed = true;
            req->decidedResult = result;
            logger_.log(logLevel::INFO, "[Txn " + to_string(req->timestamp) + "] Reached f+1 matching replies. Final Result - Successfully Processed: " + to_string(result.successfully_processed()) + ", Balance: " + to_string(result.balance_value()));
            if (reply.reply_content().view_number()>currentView_.load()) {
                logger_.log(logLevel::INFO, "[View Number " + to_string(reply.reply_content().view_number()) + "] received from the Reply message is greater than the current view number: " + to_string(currentView_.load()) + ", hence will need to update the leader. " );
                currentView_.store(reply.reply_content().view_number());
                setLeader();
            }
        }
        logger_.log(logLevel::DEBUG, "[Func: handleReply] EXIT - votes_for_decision=" + to_string(count));
    }

    bool handleTransactions(const int& set_number) {
        logger_.log(logLevel::INFO, "[Func: handleTransactions] ENTER - set_number=" + to_string(set_number));
        for (const auto& txnMap : setOfTransactions[set_number].transactions) {
            logger_.log(logLevel::INFO, "Client: "+ id_ +" | Set Number: "+ to_string(set_number) +" | Sender: " + txnMap.sender + ", Receiver: " + txnMap.receiver + ", Unit: " + to_string(txnMap.unit));    
            shared_ptr<PendingRequest> req = submitTransaction(txnMap);
            while (true) {
                {
                    lock_guard<mutex> lock(req->mtx);
                    if (req->completed) {
                        logger_.log(logLevel::INFO, "Transaction completed with result - Successfully Processed: " + to_string(req->decidedResult.successfully_processed()) + ", Balance: " + to_string(req->decidedResult.balance_value()));
                        break;
                    }
                }
                sleep_for(milliseconds(10));
            }
        }
        logger_.log(logLevel::INFO, "[Func: handleTransactions] EXIT - all transactions processed for set " + to_string(set_number));
        return true;
    }

    long long getCurrentTimestampMs() {
        auto now = system_clock::now();
        auto ms = time_point_cast<milliseconds>(now);
        return ms.time_since_epoch().count();
    }
    
    string serializeDeterministic(const google::protobuf::Message& msg) {
        string out;
        google::protobuf::io::StringOutputStream stringStream(&out);
        google::protobuf::io::CodedOutputStream codedStream(&stringStream);
        codedStream.SetSerializationDeterministic(true);
        msg.SerializeToCodedStream(&codedStream);
        return out;
    }

    void setLeader()
    {
        logger_.log(logLevel::INFO, "[Func: setLeader] ENTER - Determining leader based on currentView=" + to_string(currentView_.load()));
        int view_mod = currentView_.load()%totalNodes_.load();
        lock_guard<mutex> lg(leaderNodeMx_);
        leaderNode_ = viewLeaderMap_[view_mod];
        logger_.log(logLevel::INFO, "[Func: setLeader] STATE - Leader set to node " + leaderNode_);
        logger_.log(logLevel::INFO, "[Func: setLeader] EXIT - Leader determination complete.");
    }
};

int main(int argc, char* argv[]) {
    bls_utils::initBLS();
    int port = -1;
    string id, configFile;

    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "--id" && i + 1 < argc) id = argv[++i];
        else if (arg == "--port" && i + 1 < argc) port = stoi(argv[++i]);
        else if (arg == "--configFile" && i + 1 < argc) configFile = argv[++i];
    }

    if (id.empty() || port == -1 || configFile.empty()) {
        cerr << "Usage: ./client --id <id> --port <port> --configFile <file>";
        return 1;
    }

    MainClient client(id, port, configFile, "client_"+id+".txt");
    client.run();
    return 0;
}
