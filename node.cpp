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
#include <unordered_set>
#include <mutex>
#include <atomic>
#include <algorithm>
#include "logger.h"
#include <random>
#include <openssl/sha.h>
#include <map>
#include <iomanip>
#include "threadpool.h"
#include "resettable_timer.h"
#include "bls_helper.h"
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>

static void crash_handler(int sig) {
    void* arr[32];
    int n = backtrace(arr, 32);
    fprintf(stderr, "\n[CRASH] signal %d\n", sig);
    backtrace_symbols_fd(arr, n, STDERR_FILENO);
    _Exit(128 + sig);
}

using std::atomic;
using std::cerr;
using std::chrono::seconds;
using std::chrono::milliseconds;
using std::cout;
using std::endl;
using std::ifstream;
using std::lock_guard;
using std::map;
using std::exit;
using std::move;
using std::mutex;
using std::max_element;
using std::max;
using std::min;
using std::minmax_element;
using std::min_element;
using std::string;
using std::pair;
using std::tuple;
using std::hash;
using std::find;
using std::thread;
using std::to_string;
using std::unique_ptr;
using std::shared_ptr;
using std::hex;
using std::make_shared;
using std::deque;
using std::unordered_map;
using std::map;
using std::unordered_set;
using std::vector;
using json = nlohmann::json;
using logLevel = Logger::Level;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::CompletionQueue;
using grpc::Status;
using std::this_thread::sleep_for;
using std::stoi;

class MainNode
{
public:
    MainNode(string id, int port, string stateFile, string logFile)
        : port_(port),
          logger_(logFile, {logLevel::DEBUG, logLevel::INFO, logLevel::WARN, logLevel::ERROR, logLevel::MSG_IN, logLevel::MSG_OUT}, Logger::Output::FILE),
          id_(move(id)),
          processId_(id[1] - '0'),
          stateFile_(move(stateFile)),
          cqThread_(&MainNode::ProcessCompletions, this) {}

    void run()
    {
        logger_.log(logLevel::INFO, "[Func: run] ENTER - Starting node setup.");
        runServer();
        logger_.log(logLevel::DEBUG, "[Func: run] ACTION - Server started successfully.");
        json stateParameters = openJsonFile(stateFile_);
        logger_.log(logLevel::DEBUG, "[Func: run] ACTION - State file read successfully.");
        setNodeProperties(stateParameters);
        logger_.log(logLevel::DEBUG, "[Func: run] ACTION - Node properties initialized.");
        setLeader();
        logger_.log(logLevel::DEBUG, "[Func: run] ACTION - Leader set successfully.");
        json resourcesJson = openJsonFile(resourcesFile_);
        logger_.log(logLevel::DEBUG, "[Func: run] ACTION - Resources file read successfully.");
        initResources(resourcesJson);
        logger_.log(logLevel::INFO, "[Func: run] INFO - Resources setup completed.");
        cout<< "Resources Setup" <<endl;
        logger_.log(logLevel::DEBUG, "[Func: run] STATE - Waiting for node thread to finish.");
        nodeThread_.join();
        logger_.log(logLevel::INFO, "[Func: run] EXIT - Node run completed.");
    }

    void setNodeProperties(const json &stateParameters)
    {
        logger_.log(logLevel::INFO, "[Func: setNodeProperties] ENTER - Setting node properties from JSON.");
        if (stateParameters.contains("resources_file"))
        {
            resourcesFile_ = stateParameters["resources_file"].get<string>();
            logger_.log(logLevel::DEBUG, "[Func: setNodeProperties] STATE: resourcesFile_ set to " + resourcesFile_);
        }
        if (stateParameters.contains("data_state_file"))
        {
            dataStateFile_ = stateParameters["data_state_file"].get<string>();
            logger_.log(logLevel::DEBUG, "[Func: setNodeProperties] STATE: dataStateFile_ set to " + dataStateFile_);
        }
        if (stateParameters.contains("timeout_ms"))
        {
            timeoutMs_.store(stateParameters["timeout_ms"].get<int>());
            logger_.log(logLevel::DEBUG, "[Func: setNodeProperties] STATE: timeoutMs_ set to " + to_string(timeoutMs_.load()));
        }
        if (stateParameters.contains("checkpoint_interval"))
        {
            checkpointInterval_.store(stateParameters["checkpoint_interval"].get<int>());
            logger_.log(logLevel::DEBUG, "[Func: setNodeProperties] STATE: checkpointInterval_ set to " + to_string(checkpointInterval_.load()));
        }
        if (stateParameters.contains("no_faults"))
        {
            noFaults_.store(stateParameters["no_faults"].get<int>());
            quorumSize_.store(2 * noFaults_.load() + 1);
            logger_.log(logLevel::DEBUG, "[Func: setNodeProperties] STATE: noFaults_ set to " + to_string(noFaults_.load()) + ", quorumSize_ computed as " + to_string(quorumSize_.load()));
        }
        if (stateParameters.contains("total_nodes"))
        {
            totalNodes_.store(stateParameters["total_nodes"].get<int>());
            logger_.log(logLevel::DEBUG, "[Func: setNodeProperties] STATE: totalNodes_ set to " + to_string(totalNodes_.load()));
        }
        if (stateParameters.contains("valid_sequence_range"))
        {
            validSequenceRange_.store(stateParameters["valid_sequence_range"].get<int>());
            highWaterMark_.store(lowWaterMark_.load() + validSequenceRange_.load());
            logger_.log(logLevel::DEBUG, "[Func: setNodeProperties] STATE: validSequenceRange_ set to " + to_string(validSequenceRange_.load()) + ", highWaterMark_ computed as " + to_string(highWaterMark_.load()));
        }
        if (stateParameters.contains("fast_path_wait_time_ms"))
        {
            fastPathWaitTimeMs_.store(stateParameters["fast_path_wait_time_ms"].get<int>());
            logger_.log(logLevel::DEBUG, "[Func: setNodeProperties] STATE: fastPathWaitTimeMs_ set to " + to_string(fastPathWaitTimeMs_.load()));
        }
        logger_.log(logLevel::INFO, "[Func: setNodeProperties] EXIT - Node properties successfully set.");
    }

    ~MainNode()
    {
        logger_.log(logLevel::INFO, "[Func: ~MainNode] ENTER - Shutting down node and cleaning up resources.");
        cq_.Shutdown();
        logger_.log(logLevel::DEBUG, "[Func: ~MainNode] ACTION - Completion queue shutdown initiated.");
        if (cqThread_.joinable()) cqThread_.join();
        if (server_) 
        {
            server_->Shutdown();
            logger_.log(logLevel::DEBUG, "[Func: ~MainNode] ACTION - Server shutdown completed.");
        }
        if (nodeThread_.joinable()) nodeThread_.join();
        logger_.log(logLevel::INFO, "[Func: ~MainNode] EXIT - Cleanup completed successfully.");
    }

private:
    struct Node
    {
        string id;
        string host;
        int port;
        unique_ptr<NodeService::Stub> nodeStub;
        string publicKey;
        string fastPublicKey;
        bool byzantineVictim=false;
    };

    struct Client
    {
        string id;
        string host;
        int port;
        unique_ptr<NodeService::Stub> nodeStub;
        string publicKey;
        long long lastConsideredTransactionTimestamp = 0;
    };

    class OrchestratorServiceImpl final : public ::Orchestrator::Service
    {
    public:
        OrchestratorServiceImpl(Logger &logger, MainNode &orig) : logger_(logger), nodeOrig(orig) {}
        grpc::Status ToSetupNode(grpc::ServerContext *context,
                                            const NodeSetupRequest *request,
                                            Empty *response) override
        {
            logger_.log(logLevel::INFO, "[Func: ToSetupNode] ENTER - Received NodeSetupRequest from orchestrator.");
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: ToSetupNode] - "+request->DebugString()));
            }
            logger_.log(logLevel::DEBUG, "[Func: ToSetupNode] DEBUG - activate_node: " + to_string(request->activate_node()) + ", byzantine_node: " + to_string(request->byzantine_node()));

            nodeOrig.nodeStatus_.store(request->activate_node());
            logger_.log(logLevel::INFO, "[Func: ToSetupNode] STATE - Server Live Status Changed to " + to_string(nodeOrig.nodeStatus_.load()));

            nodeOrig.byzantineStatus_.store(request->byzantine_node());
            logger_.log(logLevel::INFO, "[Func: ToSetupNode] STATE - Server Byzantine Status Changed to " + to_string(nodeOrig.byzantineStatus_.load()));

            if (request->byzantine_node())
            {
                logger_.log(logLevel::DEBUG, "[Func: ToSetupNode] BRANCH: byzantine_node == true -> updating attacks list.");
                nodeOrig.byzantineAttacks_ = nodeOrig.RepeatedToVector(request->attacks());
                for (const auto& attack : nodeOrig.byzantineAttacks_) {
                    if (attack.type == "sign") {
                        nodeOrig.privateKey_ = nodeOrig.invalidKey_;
                        nodeOrig.fastPrivateKey_ = nodeOrig.invalidKey_;
                    }
                    else if (attack.type == "dark") {
                        nodeOrig.darkAttackActivated_ = true;
                        nodeOrig.byzantineVictims_ = attack.targetNodeIds;
                        for (const auto& i : attack.targetNodeIds) {
                            nodeOrig.nodes_[i].byzantineVictim = true;
                        }
                    }
                    else if (attack.type == "time") {
                        nodeOrig.timeAttackActivated_ = true;
                    }
                    else if (attack.type == "crash") {
                        nodeOrig.crashAttackActivated_ = true;
                    }
                    else if (attack.type == "equivocation") {
                        nodeOrig.equivocationAttackActivated_ = true;
                        for (const auto& [id, node]:nodeOrig.nodes_) {
                            if (find(attack.targetNodeIds.begin(), attack.targetNodeIds.end(), id)==attack.targetNodeIds.end()) {
                                nodeOrig.secondSet_.insert(id);
                            }
                            else nodeOrig.firstSet_.insert(id);
                        }
                    }
                }
                logger_.log(logLevel::INFO, "[Func: ToSetupNode] INFO - Server Byzantine Attacks Updated (" + to_string(nodeOrig.byzantineAttacks_.size()) + " entries)");
            }
            else
            {
                logger_.log(logLevel::DEBUG, "[Func: ToSetupNode] BRANCH: byzantine_node == false -> clearing attacks list.");
                nodeOrig.byzantineAttacks_.clear();
                nodeOrig.byzantineAttacks_.shrink_to_fit();
                logger_.log(logLevel::INFO, "[Func: ToSetupNode] INFO - Server Byzantine Attacks Cleared.");
            }

            logger_.log(logLevel::INFO, "[Func: ToSetupNode] EXIT - Node setup request processed successfully.");
            return grpc::Status::OK;
        }

        grpc::Status SendShutdown(grpc::ServerContext *context,
                                            const Empty *request,
                                            Empty *response) override
        {
            logger_.log(logLevel::INFO, "[Func: SendShutdown] ENTER - Received Node Shutdown from orchestrator.");
            nodeOrig.revertDataState();
            thread([this]() {
                nodeOrig.stopServer();
                exit(0);
            }).detach();
            return grpc::Status::OK;
        }

        grpc::Status CallHelperFunction(grpc::ServerContext *context,
                                            const HelperFunctionRequest *request,
                                            Empty *response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: CallHelperFunction] - "+request->DebugString());
            if (request->request_type() == "PrintLog")
            {
                nodeOrig.printLog();
            }
            else if (request->request_type() == "PrintDB")
            {
                nodeOrig.printDB();
            }
            else if (request->request_type() == "PrintStatus")
            {
                nodeOrig.printStatus(request->sequence_number());
            }
            else if (request->request_type() == "PrintView")
            {
                nodeOrig.printView();
            }
            return grpc::Status::OK;
        }


    private:
        Logger &logger_;
        MainNode &nodeOrig;
    };

    class NodeServiceImpl final : public ::NodeService::Service
    {
    public:
        NodeServiceImpl(Logger &logger, MainNode &orig) : logger_(logger), nodeOrig(orig) {}

        grpc::Status SendRequest(grpc::ServerContext *context,
                                            const Request *request,
                                            Empty *response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: SendRequest] - " + request->DebugString());
            logger_.log(logLevel::INFO, "[Func: SendRequest] ENTER - Received client request from " + request->client());
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendRequest] - "+request->DebugString()));
            }
            if (nodeOrig.nodeStatus_.load())
            {
                logger_.log(logLevel::DEBUG, "[Func: SendRequest] BRANCH: nodeStatus_ -> true");

                if (!bls_utils::verify_signature(nodeOrig.clients_[request->client()].publicKey, request->signature(), nodeOrig.serializeDeterministic(request->request_content()))) {
                    logger_.log(logLevel::WARN, "[Func: SendRequest] BRANCH: Signature verification failed for client " + request->client());
                    logger_.log(logLevel::INFO, "[Func: SendRequest] EXIT - Returning CANCELLED due to signature failure.");
                    return grpc::Status::CANCELLED;
                }

                logger_.log(logLevel::INFO, "[Func: SendRequest] INFO - Request signature verified successfully for " + request->client());
                logger_.log(logLevel::DEBUG, "[Func: SendRequest] STATE - timestamp: " + to_string(request->request_content().timestamp()));

                {
                    lock_guard<mutex> lg(nodeOrig.clientTransactionsExecutionLogMx_);
                    auto it = nodeOrig.clientTransactionsExecutionLog_.find({request->client(), request->request_content().timestamp()});
                    if (it != nodeOrig.clientTransactionsExecutionLog_.end()) {
                        logger_.log(logLevel::DEBUG, "[Func: SendRequest] BRANCH: Request already executed, resending reply to client " + request->client());
                        nodeOrig.sendReplyToClient(*it->second, request->client());
                        logger_.log(logLevel::INFO, "[Func: SendRequest] EXIT - Returning OK after resending reply.");
                        return grpc::Status::OK;
                    }
                }

                {
                    lock_guard<mutex> lk(nodeOrig.clientsMx_);
                    auto it = nodeOrig.clients_.find(request->client());
                    if (it != nodeOrig.clients_.end()) {
                        auto* clientMetadata = &it->second;
                        logger_.log(logLevel::DEBUG, "[Func: SendRequest] STATE - Located client metadata for " + request->client());
                        {
                            if (request->request_content().timestamp() <= clientMetadata->lastConsideredTransactionTimestamp) {
                                logger_.log(logLevel::WARN, "[Func: SendRequest] BRANCH: Request timestamp older than lastConsideredTransactionTimestamp for client " + request->client());
                                logger_.log(logLevel::INFO, "[Func: SendRequest] EXIT - Returning CANCELLED due to outdated timestamp.");
                                return grpc::Status::CANCELLED;
                            }
                        }
                    }
                }

                if (nodeOrig.isViewStable_.load() == false) {
                    logger_.log(logLevel::DEBUG, "[Func: SendRequest] BRANCH: isViewStable_ == false -> Queuing request in pendingRequest_");
                    {
                        lock_guard<mutex> lg(nodeOrig.pendingRequestMx_);
                        nodeOrig.pendingRequest_.push_back(make_shared<Request>(*request));
                        logger_.log(logLevel::DEBUG, "[Func: SendRequest] STATE - pendingRequest_ size increased");
                    }
                    logger_.log(logLevel::INFO, "[Func: SendRequest] EXIT - Returning OK after queuing request (view not stable).");
                    return grpc::Status::OK;
                }

                if (request->request_content().operation().type() == READ_ONLY) {
                    logger_.log(logLevel::DEBUG, "[Func: SendRequest] BRANCH: Request is READ_ONLY");
                    logger_.log(logLevel::INFO, "[Func: SendRequest] ACTION - Enqueuing processReadOnlyRequest to threadPool_");
                    nodeOrig.threadPool_.enqueue([this, requestCopy = *request]() {
                        this->nodeOrig.processReadOnlyRequest(requestCopy);
                    });
                }
                else {
                    logger_.log(logLevel::DEBUG, "[Func: SendRequest] BRANCH: Request is READ_WRITE (or promoted).");
                    if (nodeOrig.isLeader_.load()) {
                        logger_.log(logLevel::DEBUG, "[Func: SendRequest] BRANCH: Node is leader, processing request directly.");
                        {
                            lock_guard<mutex> lk(nodeOrig.clientsMx_);
                            auto it = nodeOrig.clients_.find(request->client());
                            if (it != nodeOrig.clients_.end()) {
                                auto* clientMetadata = &it->second;
                                logger_.log(logLevel::DEBUG, "[Func: SendRequest] STATE - Updating lastConsideredTransactionTimestamp for READ_WRITE request.");
                                {
                                    if (request->request_content().timestamp() > clientMetadata->lastConsideredTransactionTimestamp) {
                                        clientMetadata->lastConsideredTransactionTimestamp = request->request_content().timestamp();
                                    }
                                    else return grpc::Status::CANCELLED;
                                }
                            }
                        }
                        logger_.log(logLevel::INFO, "[Func: SendRequest] ACTION - Enqueuing handleRequest() in threadPool_ for client " + request->client());
                        nodeOrig.threadPool_.enqueue([this, requestCopy = *request]() {
                            this->nodeOrig.handleRequest(requestCopy);
                        });
                    }
                    else {
                        logger_.log(logLevel::DEBUG, "[Func: SendRequest] BRANCH: Node is not leader, forwarding request to leader.");
                        nodeOrig.forwardRequestToLeader(*request);
                        logger_.log(logLevel::DEBUG, "[Func: SendRequest] ACTION - Forwarded request to leader.");
                    }

                    if (!nodeOrig.timer_.is_running()) {
                        logger_.log(logLevel::DEBUG, "[Func: SendRequest] BRANCH: Timer not running -> starting timeout timer.");
                        nodeOrig.timer_.start(milliseconds(nodeOrig.timeoutMs_.load()), [this]() {
                            this->nodeOrig.isViewStable_.store(false);
                            this->nodeOrig.initiateViewChange();
                        });
                    }
                }

                logger_.log(logLevel::INFO, "[Func: SendRequest] EXIT - Request processed successfully, returning OK.");
                return grpc::Status::OK;
            }
            else
            {
                logger_.log(logLevel::WARN, "[Func: SendRequest] BRANCH: nodeStatus_ -> false (server inactive)");
                logger_.log(logLevel::INFO, "[Func: SendRequest] EXIT - Returning CANCELLED due to inactive node.");
                return grpc::Status::CANCELLED;
            }
        }

        grpc::Status SendPrePrepare(grpc::ServerContext* context,
                                    const PrePrepare* prePrepare,
                                    Empty* response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: SendPrePrepare] - " + prePrepare->DebugString());
            logger_.log(logLevel::INFO, "[Func: SendPrePrepare] ENTER - Received PrePrepare message from node " + prePrepare->node());
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendPrePrepare] - "+prePrepare->DebugString()));
            }
            if (!nodeOrig.nodeStatus_.load() || !nodeOrig.isViewStable_.load() || nodeOrig.isLeader_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendPrePrepare] BRANCH: Node inactive, view unstable, or is leader -> rejecting PrePrepare.");
                logger_.log(logLevel::INFO, "[Func: SendPrePrepare] EXIT - Returning CANCELLED due to invalid receiver state.");
                return grpc::Status::CANCELLED;
            }

            {
                const auto& req = prePrepare->message();
                logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] STATE - Validating embedded client request from client " + req.client());
                auto cit = nodeOrig.clients_.find(req.client());
                if (cit == nodeOrig.clients_.end() ||
                    !bls_utils::verify_signature(cit->second.publicKey,
                                    req.signature(),
                                    nodeOrig.serializeDeterministic(req.request_content()))) {
                    logger_.log(logLevel::ERROR, "[Func: SendPrePrepare] ERROR - Client verification failed for " + req.client());
                    logger_.log(logLevel::INFO, "[Func: SendPrePrepare] EXIT - Returning CANCELLED due to invalid client signature.");
                    return grpc::Status::CANCELLED;
                }
                logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] INFO - Client signature verified successfully for " + req.client());
            }

            bool valid = true;
            logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] STATE - Valid flag initialized to true.");

            {
                lock_guard<mutex> l(nodeOrig.leaderMx_);
                logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] ACTION - Acquired leaderMx_ lock to verify sender node " + prePrepare->node());
                auto nit = nodeOrig.nodes_.find(nodeOrig.leaderId_);
                if (nit == nodeOrig.nodes_.end() ||
                    !bls_utils::verify_signature(nit->second.publicKey,
                                    prePrepare->signature(),
                                    nodeOrig.serializeDeterministic(prePrepare->pre_prepare_content()))) {
                    logger_.log(logLevel::ERROR, "[Func: SendPrePrepare] ERROR - Verification failed for leader or sender node " + prePrepare->node());
                    valid = false;
                } else {
                    logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] INFO - Sender node verification successful.");
                }
            }

            const auto& cc = prePrepare->pre_prepare_content();
            logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] STATE - Extracted PrePrepare content for validation (seq=" + to_string(cc.sequence_number()) + ", view=" + to_string(cc.view_number()) + ").");

            if (nodeOrig.computeDigest(nodeOrig.serializeDeterministic(prePrepare->message())) != cc.message_digest()) {
                logger_.log(logLevel::ERROR, "[Func: SendPrePrepare] ERROR - Digest mismatch in PrePrepare from node " + prePrepare->node());
                valid = false;
            }
            if (nodeOrig.currentView_.load() != cc.view_number()) {
                logger_.log(logLevel::WARN, "[Func: SendPrePrepare] BRANCH: View number mismatch (expected " + to_string(nodeOrig.currentView_.load()) + ", got " + to_string(cc.view_number()) + ")");
                valid = false;
            }
            if (cc.sequence_number() <= nodeOrig.lowWaterMark_.load() ||
                cc.sequence_number() >  nodeOrig.highWaterMark_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendPrePrepare] BRANCH: Sequence number out of bounds (" + to_string(cc.sequence_number()) + ")");
                valid = false;
            }

            shared_ptr<MainNode::PBFTLogEntry> entry;
            {
                lock_guard<mutex> lk(nodeOrig.sequenceLogMapMx_);
                logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] ACTION - Checking for existing PBFTLogEntry with seq=" + to_string(cc.sequence_number()));
                auto it = nodeOrig.sequenceLogMap_.find(cc.sequence_number());
                if (it != nodeOrig.sequenceLogMap_.end()) {
                    entry = it->second;
                    logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] STATE - Found existing PBFTLogEntry for seq=" + to_string(cc.sequence_number()));
                }
            }

            if (entry != nullptr)
            {
                lock_guard<mutex> lg(entry->mtx);
                logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] ACTION - Lock acquired for PBFTLogEntry, checking conflicts.");
                const bool has_conflict =
                    entry->is_prepared ||
                    (cc.view_number() <= entry->view_number &&
                    entry->pre_prepare_data.pre_prepare != nullptr &&
                    entry->request_digest != cc.message_digest());

                if (has_conflict) {
                    logger_.log(logLevel::WARN, "[Func: SendPrePrepare] BRANCH: Conflict detected for seq=" + to_string(cc.sequence_number()));
                    valid = false;
                } else {
                    logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] STATE - No conflict found for seq=" + to_string(cc.sequence_number()));
                }
            }

            if (!valid) {
                logger_.log(logLevel::WARN, "[Func: SendPrePrepare] BRANCH: Validation failed -> initiating view change timer if not active.");
                if (!nodeOrig.timer_.is_running()) {
                    logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] ACTION - Starting view change timer.");
                    nodeOrig.timer_.start(milliseconds(nodeOrig.timeoutMs_.load()), [this] {
                        this->nodeOrig.isViewStable_.store(false);
                        this->nodeOrig.initiateViewChange();
                    });
                }
                logger_.log(logLevel::INFO, "[Func: SendPrePrepare] EXIT - Returning CANCELLED due to invalid PrePrepare.");
                return grpc::Status::CANCELLED;
            }

            Prepare prepare;
            prepare.mutable_prepare_content()->CopyFrom(cc);
            prepare.mutable_prepare_content()->set_message_type(PREPARE);
            prepare.set_node(nodeOrig.id_);
            logger_.log(logLevel::INFO, "[Func: SendPrePrepare] ACTION - Constructing Prepare message for seq=" + to_string(cc.sequence_number()));
            logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] STATE - Prepare Debug: " + prepare.prepare_content().DebugString());
            logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] STATE - Prepare Serialized: " + prepare.prepare_content().SerializeAsString());
            prepare.set_signature(bls_utils::sign_data(nodeOrig.privateKey_, nodeOrig.serializeDeterministic(prepare.prepare_content())));
            prepare.set_fast_path_signature(bls_utils::sign_data(nodeOrig.fastPrivateKey_, nodeOrig.serializeDeterministic(prepare.prepare_content())));
            logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] ACTION - Signatures generated for Prepare.");

            if (!entry) {
                logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] BRANCH: Creating new PBFTLogEntry for seq=" + to_string(cc.sequence_number()));
                entry = make_shared<MainNode::PBFTLogEntry>();
                {
                    lock_guard<mutex> lk(nodeOrig.sequenceLogMapMx_);
                    nodeOrig.sequenceLogMap_[cc.sequence_number()] = entry;
                    logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] STATE - Added new PBFTLogEntry to sequenceLogMap_.");
                }
                {
                    lock_guard<mutex> lock(entry->mtx);
                    entry->sequence_number = cc.sequence_number();
                }
            }
            {
                {
                    lock_guard<mutex> lock(entry->mtx);
                    logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] ACTION - Populating PBFTLogEntry fields.");
                    entry->view_number     = nodeOrig.currentView_.load();
                    entry->client_request  = make_shared<Request>(prePrepare->message());
                    entry->request_digest  = prePrepare->pre_prepare_content().message_digest();
                }
                {
                    lock_guard<mutex> lg(entry->pre_prepare_data.pre_prepare_mtx);
                    entry->pre_prepare_data.pre_prepare = make_shared<PrePrepare>(*prePrepare);
                }
                {
                    lock_guard<mutex> lg(entry->prepare_data.prepare_mtx);
                    entry->prepare_data.local_prepare   = make_shared<Prepare>(prepare);
                }
            }

            logger_.log(logLevel::INFO, "[Func: SendPrePrepare] ACTION - Sending Prepare message asynchronously to node " + prePrepare->node());
            logger_.log(logLevel::INFO, "[Func: SendPrePrepare] STATE - Signature used in prepare message " + prepare.fast_path_signature() +" and node "+prepare.node());
            logger_.log(logLevel::INFO, "[Func: SendPrePrepare] STATE - Content signed in prepare message " + prepare.prepare_content().SerializeAsString());
            logger_.log(logLevel::INFO, "[Func: SendPrePrepare] STATE - Keys used to sign in prepare message " + nodeOrig.privateKey_ + " | " +nodeOrig.fastPrivateKey_);
            
            if (nodeOrig.byzantineStatus_.load() && nodeOrig.darkAttackActivated_ && nodeOrig.nodes_[prePrepare->node()].byzantineVictim) {
                logger_.log(logLevel::INFO, "[Func: SendPrePrepare] ACTION - Skipping sending Prepare message to node " + prePrepare->node());
            }
            else if (nodeOrig.byzantineStatus_.load() && nodeOrig.crashAttackActivated_) {
                logger_.log(logLevel::INFO, "[Func: SendPrePrepare] ACTION - Avoiding sending Prepare message to node " + prePrepare->node());
            }
            else {
                {
                    lock_guard<mutex> l(nodeOrig.logMx_);
                    nodeOrig.log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: SendPrePrepare] - "+prepare.DebugString()));
                }
                auto* call = new MainNode::AsyncNodeCall;
                call->response_reader = nodeOrig.nodes_[prePrepare->node()].nodeStub->AsyncSendPrepare(&call->context, prepare, &nodeOrig.cq_);
                call->response_reader->Finish(&call->reply, &call->status, (void*)call);
            }

            {
                int old_val = nodeOrig.sequenceNumber_.load();
                while (true) {
                    int desired = max(old_val, cc.sequence_number());
                    if (nodeOrig.sequenceNumber_.compare_exchange_weak(old_val, desired)) {
                        logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] STATE - sequenceNumber_ updated to " + to_string(desired));
                        break;
                    }
                }
            }

            if (!nodeOrig.timer_.is_running()) {
                logger_.log(logLevel::DEBUG, "[Func: SendPrePrepare] ACTION - Starting view change timer after successful Prepare handling.");
                nodeOrig.timer_.start(milliseconds(nodeOrig.timeoutMs_.load()), [this] {
                    this->nodeOrig.isViewStable_.store(false);
                    this->nodeOrig.initiateViewChange();
                });
            }

            logger_.log(logLevel::INFO, "[Func: SendPrePrepare] EXIT - PrePrepare processed successfully, returning OK.");
            return grpc::Status::OK;
        }

        grpc::Status SendPrepare(grpc::ServerContext* context,
                                    const Prepare* prepare,
                                    Empty* response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: SendPrepare] - " + prepare->DebugString());
            logger_.log(logLevel::INFO, "[Func: SendPrepare] ENTER - Received Prepare message from node " + prepare->node());
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendPrepare] - "+prepare->DebugString()));
            }
            if (!nodeOrig.nodeStatus_.load() || !nodeOrig.isViewStable_.load() || !nodeOrig.isLeader_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendPrepare] BRANCH: Node inactive, view unstable, or not leader -> rejecting Prepare.");
                logger_.log(logLevel::INFO, "[Func: SendPrepare] EXIT - Returning CANCELLED due to invalid receiver state.");
                return grpc::Status::CANCELLED;
            }

            logger_.log(logLevel::DEBUG, "[Func: SendPrepare] STATE - Received Prepare content: " + prepare->prepare_content().DebugString());
            logger_.log(logLevel::DEBUG, "[Func: SendPrepare] STATE - Serialized Prepare: " + prepare->prepare_content().SerializeAsString());

            auto nit = nodeOrig.nodes_.find(prepare->node());
            if (nit == nodeOrig.nodes_.end() || !bls_utils::verify_signature(nit->second.publicKey, prepare->signature(), nodeOrig.serializeDeterministic(prepare->prepare_content()))) {
                logger_.log(logLevel::ERROR, "[Func: SendPrepare] ERROR - Signature verification failed for sender node " + prepare->node());
                logger_.log(logLevel::INFO, "[Func: SendPrepare] EXIT - Returning CANCELLED due to invalid signature.");
                return grpc::Status::CANCELLED;
            }
            logger_.log(logLevel::DEBUG, "[Func: SendPrepare] INFO - Primary signature verified successfully for node " + prepare->node());

            if (nit == nodeOrig.nodes_.end() || !bls_utils::verify_signature(nit->second.fastPublicKey, prepare->fast_path_signature(), nodeOrig.serializeDeterministic(prepare->prepare_content()))) {
                logger_.log(logLevel::ERROR, "[Func: SendPrepare] ERROR - Fast path signature verification failed for node " + prepare->node());
                logger_.log(logLevel::INFO, "[Func: SendPrepare] EXIT - Returning CANCELLED due to invalid fast path signature.");
                return grpc::Status::CANCELLED;
            }
            logger_.log(logLevel::DEBUG, "[Func: SendPrepare] INFO - Fast path signature verified successfully for node " + prepare->node());

            const auto& cc = prepare->prepare_content();
            logger_.log(logLevel::DEBUG, "[Func: SendPrepare] STATE - Prepare content extracted (seq=" + to_string(cc.sequence_number()) + ", view=" + to_string(cc.view_number()) + ").");

            if (nodeOrig.currentView_.load() != cc.view_number()) {
                logger_.log(logLevel::WARN, "[Func: SendPrepare] BRANCH: View number mismatch (expected " + to_string(nodeOrig.currentView_.load()) + ", got " + to_string(cc.view_number()) + ")");
                logger_.log(logLevel::INFO, "[Func: SendPrepare] EXIT - Returning CANCELLED due to view mismatch.");
                return grpc::Status::CANCELLED;
            }
            if (cc.sequence_number() <= nodeOrig.lowWaterMark_.load() || cc.sequence_number() > nodeOrig.highWaterMark_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendPrepare] BRANCH: Sequence number out of bounds (" + to_string(cc.sequence_number()) + ")");
                logger_.log(logLevel::INFO, "[Func: SendPrepare] EXIT - Returning CANCELLED due to invalid sequence number.");
                return grpc::Status::CANCELLED;
            }

            shared_ptr<MainNode::PBFTLogEntry> entry;
            {
                lock_guard<mutex> lg(nodeOrig.sequenceLogMapMx_);
                logger_.log(logLevel::DEBUG, "[Func: SendPrepare] ACTION - Checking for PBFTLogEntry with seq=" + to_string(cc.sequence_number()));
                auto it = nodeOrig.sequenceLogMap_.find(cc.sequence_number());
                if (it != nodeOrig.sequenceLogMap_.end()) {
                    entry = it->second;
                    logger_.log(logLevel::DEBUG, "[Func: SendPrepare] STATE - Existing PBFTLogEntry found for seq=" + to_string(cc.sequence_number()));
                }
                else {
                    logger_.log(logLevel::WARN, "[Func: SendPrepare] BRANCH: No PBFTLogEntry found for seq=" + to_string(cc.sequence_number()));
                    logger_.log(logLevel::INFO, "[Func: SendPrepare] EXIT - Returning CANCELLED due to missing entry.");
                    return grpc::Status::CANCELLED;
                }
            }

            {
                lock_guard<mutex> lg(entry->mtx);
                logger_.log(logLevel::DEBUG, "[Func: SendPrepare] ACTION - Locked PBFTLogEntry for conflict validation.");
                const bool has_conflict =
                    entry->sequence_number != cc.sequence_number() ||
                    cc.view_number() != entry->view_number ||
                    entry->pre_prepare_data.pre_prepare == nullptr ||
                    entry->request_digest != cc.message_digest();

                if (has_conflict) {
                    logger_.log(logLevel::WARN, "[Func: SendPrepare] BRANCH: Conflict detected with existing log entry (seq=" + to_string(cc.sequence_number()) + ")");
                    logger_.log(logLevel::INFO, "[Func: SendPrepare] EXIT - Returning CANCELLED due to conflict.");
                    return grpc::Status::CANCELLED;
                }
                logger_.log(logLevel::DEBUG, "[Func: SendPrepare] STATE - No conflicts found for seq=" + to_string(cc.sequence_number()));
            }

            bool trigger_async = false;
            {
                lock_guard<mutex> lg(entry->prepare_data.prepare_mtx);
                logger_.log(logLevel::DEBUG, "[Func: SendPrepare] ACTION - Locked prepare_data for updating Prepare records.");
                if (find(entry->prepare_data.nodes_received.begin(), entry->prepare_data.nodes_received.end(), prepare->node())!=entry->prepare_data.nodes_received.end())
                {
                    logger_.log(logLevel::WARN, "[Func: SendPrepare] BRANCH: Duplicate Prepare received from node " + prepare->node());
                    logger_.log(logLevel::INFO, "[Func: SendPrepare] EXIT - Returning CANCELLED due to duplicate.");
                    return grpc::Status::CANCELLED;
                }

                entry->prepare_data.valid_prepares_received.push_back(make_shared<Prepare>(*prepare));
                entry->prepare_data.valid_prepare_count++;
                entry->prepare_data.agg_signatures.push_back(prepare->signature());
                entry->prepare_data.fast_path_agg_signatures.push_back(prepare->fast_path_signature());
                entry->prepare_data.nodes_received.push_back(prepare->node());
                logger_.log(logLevel::DEBUG, "[Func: SendPrepare] STATE - Updated prepare_data: count=" + to_string(entry->prepare_data.valid_prepare_count) +
                    ", nodes_received=" + to_string(entry->prepare_data.nodes_received.size()));

                if (entry->prepare_data.valid_prepare_count >= nodeOrig.quorumSize_.load()) {
                    logger_.log(logLevel::DEBUG, "[Func: SendPrepare] BRANCH: Quorum reached (>= " + to_string(nodeOrig.quorumSize_.load()) + ")");
                    if (!entry->prepare_data.has_quorum) {
                        entry->prepare_data.has_quorum = true;
                        entry->prepare_data.quorum_nodes = entry->prepare_data.nodes_received;
                        entry->prepare_data.quorum_signatures = entry->prepare_data.agg_signatures;
                        if (nodeOrig.isLeader_.load() && nodeOrig.byzantineStatus_.load() && nodeOrig.crashAttackActivated_) {
                            logger_.log(logLevel::INFO, "[Func: SendPrepare] INFO - Not sending CollectedPrepare although quorum established for seq=" + to_string(cc.sequence_number()));
                        }
                        else {
                            trigger_async = true;
                            logger_.log(logLevel::INFO, "[Func: SendPrepare] INFO - Prepare quorum established for seq=" + to_string(cc.sequence_number()));
                        }
                    }
                    if (entry->prepare_data.valid_prepare_count == nodeOrig.totalNodes_.load()) {
                        entry->prepare_data.has_total_consensus = true;
                        logger_.log(logLevel::INFO, "[Func: SendPrepare] INFO - Total consensus reached for seq=" + to_string(cc.sequence_number()));
                    }
                }
            }

            if (trigger_async) {
                logger_.log(logLevel::INFO, "[Func: SendPrepare] ACTION - Quorum met, enqueuing handleCollectedPrepareAsync.");
                nodeOrig.threadPool_.enqueue([this, entry]() {
                    this->nodeOrig.handleCollectedPrepareAsync(entry);
                });
            }

            logger_.log(logLevel::INFO, "[Func: SendPrepare] EXIT - Prepare processed successfully, returning OK.");
            return grpc::Status::OK;
        }

        grpc::Status SendCollectedPrepare(grpc::ServerContext* context,
                                    const CollectedPrepare* collectedPrepare,
                                    Empty* response) override
        {
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendCollectedPrepare] - "+collectedPrepare->DebugString()));
            }
            if (!nodeOrig.nodeStatus_.load() || !nodeOrig.isViewStable_.load() || nodeOrig.isLeader_.load()) {
                return grpc::Status::CANCELLED;
            }
            logger_.log(logLevel::INFO, "Received CP: "+collectedPrepare->prepare_content().DebugString());
            logger_.log(logLevel::INFO, "Received CP Serial: "+collectedPrepare->prepare_content().SerializeAsString());
            {
                lock_guard<mutex> lk(nodeOrig.leaderMx_);
                if (nodeOrig.leaderId_ != collectedPrepare->node()) {
                    logger_.log(logLevel::INFO, "Collected Prepare received from invalid node: " + collectedPrepare->node());
                    return grpc::Status::CANCELLED;
                }
            }

            if (collectedPrepare->is_fast_path()) {
                if (!bls_utils::verify_signature(nodeOrig.aggFastPublicKey_, collectedPrepare->signature(), nodeOrig.serializeDeterministic(collectedPrepare->prepare_content()))) {
                    logger_.log(logLevel::INFO, "Verification failed for fast path aggregated signature for node " + collectedPrepare->node());
                    logger_.log(logLevel::INFO, "Verification done using fast path agg public key " + nodeOrig.aggFastPublicKey_);
                    logger_.log(logLevel::INFO, "Signature received was " + collectedPrepare->signature());
                    return grpc::Status::CANCELLED;
                }
            }
            else if (!bls_utils::verify_signature(nodeOrig.aggPublicKey_, collectedPrepare->signature(), nodeOrig.serializeDeterministic(collectedPrepare->prepare_content()))) {
                logger_.log(logLevel::INFO, "Verification failed for aggregated signature for node " + collectedPrepare->node());
                logger_.log(logLevel::INFO, "Verification done using agg public key " + nodeOrig.aggPublicKey_);
                logger_.log(logLevel::INFO, "Signature received was " + collectedPrepare->signature());
                return grpc::Status::CANCELLED;
            }

            const auto& cc = collectedPrepare->prepare_content();

            if (nodeOrig.currentView_.load() != cc.view_number()) {
                logger_.log(logLevel::INFO, "View number mismatch in Collected Prepare from node " + collectedPrepare->node());
                return grpc::Status::CANCELLED;
            }
            if (cc.sequence_number() <= nodeOrig.lowWaterMark_.load() ||
                cc.sequence_number() >  nodeOrig.highWaterMark_.load()) {
                logger_.log(logLevel::INFO, "Sequence number out of bounds in Collected Prepare from node " + collectedPrepare->node());
                return grpc::Status::CANCELLED;
            }

            shared_ptr<MainNode::PBFTLogEntry> entry;
            {
                lock_guard<mutex> lk(nodeOrig.sequenceLogMapMx_);
                auto it = nodeOrig.sequenceLogMap_.find(cc.sequence_number());
                if (it != nodeOrig.sequenceLogMap_.end()) {
                    entry = it->second;
                }
                else {
                    return grpc::Status::CANCELLED;
                }
            }

            {
                lock_guard<mutex> lg(entry->mtx);
                const bool has_conflict =
                    entry->sequence_number != cc.sequence_number() ||
                    cc.view_number() != entry->view_number ||
                    entry->pre_prepare_data.pre_prepare == nullptr ||
                    entry->request_digest != cc.message_digest();

                if (has_conflict) {
                    logger_.log(logLevel::INFO, "Conflict with existing log entry for sequence number: " + to_string(cc.sequence_number()));
                    return grpc::Status::CANCELLED;
                }
            }

            {
                lock_guard<mutex> lg(entry->prepare_data.prepare_mtx);
                entry->prepare_data.collected_prepare = make_shared<CollectedPrepare>(*collectedPrepare);
            }
            bool sendForClientRequest = false;
            if (nodeOrig.byzantineStatus_.load() && nodeOrig.crashAttackActivated_) {
                logger_.log(logLevel::INFO, "CP : Avoiding updating its status to be PREPARED and sending COMMIT/executing requests.");
            }
            else {
                string client;
                long long timestamp;
                bool proceed = false;
                {
                    lock_guard<mutex> lk(entry->mtx);
                    if (entry->request_digest != "NULL") {
                        client = entry->client_request->client();
                        timestamp = entry->client_request->request_content().timestamp();
                        proceed = true;
                        if (entry->client_request == nullptr) sendForClientRequest=true;
                    }
                }

                if (proceed) {
                    lock_guard<mutex> lk(nodeOrig.clientsMx_);
                    auto it = nodeOrig.clients_.find(client);
                    if (it != nodeOrig.clients_.end()) {
                        auto* clientMetadata = &it->second;
                        logger_.log(logLevel::DEBUG, "[Func: SendCollectedPrepare] ACTION - Updating client's lastConsideredTransactionTimestamp.");
                        {
                            if (timestamp > clientMetadata->lastConsideredTransactionTimestamp) {
                                clientMetadata->lastConsideredTransactionTimestamp = timestamp;
                                logger_.log(logLevel::DEBUG, "[Func: SendCollectedPrepare] STATE - Updated timestamp for client " + client);
                            }
                        }
                    }
                }

                

                if (collectedPrepare->is_fast_path()) {
                    if (sendForClientRequest) {
                        bool still_needed = false;
                        {
                            lock_guard<mutex> lk(entry->mtx);
                            if (entry->client_request == nullptr) still_needed=true;
                        }
                        if (still_needed) nodeOrig.requestClientMessage(cc.sequence_number());
                        {
                            string client;
                            long long timestamp;
                            {
                                lock_guard<mutex> lk(entry->mtx);
                                client = entry->client_request->client();
                                timestamp = entry->client_request->request_content().timestamp();
                            }
                            
                            lock_guard<mutex> lk(nodeOrig.clientsMx_);
                            auto it = nodeOrig.clients_.find(client);
                            if (it != nodeOrig.clients_.end()) {
                                auto* clientMetadata = &it->second;
                                logger_.log(logLevel::DEBUG, "[Func: SendCollectedPrepare] ACTION - Updating client's lastConsideredTransactionTimestamp.");
                                {
                                    if (timestamp > clientMetadata->lastConsideredTransactionTimestamp) {
                                        clientMetadata->lastConsideredTransactionTimestamp = timestamp;
                                        logger_.log(logLevel::DEBUG, "[Func: SendCollectedPrepare] STATE - Updated timestamp for client " + client);
                                    }
                                }
                            }
                        }
                    }

                    {
                        lock_guard<mutex> lk(entry->mtx);
                        entry->is_fast_path = true;
                        entry->is_prepared = true;
                        entry->is_ready_for_execution = true;
                    }
                    nodeOrig.threadPool_.enqueue([this, sequence_number = cc.sequence_number()]() {
                        this->nodeOrig.executeSequence(sequence_number);
                    });
                }
                else {
                    {
                        lock_guard<mutex> lk(entry->mtx);
                        entry->is_prepared = true;
                    }
                    Commit commit;
                    commit.mutable_commit_content()->CopyFrom(cc);
                    commit.mutable_commit_content()->set_message_type(COMMIT);
                    commit.set_node(nodeOrig.id_);
                    commit.set_signature(bls_utils::sign_data(nodeOrig.privateKey_, nodeOrig.serializeDeterministic(commit.commit_content())));
                    
                    {
                        lock_guard<mutex> lk(entry->commit_data.commit_mtx);
                        entry->commit_data.local_commit = make_shared<Commit>(commit);
                    }
                    
                    if (nodeOrig.byzantineStatus_.load() && nodeOrig.darkAttackActivated_ && nodeOrig.nodes_[collectedPrepare->node()].byzantineVictim) {
                        logger_.log(logLevel::INFO, "CP : Skipping sending commit to "+collectedPrepare->node());
                    }
                    else {
                        {
                            lock_guard<mutex> l(nodeOrig.logMx_);
                            nodeOrig.log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: SendCollectedPrepare] - "+commit.DebugString()));
                        }
                        auto* call = new MainNode::AsyncNodeCall;
                        call->response_reader = nodeOrig.nodes_[collectedPrepare->node()].nodeStub->AsyncSendCommit(&call->context, commit, &nodeOrig.cq_);
                        call->response_reader->Finish(&call->reply, &call->status, (void*)call);
                    }

                    if (sendForClientRequest) {
                        bool still_needed = false;
                        {
                            lock_guard<mutex> lk(entry->mtx);
                            if (entry->client_request == nullptr) still_needed=true;
                        }
                        nodeOrig.requestClientMessage(cc.sequence_number());
                        {
                            string client;
                            long long timestamp;
                            {
                                lock_guard<mutex> lk(entry->mtx);
                                client = entry->client_request->client();
                                timestamp = entry->client_request->request_content().timestamp();
                            }
                            lock_guard<mutex> lk(nodeOrig.clientsMx_);
                            auto it = nodeOrig.clients_.find(client);
                            if (it != nodeOrig.clients_.end()) {
                                auto* clientMetadata = &it->second;
                                logger_.log(logLevel::DEBUG, "[Func: SendCollectedPrepare] ACTION - Updating client's lastConsideredTransactionTimestamp.");
                                {
                                    if (timestamp > clientMetadata->lastConsideredTransactionTimestamp) {
                                        clientMetadata->lastConsideredTransactionTimestamp = timestamp;
                                        logger_.log(logLevel::DEBUG, "[Func: SendCollectedPrepare] STATE - Updated timestamp for client " + client);
                                    }
                                }
                            }
                        }
                    }

                }
            }

            {
                int old_val = nodeOrig.sequenceNumber_.load();
                while (true) {
                    int desired = max(old_val, cc.sequence_number());
                    if (nodeOrig.sequenceNumber_.compare_exchange_weak(old_val, desired))
                        break;
                }
            }

            return grpc::Status::OK;
        }

        grpc::Status SendCommit(grpc::ServerContext* context,
                                    const Commit* commit,
                                    Empty* response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: SendCommit] - " + commit->DebugString());
            logger_.log(logLevel::INFO, "[Func: SendCommit] ENTER - Received Commit message from node " + commit->node());
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendCommit] - "+commit->DebugString()));
            }

            if (!nodeOrig.nodeStatus_.load() || !nodeOrig.isViewStable_.load() || !nodeOrig.isLeader_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendCommit] BRANCH: Node inactive, view unstable, or not leader -> rejecting Commit.");
                logger_.log(logLevel::INFO, "[Func: SendCommit] EXIT - Returning CANCELLED due to invalid receiver state.");
                return grpc::Status::CANCELLED;
            }

            auto nit = nodeOrig.nodes_.find(commit->node());
            if (nit == nodeOrig.nodes_.end() || !bls_utils::verify_signature(nit->second.publicKey, commit->signature(), nodeOrig.serializeDeterministic(commit->commit_content()))) {
                logger_.log(logLevel::ERROR, "[Func: SendCommit] ERROR - Signature verification failed for sender node " + commit->node());
                logger_.log(logLevel::INFO, "[Func: SendCommit] EXIT - Returning CANCELLED due to invalid commit signature.");
                return grpc::Status::CANCELLED;
            }
            logger_.log(logLevel::DEBUG, "[Func: SendCommit] INFO - Signature verified successfully for node " + commit->node());

            const auto& cc = commit->commit_content();
            logger_.log(logLevel::DEBUG, "[Func: SendCommit] STATE - Commit content extracted (seq=" + to_string(cc.sequence_number()) + ", view=" + to_string(cc.view_number()) + ").");

            if (nodeOrig.currentView_.load() != cc.view_number()) {
                logger_.log(logLevel::WARN, "[Func: SendCommit] BRANCH: View number mismatch (expected " + to_string(nodeOrig.currentView_.load()) + ", got " + to_string(cc.view_number()) + ")");
                logger_.log(logLevel::INFO, "[Func: SendCommit] EXIT - Returning CANCELLED due to view mismatch.");
                return grpc::Status::CANCELLED;
            }
            if (cc.sequence_number() <= nodeOrig.lowWaterMark_.load() || cc.sequence_number() > nodeOrig.highWaterMark_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendCommit] BRANCH: Sequence number out of bounds (" + to_string(cc.sequence_number()) + ")");
                logger_.log(logLevel::INFO, "[Func: SendCommit] EXIT - Returning CANCELLED due to invalid sequence number.");
                return grpc::Status::CANCELLED;
            }

            shared_ptr<MainNode::PBFTLogEntry> entry;
            {
                lock_guard<mutex> lk(nodeOrig.sequenceLogMapMx_);
                logger_.log(logLevel::DEBUG, "[Func: SendCommit] ACTION - Checking for PBFTLogEntry with seq=" + to_string(cc.sequence_number()));
                auto it = nodeOrig.sequenceLogMap_.find(cc.sequence_number());
                if (it != nodeOrig.sequenceLogMap_.end()) {
                    entry = it->second;
                    logger_.log(logLevel::DEBUG, "[Func: SendCommit] STATE - Existing PBFTLogEntry found for seq=" + to_string(cc.sequence_number()));
                }
                else {
                    logger_.log(logLevel::WARN, "[Func: SendCommit] BRANCH: No PBFTLogEntry found for seq=" + to_string(cc.sequence_number()));
                    logger_.log(logLevel::INFO, "[Func: SendCommit] EXIT - Returning CANCELLED due to missing entry.");
                    return grpc::Status::CANCELLED;
                }
            }

            {
                lock_guard<mutex> lg(entry->mtx);
                logger_.log(logLevel::DEBUG, "[Func: SendCommit] ACTION - Locked PBFTLogEntry for validation.");
                const bool has_conflict =
                    entry->sequence_number != cc.sequence_number() ||
                    cc.view_number() != entry->view_number ||
                    entry->pre_prepare_data.pre_prepare == nullptr ||
                    entry->request_digest != cc.message_digest() ||
                    entry->is_prepared == false;

                if (has_conflict) {
                    logger_.log(logLevel::WARN, "[Func: SendCommit] BRANCH: Conflict detected in log entry (seq=" + to_string(cc.sequence_number()) + ")");
                    logger_.log(logLevel::INFO, "[Func: SendCommit] EXIT - Returning CANCELLED due to conflicting data.");
                    return grpc::Status::CANCELLED;
                }
                logger_.log(logLevel::DEBUG, "[Func: SendCommit] STATE - Commit entry validated successfully (no conflicts).");
            }

            bool trigger_async = false;
            {
                lock_guard<mutex> lg(entry->commit_data.commit_mtx);
                logger_.log(logLevel::DEBUG, "[Func: SendCommit] ACTION - Locked commit_data to record commit.");
                
                if (find(entry->commit_data.nodes_received.begin(), entry->commit_data.nodes_received.end(), commit->node()) != entry->commit_data.nodes_received.end()) {
                    logger_.log(logLevel::WARN, "[Func: SendCommit] BRANCH: Duplicate Commit detected from node " + commit->node());
                    logger_.log(logLevel::INFO, "[Func: SendCommit] EXIT - Returning CANCELLED due to duplicate commit.");
                    return grpc::Status::CANCELLED;
                }

                entry->commit_data.valid_commits_received.push_back(make_shared<Commit>(*commit));
                entry->commit_data.valid_commit_count++;
                entry->commit_data.agg_signatures.push_back(commit->signature());
                entry->commit_data.nodes_received.push_back(commit->node());
                logger_.log(logLevel::DEBUG, "[Func: SendCommit] STATE - commit_data updated (count=" + to_string(entry->commit_data.valid_commit_count) + 
                    ", nodes=" + to_string(entry->commit_data.nodes_received.size()) + ").");

                if (entry->commit_data.valid_commit_count >= nodeOrig.quorumSize_.load()) {
                    logger_.log(logLevel::DEBUG, "[Func: SendCommit] BRANCH: Quorum condition met (>= " + to_string(nodeOrig.quorumSize_.load()) + ")");
                    if (!entry->commit_data.has_quorum) {
                        entry->commit_data.has_quorum = true;
                        entry->commit_data.quorum_nodes = entry->commit_data.nodes_received;
                        entry->commit_data.quorum_signatures = entry->commit_data.agg_signatures;
                        trigger_async = true;
                        logger_.log(logLevel::INFO, "[Func: SendCommit] INFO - Commit quorum established for seq=" + to_string(cc.sequence_number()));
                    }
                }
            }

            if (trigger_async) {
                logger_.log(logLevel::INFO, "[Func: SendCommit] ACTION - Quorum met, enqueuing handleCollectedCommitAsync.");
                nodeOrig.threadPool_.enqueue([this, entry]() {
                    this->nodeOrig.handleCollectedCommitAsync(entry);
                });
            }

            logger_.log(logLevel::INFO, "[Func: SendCommit] EXIT - Commit processed successfully, returning OK.");
            return grpc::Status::OK;
        }

        grpc::Status SendCollectedCommit(grpc::ServerContext* context,
                                    const CollectedCommit* collectedCommit,
                                    Empty* response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: SendCollectedCommit] - " + collectedCommit->DebugString());
            logger_.log(logLevel::INFO, "[Func: SendCollectedCommit] ENTER - Received CollectedCommit from node " + collectedCommit->node());
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendCollectedCommit] - "+collectedCommit->DebugString()));
            }

            if (!nodeOrig.nodeStatus_.load() || !nodeOrig.isViewStable_.load() || nodeOrig.isLeader_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendCollectedCommit] BRANCH: Node inactive, view unstable, or is leader -> rejecting CollectedCommit.");
                logger_.log(logLevel::INFO, "[Func: SendCollectedCommit] EXIT - Returning CANCELLED due to invalid receiver state.");
                return grpc::Status::CANCELLED;
            }

            {
                lock_guard<mutex> lk(nodeOrig.leaderMx_);
                logger_.log(logLevel::DEBUG, "[Func: SendCollectedCommit] ACTION - Acquired leaderMx_ to validate sender.");
                if (nodeOrig.leaderId_ != collectedCommit->node()) {
                    logger_.log(logLevel::ERROR, "[Func: SendCollectedCommit] ERROR - CollectedCommit received from invalid node: " + collectedCommit->node());
                    logger_.log(logLevel::INFO, "[Func: SendCollectedCommit] EXIT - Returning CANCELLED due to invalid leader ID.");
                    return grpc::Status::CANCELLED;
                }
            }

            logger_.log(logLevel::DEBUG, "[Func: SendCollectedCommit] STATE - Verifying aggregated signature for node " + collectedCommit->node());
            if (!bls_utils::verify_signature(nodeOrig.aggPublicKey_, collectedCommit->signature(), nodeOrig.serializeDeterministic(collectedCommit->commit_content()))) {
                logger_.log(logLevel::ERROR, "[Func: SendCollectedCommit] ERROR - Aggregated signature verification failed for node " + collectedCommit->node());
                logger_.log(logLevel::INFO, "[Func: SendCollectedCommit] EXIT - Returning CANCELLED due to invalid aggregated signature.");
                return grpc::Status::CANCELLED;
            }
            logger_.log(logLevel::DEBUG, "[Func: SendCollectedCommit] INFO - Aggregated signature verified successfully.");

            const auto& cc = collectedCommit->commit_content();
            logger_.log(logLevel::DEBUG, "[Func: SendCollectedCommit] STATE - Commit content extracted (seq=" + to_string(cc.sequence_number()) + ", view=" + to_string(cc.view_number()) + ").");

            if (nodeOrig.currentView_.load() != cc.view_number()) {
                logger_.log(logLevel::WARN, "[Func: SendCollectedCommit] BRANCH: View number mismatch (expected " + to_string(nodeOrig.currentView_.load()) + ", got " + to_string(cc.view_number()) + ")");
                logger_.log(logLevel::INFO, "[Func: SendCollectedCommit] EXIT - Returning CANCELLED due to view mismatch.");
                return grpc::Status::CANCELLED;
            }
            if (cc.sequence_number() <= nodeOrig.lowWaterMark_.load() || cc.sequence_number() > nodeOrig.highWaterMark_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendCollectedCommit] BRANCH: Sequence number out of bounds (" + to_string(cc.sequence_number()) + ")");
                logger_.log(logLevel::INFO, "[Func: SendCollectedCommit] EXIT - Returning CANCELLED due to invalid sequence number.");
                return grpc::Status::CANCELLED;
            }

            shared_ptr<MainNode::PBFTLogEntry> entry;
            {
                lock_guard<mutex> lk(nodeOrig.sequenceLogMapMx_);
                logger_.log(logLevel::DEBUG, "[Func: SendCollectedCommit] ACTION - Searching sequenceLogMap_ for seq=" + to_string(cc.sequence_number()));
                auto it = nodeOrig.sequenceLogMap_.find(cc.sequence_number());
                if (it != nodeOrig.sequenceLogMap_.end()) {
                    entry = it->second;
                    logger_.log(logLevel::DEBUG, "[Func: SendCollectedCommit] STATE - Found existing PBFTLogEntry for seq=" + to_string(cc.sequence_number()));
                }
                else {
                    logger_.log(logLevel::WARN, "[Func: SendCollectedCommit] BRANCH: No PBFTLogEntry found for seq=" + to_string(cc.sequence_number()));
                    logger_.log(logLevel::INFO, "[Func: SendCollectedCommit] EXIT - Returning CANCELLED due to missing log entry.");
                    return grpc::Status::CANCELLED;
                }
            }

            {
                lock_guard<mutex> lg(entry->mtx);
                logger_.log(logLevel::DEBUG, "[Func: SendCollectedCommit] ACTION - Locked PBFTLogEntry for conflict check.");
                const bool has_conflict =
                    entry->sequence_number != cc.sequence_number() ||
                    cc.view_number() != entry->view_number ||
                    entry->pre_prepare_data.pre_prepare == nullptr ||
                    entry->request_digest != cc.message_digest() ||
                    entry->is_prepared == false;

                if (has_conflict) {
                    logger_.log(logLevel::WARN, "[Func: SendCollectedCommit] BRANCH: Conflict detected for seq=" + to_string(cc.sequence_number()));
                    logger_.log(logLevel::INFO, "[Func: SendCollectedCommit] EXIT - Returning CANCELLED due to conflicting log entry.");
                    return grpc::Status::CANCELLED;
                }

                logger_.log(logLevel::DEBUG, "[Func: SendCollectedCommit] STATE - No conflict found. Marking entry as committed and ready for execution.");
                entry->is_committed = true;
                entry->is_ready_for_execution = true;
            }

            {
                lock_guard<mutex> lg(entry->commit_data.commit_mtx);
                logger_.log(logLevel::DEBUG, "[Func: SendCollectedCommit] ACTION - Storing CollectedCommit in commit_data.");
                entry->commit_data.collected_commit = make_shared<CollectedCommit>(*collectedCommit);
            }

            logger_.log(logLevel::INFO, "[Func: SendCollectedCommit] ACTION - Enqueuing executeSequence for seq=" + to_string(cc.sequence_number()));
            nodeOrig.threadPool_.enqueue([this, sequence_number = cc.sequence_number()]() {
                this->nodeOrig.executeSequence(sequence_number);
            });

            {
                int old_val = nodeOrig.sequenceNumber_.load();
                while (true) {
                    int desired = max(old_val, cc.sequence_number());
                    if (nodeOrig.sequenceNumber_.compare_exchange_weak(old_val, desired)) {
                        logger_.log(logLevel::DEBUG, "[Func: SendCollectedCommit] STATE - sequenceNumber_ updated to " + to_string(desired));
                        break;
                    }
                }
            }

            logger_.log(logLevel::INFO, "[Func: SendCollectedCommit] EXIT - CollectedCommit processed successfully, returning OK.");
            return grpc::Status::OK;
        }

        grpc::Status SendCheckpoint(grpc::ServerContext* context,
                                    const Checkpoint* checkpoint,
                                    Empty* response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: SendCheckpoint] - " + checkpoint->DebugString());
            logger_.log(logLevel::INFO, "[Func: SendCheckpoint] ENTER - Received Checkpoint from node " + checkpoint->node());

            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendCheckpoint] - "+checkpoint->DebugString()));
            }

            if (!nodeOrig.nodeStatus_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendCheckpoint] BRANCH: nodeStatus_ -> false (node inactive)");
                logger_.log(logLevel::INFO, "[Func: SendCheckpoint] EXIT - Returning CANCELLED due to inactive node.");
                return grpc::Status::CANCELLED;
            }

            auto nit = nodeOrig.nodes_.find(checkpoint->node());
            if (nit == nodeOrig.nodes_.end() || !bls_utils::verify_signature(nit->second.publicKey, checkpoint->signature(), nodeOrig.serializeDeterministic(checkpoint->checkpoint_content()))) {
                logger_.log(logLevel::ERROR, "[Func: SendCheckpoint] ERROR - Signature verification failed for sender node " + checkpoint->node());
                logger_.log(logLevel::INFO, "[Func: SendCheckpoint] EXIT - Returning CANCELLED due to invalid checkpoint signature.");
                return grpc::Status::CANCELLED;
            }
            logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] INFO - Signature verified successfully for node " + checkpoint->node());

            shared_ptr<MainNode::CheckpointData> checkpointEntry;
            int seqNum = checkpoint->checkpoint_content().sequence_number();
            const auto& cc = checkpoint->checkpoint_content();
            bool new_entry = false;

            logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] STATE - Processing checkpoint seq=" + to_string(seqNum) + ", digest=" + cc.data_digest());

            {
                lock_guard<mutex> lg(nodeOrig.checkpointLogMx_);
                logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] ACTION - Searching checkpointLog_ for seq=" + to_string(seqNum));
                auto cit = nodeOrig.checkpointLog_.find(seqNum);
                if (cit != nodeOrig.checkpointLog_.end()) {
                    checkpointEntry = cit->second;
                    logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] STATE - Existing checkpoint entry found for seq=" + to_string(seqNum));
                }
                else {
                    checkpointEntry = make_shared<MainNode::CheckpointData>();
                    nodeOrig.checkpointLog_[seqNum] = checkpointEntry;
                    new_entry = true;
                    logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] STATE - Created new CheckpointData entry for seq=" + to_string(seqNum));
                }
            }

            if (new_entry) {
                logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] BRANCH: new_entry == true -> initializing new checkpoint entry.");
                lock_guard<mutex> lk(checkpointEntry->checkpoint_mtx);
                checkpointEntry->sequence_number = seqNum;
                checkpointEntry->data_digest = cc.data_digest();
                checkpointEntry->valid_checkpoints_received.push_back(make_shared<Checkpoint>(*checkpoint));
                checkpointEntry->valid_checkpoint_count++;
                checkpointEntry->nodes_received.push_back(checkpoint->node());
                logger_.log(logLevel::INFO, "[Func: SendCheckpoint] EXIT - New checkpoint entry created successfully, returning OK.");
                return grpc::Status::OK;
            }

            bool isStateSyncRequired = false;
            {
                lock_guard<mutex> lk(checkpointEntry->checkpoint_mtx);
                logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] ACTION - Locked checkpoint entry for updating state.");

                if (find(checkpointEntry->nodes_received.begin(), checkpointEntry->nodes_received.end(), checkpoint->node())!=checkpointEntry->nodes_received.end()) {
                    logger_.log(logLevel::WARN, "[Func: SendCheckpoint] BRANCH: Duplicate checkpoint received from node " + checkpoint->node());
                    logger_.log(logLevel::INFO, "[Func: SendCheckpoint] EXIT - Returning CANCELLED due to duplicate checkpoint.");
                    return grpc::Status::CANCELLED;
                }

                if (cc.data_digest() != checkpointEntry->data_digest) {
                    logger_.log(logLevel::ERROR, "[Func: SendCheckpoint] ERROR - Data digest mismatch for checkpoint seq=" + to_string(seqNum));
                    logger_.log(logLevel::INFO, "[Func: SendCheckpoint] EXIT - Returning CANCELLED due to digest mismatch.");
                    return grpc::Status::CANCELLED;
                }

                checkpointEntry->valid_checkpoints_received.push_back(make_shared<Checkpoint>(*checkpoint));
                checkpointEntry->valid_checkpoint_count++;
                checkpointEntry->nodes_received.push_back(checkpoint->node());

                logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] STATE - Updated checkpoint data: count=" + to_string(checkpointEntry->valid_checkpoint_count) + 
                    ", nodes=" + to_string(checkpointEntry->nodes_received.size()));

                if (checkpointEntry->is_stable && seqNum <= nodeOrig.lastStableCheckpointSeqNum_.load()) {
                    logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] BRANCH: Already stable and seq <= lastStableCheckpointSeqNum_ -> returning OK.");
                    return grpc::Status::OK;
                }
                if (checkpointEntry->valid_checkpoint_count < nodeOrig.quorumSize_.load()) {
                    logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] BRANCH: Quorum not yet reached (" + to_string(checkpointEntry->valid_checkpoint_count) + "/" + to_string(nodeOrig.quorumSize_.load()) + ")");
                    return grpc::Status::OK;
                }

                checkpointEntry->quorum_checkpoints = checkpointEntry->valid_checkpoints_received;
                checkpointEntry->quorum_nodes = checkpointEntry->nodes_received;
                logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] STATE - Quorum reached, checkpoint marked for further verification.");

                if (checkpointEntry->local_checkpoint == nullptr && checkpointEntry->data_state.is_null()) {
                    isStateSyncRequired = true;
                    logger_.log(logLevel::INFO, "[Func: SendCheckpoint] BRANCH: State sync required (missing local checkpoint/data state).");
                }
                else {
                    checkpointEntry->is_stable = true;
                    nodeOrig.lastStableCheckpointSeqNum_.store(seqNum);
                    nodeOrig.lowWaterMark_.store(seqNum);
                    nodeOrig.highWaterMark_.store(seqNum + nodeOrig.validSequenceRange_.load());
                    logger_.log(logLevel::INFO, "[Func: SendCheckpoint] INFO - Checkpoint stabilized at seq=" + to_string(seqNum));
                }
            }

            if (isStateSyncRequired)
            {
                logger_.log(logLevel::INFO, "[Func: SendCheckpoint] ACTION - Initiating state sync from node " + checkpoint->node());
                auto [valid, data_state] = nodeOrig.requestDataStateAndValidate(checkpoint->node(), cc.data_digest(), seqNum);
                if (!valid) {
                    logger_.log(logLevel::ERROR, "[Func: SendCheckpoint] ERROR - State sync validation failed for node " + checkpoint->node());
                    logger_.log(logLevel::INFO, "[Func: SendCheckpoint] EXIT - Returning CANCELLED due to invalid state data.");
                    return grpc::Status::CANCELLED;
                }
                {
                    lock_guard<mutex> lk(checkpointEntry->checkpoint_mtx);
                    if (checkpointEntry->is_stable && seqNum <= nodeOrig.lastStableCheckpointSeqNum_.load()) {
                        logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] BRANCH: Checkpoint already stable, returning OK.");
                        return grpc::Status::OK;
                    }
                    checkpointEntry->data_state = data_state;
                    checkpointEntry->is_stable = true;
                    logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] STATE - State data synced and checkpoint marked stable.");
                }

                {
                    lock_guard<mutex> lk1(nodeOrig.executionInProgressMx_);
                    nodeOrig.lastExecutedSeqNum_.store(seqNum);
                    logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] STATE - Updated lastExecutedSeqNum_ to " + to_string(seqNum));
                }
                {
                    lock_guard<mutex> lg(nodeOrig.dataStateFileMx_);
                    nodeOrig.writeJsonFile(nodeOrig.dataStateFile_, data_state);
                    logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] ACTION - Persisted updated data state to file.");
                }

                nodeOrig.lastStableCheckpointSeqNum_.store(seqNum);
                nodeOrig.lowWaterMark_.store(seqNum);
                nodeOrig.highWaterMark_.store(seqNum + nodeOrig.validSequenceRange_.load());
                logger_.log(logLevel::INFO, "[Func: SendCheckpoint] INFO - Updated stable checkpoint seqNum and watermarks.");
            }

            {
                int old_val = nodeOrig.sequenceNumber_.load();
                while (true) {
                    int desired = max(old_val, cc.sequence_number());
                    if (nodeOrig.sequenceNumber_.compare_exchange_weak(old_val, desired)) {
                        logger_.log(logLevel::DEBUG, "[Func: SendCheckpoint] STATE - sequenceNumber_ updated to " + to_string(desired));
                        break;
                    }
                }
            }

            logger_.log(logLevel::INFO, "[Func: SendCheckpoint] EXIT - Checkpoint processed successfully, returning OK.");
            return grpc::Status::OK;
        }

        grpc::Status SendDataStateRequest(grpc::ServerContext* context,
                                    const DataStateRequest* request,
                                    DataStateResponse* response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: SendDataStateRequest] - " + request->DebugString());
            logger_.log(logLevel::INFO, "[Func: SendDataStateRequest] ENTER - Received DataStateRequest from node " + request->node());
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendDataStateRequest] - "+request->DebugString()));
            }
            if (!nodeOrig.nodeStatus_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendDataStateRequest] BRANCH: nodeStatus_ -> false (node inactive)");
                logger_.log(logLevel::INFO, "[Func: SendDataStateRequest] EXIT - Returning CANCELLED due to inactive node.");
                return grpc::Status::CANCELLED;
            }

            auto nit = nodeOrig.nodes_.find(request->node());
            if (nit == nodeOrig.nodes_.end() || !bls_utils::verify_signature(nit->second.publicKey, request->signature(), nodeOrig.serializeDeterministic(request->data_state_request_content()))) {
                logger_.log(logLevel::ERROR, "[Func: SendDataStateRequest] ERROR - Signature verification failed for sender node " + request->node());
                logger_.log(logLevel::INFO, "[Func: SendDataStateRequest] EXIT - Returning CANCELLED due to invalid signature.");
                return grpc::Status::CANCELLED;
            }
            logger_.log(logLevel::DEBUG, "[Func: SendDataStateRequest] INFO - Signature verified successfully for node " + request->node());

            shared_ptr<MainNode::CheckpointData> checkpointEntry;
            int seqNum = request->data_state_request_content().sequence_number();
            logger_.log(logLevel::DEBUG, "[Func: SendDataStateRequest] STATE - Requested sequence number: " + to_string(seqNum));

            {
                lock_guard<mutex> lg(nodeOrig.checkpointLogMx_);
                logger_.log(logLevel::DEBUG, "[Func: SendDataStateRequest] ACTION - Searching checkpointLog_ for seq=" + to_string(seqNum));
                auto cit = nodeOrig.checkpointLog_.find(seqNum);
                if (cit != nodeOrig.checkpointLog_.end()) {
                    checkpointEntry = cit->second;
                    logger_.log(logLevel::DEBUG, "[Func: SendDataStateRequest] STATE - Found checkpoint entry for seq=" + to_string(seqNum));
                }
                else {
                    logger_.log(logLevel::WARN, "[Func: SendDataStateRequest] BRANCH: No checkpoint entry found for seq=" + to_string(seqNum));
                    logger_.log(logLevel::INFO, "[Func: SendDataStateRequest] EXIT - Returning CANCELLED due to missing checkpoint data.");
                    return grpc::Status::CANCELLED;
                }
            }

            lock_guard<mutex> lk(checkpointEntry->checkpoint_mtx);
            logger_.log(logLevel::DEBUG, "[Func: SendDataStateRequest] ACTION - Locked checkpoint entry for seq=" + to_string(seqNum) + " to build response.");

            response->set_node(nodeOrig.id_);
            auto* response_content = response->mutable_data_state_response_content();
            response_content->set_sequence_number(seqNum);
            response_content->set_data_state(checkpointEntry->data_state.dump());
            response->set_signature(bls_utils::sign_data(nodeOrig.privateKey_, nodeOrig.serializeDeterministic(*response_content)));

            logger_.log(logLevel::INFO, "[Func: SendDataStateRequest] ACTION - DataStateResponse prepared successfully for seq=" + to_string(seqNum));
            logger_.log(logLevel::DEBUG, "[Func: SendDataStateRequest] STATE - Response DebugString: " + response->DebugString());
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: SendDataStateRequest] - "+response->DebugString()));
            }
            logger_.log(logLevel::INFO, "[Func: SendDataStateRequest] EXIT - Returning OK.");
            return grpc::Status::OK;
        }

        grpc::Status SendViewChange(grpc::ServerContext* context,
                                    const ViewChange* viewChange,
                                    Empty* response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: SendViewChange] - " + viewChange->DebugString());
            logger_.log(logLevel::INFO, "[Func: SendViewChange] ENTER - Received ViewChange from node " + viewChange->node());

            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendViewChange] - "+viewChange->DebugString()));
            }
            if (!nodeOrig.nodeStatus_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendViewChange] BRANCH: nodeStatus_ -> false (node inactive)");
                logger_.log(logLevel::INFO, "[Func: SendViewChange] EXIT - Returning CANCELLED due to inactive node.");
                return grpc::Status::CANCELLED;
            }

            auto nit = nodeOrig.nodes_.find(viewChange->node());
            if (nit == nodeOrig.nodes_.end() || !bls_utils::verify_signature(nit->second.publicKey, viewChange->signature(), nodeOrig.serializeDeterministic(viewChange->view_change_content()))) {
                logger_.log(logLevel::ERROR, "[Func: SendViewChange] ERROR - Signature verification failed for sender node " + viewChange->node());
                logger_.log(logLevel::ERROR, "[Func: SendViewChange] ERROR - Signature verified using key " + nit->second.publicKey);
                logger_.log(logLevel::INFO, "[Func: SendViewChange] EXIT - Returning CANCELLED due to invalid signature.");
                return grpc::Status::CANCELLED;
            }
            logger_.log(logLevel::DEBUG, "[Func: SendViewChange] INFO - Signature verified successfully for node " + viewChange->node());

            int viewNo = viewChange->view_change_content().view_number();
            const auto& vc = viewChange->view_change_content();
            logger_.log(logLevel::DEBUG, "[Func: SendViewChange] STATE - Received view change for viewNo=" + to_string(viewNo));

            if (viewNo < nodeOrig.currentView_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendViewChange] BRANCH: Current view (" + to_string(nodeOrig.currentView_.load()) + ") ahead of sender node " + viewChange->node());
                logger_.log(logLevel::INFO, "[Func: SendViewChange] EXIT - Returning CANCELLED (outdated view).");
                return grpc::Status::CANCELLED;
            }

            shared_ptr<MainNode::ViewChangeData> viewChangeEntry;
            bool new_entry = false;
            {
                lock_guard<mutex> lg(nodeOrig.viewChangeLogMx_);
                logger_.log(logLevel::DEBUG, "[Func: SendViewChange] ACTION - Searching for existing viewChangeLog entry for view=" + to_string(viewNo));
                auto cit = nodeOrig.viewChangeLog_.find(viewNo);
                if (cit != nodeOrig.viewChangeLog_.end()) {
                    viewChangeEntry = cit->second;
                    logger_.log(logLevel::DEBUG, "[Func: SendViewChange] STATE - Found existing entry for view=" + to_string(viewNo));
                }
                else {
                    viewChangeEntry = make_shared<MainNode::ViewChangeData>();
                    nodeOrig.viewChangeLog_[viewNo] = viewChangeEntry;
                    new_entry = true;
                    logger_.log(logLevel::DEBUG, "[Func: SendViewChange] STATE - Created new ViewChangeData entry for view=" + to_string(viewNo));
                }
            }

            bool counted = false;
            bool has_consensus = false;
            {
                lock_guard<mutex> lk(viewChangeEntry->view_change_mtx);
                logger_.log(logLevel::DEBUG, "[Func: SendViewChange] ACTION - Acquired view_change_mtx for updating ViewChangeData.");
                if (new_entry) viewChangeEntry->view_number = viewNo;
                if (find(viewChangeEntry->nodes_received.begin(), viewChangeEntry->nodes_received.end(), viewChange->node()) != viewChangeEntry->nodes_received.end()) {
                    logger_.log(logLevel::WARN, "[Func: SendViewChange] BRANCH: Duplicate ViewChange detected from node " + viewChange->node());
                    logger_.log(logLevel::INFO, "[Func: SendViewChange] EXIT - Returning CANCELLED due to duplicate message.");
                    return grpc::Status::CANCELLED;
                }

                counted = true;
                viewChangeEntry->valid_view_changes_received.push_back(make_shared<ViewChange>(*viewChange));
                viewChangeEntry->nodes_received.push_back(viewChange->node());
                viewChangeEntry->valid_view_change_count++;

                logger_.log(logLevel::DEBUG, "[Func: SendViewChange] STATE - Updated ViewChangeData (count=" + to_string(viewChangeEntry->valid_view_change_count) + 
                    ", nodes=" + to_string(viewChangeEntry->nodes_received.size()) + ")");

                if (viewChangeEntry->valid_view_change_count >= nodeOrig.quorumSize_.load() && !viewChangeEntry->has_consensus)
                {
                    viewChangeEntry->has_consensus = true;
                    viewChangeEntry->quorum_nodes = viewChangeEntry->nodes_received;
                    viewChangeEntry->quorum_view_changes = viewChangeEntry->valid_view_changes_received;
                    logger_.log(logLevel::INFO, "[Func: SendViewChange] INFO - Quorum reached for view change (view=" + to_string(viewNo) + ")");

                    if (nodeOrig.isLeader_.load()) {
                        logger_.log(logLevel::INFO, "[Func: SendViewChange] ACTION - Node is leader, creating new view " + to_string(viewNo));
                        if (nodeOrig.byzantineStatus_.load() && nodeOrig.crashAttackActivated_) {
                            logger_.log(logLevel::INFO, "[Func: SendViewChange] ACTION - Avoiding sending new view although having received enough quorum for view changes (view=" + to_string(viewNo) + ")");
                        }
                        else {
                            nodeOrig.threadPool_.enqueue([this, viewNo]() {
                                this->nodeOrig.createNewView(viewNo);
                            });
                        }
                        logger_.log(logLevel::INFO, "[Func: SendViewChange] EXIT - Returning OK after scheduling new view creation.");
                        return grpc::Status::OK;
                    }
                    
                    if (!nodeOrig.timer_.is_running()) {
                        logger_.log(logLevel::DEBUG, "[Func: SendViewChange] ACTION - Node not leader, scheduling timer for next view change.");
                        int multiplier = nodeOrig.consecutiveViewChangeCount_.fetch_add(1) + 1;
                        logger_.log(logLevel::DEBUG, "[Func: SendViewChange] STATE - Count of consecutive view change : "+ to_string(multiplier) + ".");
                        int old = nodeOrig.timeoutMs_.load();
                        nodeOrig.timeoutMs_.store(multiplier * old);
                        logger_.log(logLevel::DEBUG, "[Func: SendViewChange] ACTION - Updating timeoutMs_ value from  : "+to_string(old)+ " to " + to_string(multiplier * old) + ".");
                        nodeOrig.timer_.start(milliseconds(nodeOrig.timeoutMs_.load()), [this]() {
                            this->nodeOrig.isViewStable_.store(false);
                            this->nodeOrig.initiateViewChange();
                        });
                        logger_.log(logLevel::INFO, "[Func: SendViewChange] EXIT - Returning OK after starting view change timer.");
                        return grpc::Status::OK;
                    }
                }
                has_consensus = viewChangeEntry->has_consensus;
            }

            if (nodeOrig.isViewStable_.load() && counted && !has_consensus) {
                logger_.log(logLevel::DEBUG, "[Func: SendViewChange] STATE - View stable, counting received ViewChange messages.");
                nodeOrig.latestViewChangeCount_.fetch_add(1);
                if (nodeOrig.leastViewNumViewChangeReceived_.load() == 0) {
                    nodeOrig.leastViewNumViewChangeReceived_.store(viewNo);
                    logger_.log(logLevel::DEBUG, "[Func: SendViewChange] STATE - Initialized leastViewNumViewChangeReceived_ to " + to_string(viewNo));
                } else {
                    int old_val = nodeOrig.leastViewNumViewChangeReceived_.load();
                    while (true) {
                        int desired = min(old_val, viewNo);
                        if (nodeOrig.leastViewNumViewChangeReceived_.compare_exchange_weak(old_val, desired)) {
                            logger_.log(logLevel::DEBUG, "[Func: SendViewChange] STATE - Updated leastViewNumViewChangeReceived_ to " + to_string(desired));
                            break;
                        }
                    }
                }

                if (nodeOrig.latestViewChangeCount_.load() >= (nodeOrig.noFaults_.load() + 1) && nodeOrig.isViewStable_.load()) {
                    logger_.log(logLevel::INFO, "[Func: SendViewChange] INFO - Sufficient ViewChange messages received (" + to_string(nodeOrig.latestViewChangeCount_.load()) + ") initiating view change.");
                    nodeOrig.latestViewChangeCount_.store(0);
                    nodeOrig.isViewStable_.store(false);
                    nodeOrig.initiateViewChange(nodeOrig.leastViewNumViewChangeReceived_.load());
                }
            }

            logger_.log(logLevel::INFO, "[Func: SendViewChange] EXIT - ViewChange processed successfully, returning OK.");
            return grpc::Status::OK;
        }

        grpc::Status SendNewView(grpc::ServerContext* context,
                                    const NewView* newView,
                                    Empty* response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: SendNewView] - " + newView->DebugString());
            logger_.log(logLevel::INFO, "[Func: SendNewView] ENTER - Received NewView from node " + newView->node());
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendNewView] - "+newView->DebugString()));
            }

            {
                lock_guard<mutex> l(nodeOrig.newViewMessageListMx_);
                nodeOrig.newViewMessageList_.push_back(newView->DebugString());
            }

            if (!nodeOrig.nodeStatus_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendNewView] BRANCH: nodeStatus_ -> false (node inactive)");
                logger_.log(logLevel::INFO, "[Func: SendNewView] EXIT - Returning CANCELLED due to inactive node.");
                return grpc::Status::CANCELLED;
            }

            auto nit = nodeOrig.nodes_.find(newView->node());
            if (nit == nodeOrig.nodes_.end() || !bls_utils::verify_signature(nit->second.publicKey, newView->signature(), nodeOrig.serializeDeterministic(newView->new_view_content()))) {
                logger_.log(logLevel::ERROR, "[Func: SendNewView] ERROR - Signature verification failed for sender node " + newView->node());
                logger_.log(logLevel::INFO, "[Func: SendNewView] EXIT - Returning CANCELLED due to invalid signature.");
                return grpc::Status::CANCELLED;
            }

            if (nodeOrig.currentView_.load()>newView->new_view_content().view_number()) {
                logger_.log(logLevel::WARN, "[Func: SendNewView] BRANCH: Current view ahead of sender node " + newView->node());
                logger_.log(logLevel::INFO, "[Func: SendNewView] EXIT - Returning CANCELLED due to outdated new view.");
                return grpc::Status::CANCELLED;
            }

            for (const auto& vce : newView->new_view_content().view_change_evidence()) {
                if (vce.node()==nodeOrig.id_) {
                    if (!bls_utils::verify_signature(nodeOrig.publicKey_, vce.signature(), nodeOrig.serializeDeterministic(vce.view_change_content()))) {
                        logger_.log(logLevel::ERROR, "[Func: SendNewView] ERROR - ViewChange evidence verification failed for node " + vce.node() + " (sender " + newView->node() + ")");
                        logger_.log(logLevel::INFO, "[Func: SendNewView] EXIT - Returning CANCELLED due to invalid view change evidence.");
                        return grpc::Status::CANCELLED;
                    }
                    continue;
                }
                auto nit = nodeOrig.nodes_.find(vce.node());
                if (nit == nodeOrig.nodes_.end() || !bls_utils::verify_signature(nit->second.publicKey, vce.signature(), nodeOrig.serializeDeterministic(vce.view_change_content()))) {
                    logger_.log(logLevel::ERROR, "[Func: SendNewView] ERROR - ViewChange evidence verification failed for node " + vce.node() + " (sender " + newView->node() + ")");
                    logger_.log(logLevel::INFO, "[Func: SendNewView] EXIT - Returning CANCELLED due to invalid view change evidence.");
                    return grpc::Status::CANCELLED;
                }
            }
            int viewNo = newView->new_view_content().view_number();
            if (!nodeOrig.computeAndVerifyPrePrepareSet(newView->new_view_content(), viewNo)) {
                logger_.log(logLevel::ERROR, "[Func: SendNewView] ERROR - Pre-Prepare set validation failed.");
                logger_.log(logLevel::INFO, "[Func: SendNewView] EXIT - Returning CANCELLED due to invalid pre-prepare set.");
                return grpc::Status::CANCELLED;
            }
            nodeOrig.currentView_.store(newView->new_view_content().view_number());
            if (nodeOrig.timer_.is_running()) nodeOrig.timer_.stop();
            nodeOrig.setLeader();
            nodeOrig.isViewStable_.store(true);
            nodeOrig.latestViewChangeCount_.store(0);
            nodeOrig.leastViewNumViewChangeReceived_.store(0);
            nodeOrig.consecutiveViewChangeCount_.store(0);
            logger_.log(logLevel::INFO, "[Func: SendNewView] STATE - View updated to " + to_string(nodeOrig.currentView_.load()) + " and stability restored.");

            const auto& nvc = newView->new_view_content();

            int minSeqReceived = 0, maxSeqReceived = 0;
            if (!nvc.pre_prepare_set().empty()) {
                auto [min_it, max_it] = minmax_element(
                    nvc.pre_prepare_set().begin(), nvc.pre_prepare_set().end(),
                    [](const auto& a, const auto& b){ return a.first < b.first; });
                minSeqReceived = min_it->first;
                maxSeqReceived = max_it->first;
                logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - PrePrepare range determined: [" + to_string(minSeqReceived) + ", " + to_string(maxSeqReceived) + "]");
            } else {
                logger_.log(logLevel::DEBUG, "[Func: SendNewView] BRANCH: Empty pre_prepare_set.");
                logger_.log(logLevel::INFO, "[Func: SendNewView] EXIT - Returning OK (no pre-prepare entries to process).");
                return grpc::Status::OK;
            }
            int seqNum = minSeqReceived;
            logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - Log Correctness loop starts with seq no "+to_string(seqNum));
            while (seqNum <= maxSeqReceived) {
                logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - Inside while loop with seq no "+to_string(seqNum));
                bool run_execute = false;
                shared_ptr<PBFTLogEntry> logEntry;
                PrePrepareViewChangeSet pre_prepare_set;
                const auto& preMap = nvc.pre_prepare_set();
                auto it = preMap.find(seqNum);
                if (it!=nvc.pre_prepare_set().end()) {
                    logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - Entry present in pre_prepare_set.");
                    pre_prepare_set = it->second;
                }
                else {
                    seqNum++;
                    logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - Entry not present in pre_prepare_set.");
                    continue;
                }
                {
                    lock_guard<mutex> lk(nodeOrig.sequenceLogMapMx_);
                    auto it = nodeOrig.sequenceLogMap_.find(seqNum);
                    if (it!=nodeOrig.sequenceLogMap_.end()) {
                        logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - Log Entry already exists.");
                        logEntry = nodeOrig.sequenceLogMap_[seqNum];
                    }
                    else {
                        logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - Log Entry doesn't exist so creating new one.");
                        logEntry = make_shared<PBFTLogEntry>();
                        logEntry->sequence_number = seqNum;
                        nodeOrig.sequenceLogMap_[seqNum] = logEntry;
                    }
                }
                bool updateCollectedPrepare = false;
                bool fetch_request = false;
                {
                    lock_guard<mutex> lk(logEntry->mtx);
                    logEntry->view_number = viewNo;
                    if (logEntry->request_digest!=pre_prepare_set.pre_prepare().pre_prepare_content().message_digest()) {
                        logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - Digest doesnt match between existing entry and the new one.");
                        logEntry->request_digest = pre_prepare_set.pre_prepare().pre_prepare_content().message_digest();
                        if (logEntry->request_digest!="NULL") {
                            fetch_request = true;
                            logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - assigning fetch_request to "+to_string(fetch_request));
                        }
                        logEntry->client_request = nullptr;
                    }

                    if (pre_prepare_set.prepared_evidence().is_fast_path() && !logEntry->is_fast_path && !logEntry->is_executed) {
                        logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - Entry has followed fast path");
                        logEntry->is_prepared = true;
                        logEntry->is_ready_for_execution = true;
                        logEntry->is_fast_path = true;
                        updateCollectedPrepare = true;
                        run_execute = true;
                    }
                }

                if (fetch_request) {
                    nodeOrig.threadPool_.enqueue([this, sequence_number = seqNum]() {
                        this->nodeOrig.requestClientMessage(sequence_number);
                    });
                }
                {
                    lock_guard<mutex> lk(logEntry->pre_prepare_data.pre_prepare_mtx);
                    logEntry->pre_prepare_data.pre_prepare = make_shared<PrePrepare>(pre_prepare_set.pre_prepare());
                }
                Prepare prepare;
                prepare.mutable_prepare_content()->CopyFrom(pre_prepare_set.pre_prepare().pre_prepare_content());
                prepare.mutable_prepare_content()->set_message_type(PREPARE);
                prepare.set_node(nodeOrig.id_);
                prepare.set_signature(bls_utils::sign_data(nodeOrig.privateKey_, nodeOrig.serializeDeterministic(prepare.prepare_content())));
                prepare.set_fast_path_signature(bls_utils::sign_data(nodeOrig.fastPrivateKey_, nodeOrig.serializeDeterministic(prepare.prepare_content())));
                {
                    lock_guard<mutex> lg(logEntry->prepare_data.prepare_mtx);
                    logEntry->prepare_data.local_prepare = make_shared<Prepare>(prepare);
                    logEntry->prepare_data.valid_prepares_received.clear();
                    logEntry->prepare_data.valid_prepares_received.push_back(logEntry->prepare_data.local_prepare);
                    logEntry->prepare_data.valid_prepare_count = 0;
                    logEntry->prepare_data.valid_prepare_count++;
                    logEntry->prepare_data.nodes_received.clear();
                    logEntry->prepare_data.nodes_received.push_back(nodeOrig.id_);
                    logEntry->prepare_data.agg_signatures.clear();
                    logEntry->prepare_data.has_quorum = false;
                    logEntry->prepare_data.has_total_consensus = false;
                    if (updateCollectedPrepare) {
                        logEntry->prepare_data.collected_prepare = make_shared<CollectedPrepare>(pre_prepare_set.prepared_evidence().collected_prepare());
                    }
                }

                {
                lock_guard<mutex> lg(logEntry->commit_data.commit_mtx);
                logEntry->commit_data.valid_commits_received.clear();
                logEntry->commit_data.valid_commit_count = 0;
                logEntry->commit_data.nodes_received.clear();
                logEntry->commit_data.agg_signatures.clear();
                logEntry->commit_data.has_quorum = false;
                }

                if (nodeOrig.byzantineStatus_.load() && nodeOrig.darkAttackActivated_ && nodeOrig.nodes_[newView->node()].byzantineVictim) {
                    logger_.log(logLevel::INFO, "[Func: SendNewView] ACTION - Skipping Prepare sending to "+newView->node());
                }
                else if (nodeOrig.byzantineStatus_.load() && nodeOrig.crashAttackActivated_) {
                    logger_.log(logLevel::INFO, "[Func: SendNewView] ACTION - Avoiding sending Prepare to "+newView->node());
                }
                else {
                    {
                        lock_guard<mutex> l(nodeOrig.logMx_);
                        nodeOrig.log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: SendNewView] - "+prepare.DebugString()));
                    }
                    auto* call = new MainNode::AsyncNodeCall;
                    call->response_reader = nodeOrig.nodes_[newView->node()].nodeStub->AsyncSendPrepare(&call->context, prepare, &nodeOrig.cq_);
                    call->response_reader->Finish(&call->reply, &call->status, (void*)call);
                }

                if (run_execute) {
                    bool still_needed = false;
                    {
                        lock_guard<mutex> lk(logEntry->mtx);
                        if (logEntry->request_digest!="NULL") still_needed=true;
                    }
                    
                    if (still_needed)
                    {
                        string client;
                        long long timestamp;
                        {
                            lock_guard<mutex> lk(logEntry->mtx);
                            client = logEntry->client_request->client();
                            timestamp = logEntry->client_request->request_content().timestamp();
                        }
                        lock_guard<mutex> lk(nodeOrig.clientsMx_);
                        auto it = nodeOrig.clients_.find(client);
                        if (it != nodeOrig.clients_.end()) {
                            auto* clientMetadata = &it->second;
                            logger_.log(logLevel::DEBUG, "[Func: SendNewView] ACTION - Updating client's lastConsideredTransactionTimestamp.");
                            {
                                if (timestamp > clientMetadata->lastConsideredTransactionTimestamp) {
                                    clientMetadata->lastConsideredTransactionTimestamp = timestamp;
                                    logger_.log(logLevel::DEBUG, "[Func: SendNewView] STATE - Updated timestamp for client " + client);
                                }
                            }
                        }
                    }

                    nodeOrig.threadPool_.enqueue([this, sequence_number = seqNum]() {
                        this->nodeOrig.executeSequence(sequence_number);
                    });
                }
                seqNum++;
            }

            if (!nodeOrig.timer_.is_running() && nodeOrig.lastExecutedSeqNum_.load()<maxSeqReceived) {
                logger_.log(logLevel::DEBUG, "[Func: SendNewView] ACTION - Starting stability timer after NewView processing.");
                nodeOrig.timer_.start(milliseconds(nodeOrig.timeoutMs_.load()), [this] {
                    this->nodeOrig.isViewStable_.store(false);
                    this->nodeOrig.initiateViewChange();
                });
            }

            logger_.log(logLevel::INFO, "[Func: SendNewView] EXIT - NewView processed successfully, returning OK.");
            return grpc::Status::OK;
        }

        grpc::Status SendClientMessageRequest(grpc::ServerContext* context,
                                    const ClientMessageRequest* request,
                                    ClientMessageResponse* response) override
        {
            logger_.log(logLevel::MSG_IN, "[RPC: SendClientMessageRequest] - " + request->DebugString());
            logger_.log(logLevel::INFO, "[Func: SendClientMessageRequest] ENTER - Received ClientMessageRequest from node " + request->node());
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: SendClientMessageRequest] - "+request->DebugString()));
            }
            if (!nodeOrig.nodeStatus_.load()) {
                logger_.log(logLevel::WARN, "[Func: SendClientMessageRequest] BRANCH: nodeStatus_ -> false (node inactive)");
                logger_.log(logLevel::INFO, "[Func: SendClientMessageRequest] EXIT - Returning CANCELLED due to inactive node.");
                return grpc::Status::CANCELLED;
            }

            auto nit = nodeOrig.nodes_.find(request->node());
            if (nit == nodeOrig.nodes_.end() || !bls_utils::verify_signature(nit->second.publicKey, request->signature(), nodeOrig.serializeDeterministic(request->client_message_request_content()))) {
                logger_.log(logLevel::ERROR, "[Func: SendClientMessageRequest] ERROR - Signature verification failed for sender node " + request->node());
                logger_.log(logLevel::INFO, "[Func: SendClientMessageRequest] EXIT - Returning CANCELLED due to invalid signature.");
                return grpc::Status::CANCELLED;
            }
            logger_.log(logLevel::DEBUG, "[Func: SendClientMessageRequest] INFO - Signature verified successfully for node " + request->node());

            shared_ptr<MainNode::PBFTLogEntry> logEntry;
            int seqNum = request->client_message_request_content().sequence_number();
            logger_.log(logLevel::DEBUG, "[Func: SendClientMessageRequest] STATE - Looking up PBFTLogEntry for seqNum=" + to_string(seqNum));

            {
                lock_guard<mutex> lg(nodeOrig.sequenceLogMapMx_);
                logger_.log(logLevel::DEBUG, "[Func: SendClientMessageRequest] ACTION - Acquired sequenceLogMapMx_ to search for seqNum.");
                auto cit = nodeOrig.sequenceLogMap_.find(seqNum);
                if (cit != nodeOrig.sequenceLogMap_.end()) {
                    logEntry = cit->second;
                    logger_.log(logLevel::DEBUG, "[Func: SendClientMessageRequest] STATE - Found PBFTLogEntry for seqNum=" + to_string(seqNum));
                }
                else {
                    logger_.log(logLevel::WARN, "[Func: SendClientMessageRequest] BRANCH: No PBFTLogEntry found for seqNum=" + to_string(seqNum));
                    logger_.log(logLevel::INFO, "[Func: SendClientMessageRequest] EXIT - Returning CANCELLED due to missing log entry.");
                    return grpc::Status::CANCELLED;
                }
            }

            lock_guard<mutex> lk(logEntry->mtx);
            logger_.log(logLevel::DEBUG, "[Func: SendClientMessageRequest] ACTION - Locked PBFTLogEntry to prepare response.");

            response->set_node(nodeOrig.id_);
            auto* response_content = response->mutable_client_message_response_content();
            response_content->set_sequence_number(seqNum);
            if (logEntry->client_request == nullptr) {
                logger_.log(logLevel::DEBUG, "[Func: SendClientMessageRequest] BRANCH - Client request is missing, hence returning CANCELLED.");
                return grpc::Status::CANCELLED;
            }
            response_content->mutable_client_request()->CopyFrom(*logEntry->client_request);
            response->set_signature(bls_utils::sign_data(nodeOrig.privateKey_, nodeOrig.serializeDeterministic(*response_content)));

            logger_.log(logLevel::INFO, "[Func: SendClientMessageRequest] ACTION - Response prepared successfully for seqNum=" + to_string(seqNum));
            logger_.log(logLevel::DEBUG, "[Func: SendClientMessageRequest] STATE - Response DebugString: " + response->DebugString());
            {
                lock_guard<mutex> l(nodeOrig.logMx_);
                nodeOrig.log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: SendClientMessageRequest] - "+response->DebugString()));
            }
            logger_.log(logLevel::INFO, "[Func: SendClientMessageRequest] EXIT - Returning OK.");
            return grpc::Status::OK;
        }

    private:
        Logger &logger_;
        MainNode &nodeOrig;
    };

    string id_;
    int port_, processId_;
    int lastLogPrinted_ = 0;
    vector<string> log_;
    string resourcesFile_, dataStateFile_, stateFile_;
    atomic<int> currentView_{1};
    atomic<int> sequenceNumber_{0};
    atomic<int> noFaults_;
    atomic<int> quorumSize_;
    atomic<int> totalNodes_;
    ResettableTimer timer_;
    atomic<int> timeoutMs_;
    atomic<int> fastPathWaitTimeMs_;
    atomic<int> checkpointInterval_;
    atomic<int> lowWaterMark_{0};
    atomic<int> highWaterMark_;
    atomic<int> validSequenceRange_;
    atomic<int> lastStableCheckpointSeqNum_{0};
    atomic<int> latestViewChangeCount_{0};
    atomic<int> leastViewNumViewChangeReceived_{0};
    atomic<bool> isLeader_{false};
    atomic<int> consecutiveViewChangeCount_{0};
    string leaderId_;
    unordered_map<int, string> viewLeaderMap_{{0, "n7"}, {1, "n1"}, {2, "n2"}, {3, "n3"}, {4, "n4"}, {5, "n5"}, {6, "n6"}};
    atomic<bool> isViewStable_{true};
    atomic<int> lastExecutedSeqNum_{0};
    string privateKey_, publicKey_, fastPrivateKey_, invalidKey_, fastPublicKey_, aggPublicKey_, aggFastPublicKey_;
    unordered_map<string, Node> nodes_;
    unordered_map<string, Client> clients_;
    Logger logger_;
    OrchestratorServiceImpl orchestratorService_{logger_, *this};
    NodeServiceImpl nodeService_{logger_, *this};
    unique_ptr<grpc::Server> server_;
    thread nodeThread_;
    CompletionQueue cq_;
    thread cqThread_;
    atomic<bool> nodeStatus_{false};
    atomic<bool> byzantineStatus_{false};
    vector<string> byzantineVictims_;
    bool darkAttackActivated_ = false;
    bool timeAttackActivated_ = false;
    bool crashAttackActivated_ = false;
    bool equivocationAttackActivated_ = false;
    unordered_set<string> firstSet_, secondSet_, completeSet_;
    mutex clientsMx_, logMx_, newViewMessageListMx_, dataStateFileMx_, sequenceMx_, leaderMx_, sequenceLogMapMx_, checkpointLogMx_, clientTransactionsExecutionLogMx_, pendingRequestMx_, executionInProgressMx_, viewChangeLogMx_;
    struct ByzantineAttack
    {
        string type;
        vector<string> targetNodeIds;
    };
    vector<ByzantineAttack> byzantineAttacks_;
    struct AsyncNodeCall {
        Empty reply;
        grpc::ClientContext context;
        grpc::Status status;
        unique_ptr<grpc::ClientAsyncResponseReader<Empty>> response_reader;
    };
    struct PairHash1
    {
        size_t operator()(const pair<string, long long> &p) const noexcept
        {
            size_t h1 = hash<string>{}(p.first);
            size_t h2 = hash<long long>{}(p.second);

            return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
        }
    };
    unordered_map<pair<string, long long>, shared_ptr<Reply>, PairHash1> clientTransactionsExecutionLog_;
    deque<shared_ptr<Request>> pendingRequest_;
    ThreadPool threadPool_{6};
    struct PhasePrepareData {
        shared_ptr<Prepare> local_prepare;
        shared_ptr<CollectedPrepare> collected_prepare;
        vector<shared_ptr<Prepare>> valid_prepares_received;
        vector<string> nodes_received;
        vector<string> agg_signatures;
        vector<string> fast_path_agg_signatures;
        vector<string> quorum_signatures;
        vector<string> quorum_nodes;
        int valid_prepare_count = 0;
        bool has_quorum = false;
        bool has_total_consensus = false;
        mutex prepare_mtx;
    };
    struct PhaseCommitData {
        shared_ptr<Commit> local_commit;
        shared_ptr<CollectedCommit> collected_commit;
        vector<shared_ptr<Commit>> valid_commits_received;
        vector<string> nodes_received;
        vector<string> agg_signatures;
        vector<string> quorum_signatures;
        vector<string> quorum_nodes;
        int valid_commit_count = 0;
        bool has_quorum = false;
        mutex commit_mtx;
    };
    struct PhasePrePrepareData {
        shared_ptr<PrePrepare> pre_prepare;
        mutex pre_prepare_mtx;
    };
    struct PBFTLogEntry {
        int sequence_number = 0;
        int view_number = 0;
        shared_ptr<Request> client_request;
        string request_digest;
        PhasePrePrepareData pre_prepare_data;
        PhasePrepareData prepare_data;
        PhaseCommitData commit_data;
        bool is_prepared = false;
        bool is_committed = false;
        bool is_ready_for_execution = false;
        bool is_executed = false;
        bool is_fast_path = false;
        Reply reply;
        mutex mtx;
    };
    unordered_map<int, shared_ptr<PBFTLogEntry>> sequenceLogMap_;
    struct CheckpointData {
        int sequence_number;
        json data_state;
        string data_digest;
        shared_ptr<Checkpoint> local_checkpoint;
        vector<shared_ptr<Checkpoint>> valid_checkpoints_received;
        vector<string> nodes_received;
        vector<shared_ptr<Checkpoint>> quorum_checkpoints;
        vector<string> quorum_nodes;
        int valid_checkpoint_count = 0;
        bool is_stable = false;
        mutex checkpoint_mtx;
    };
    unordered_map<int, shared_ptr<CheckpointData>> checkpointLog_;
    struct ViewChangeData {
        int view_number;
        shared_ptr<ViewChange> local_view_change;
        vector<shared_ptr<ViewChange>> valid_view_changes_received;
        vector<string> nodes_received;
        vector<shared_ptr<ViewChange>> quorum_view_changes;
        vector<string> quorum_nodes;
        int valid_view_change_count = 0;
        bool has_consensus = false;
        mutex view_change_mtx;
    };
    unordered_map<int, shared_ptr<ViewChangeData>> viewChangeLog_;
    vector<string> newViewMessageList_;

    void printLog() 
    {
        lock_guard<mutex> l(logMx_);
        for (size_t i = lastLogPrinted_; i < log_.size(); i++)
        {
            cout <<log_[i] << "\n";
        }
        lastLogPrinted_ = log_.size();
    }

    void printDB()
    {
        lock_guard<mutex> l(dataStateFileMx_);
        json data_store = openJsonFile(dataStateFile_);
        cout << "Data Store: \n" << data_store.dump() << "\n";
        cout << "Last Executed Sequence Number: " + to_string(lastExecutedSeqNum_.load()) << "\n";
        cout << "Last Stable Checkpointed Sequence Number: " + to_string(lastStableCheckpointSeqNum_.load()) << "\n";
        int checkpointSeqNum = 0;
        {
            lock_guard<mutex> lk(checkpointLogMx_);
            auto maxIt = max_element(
                checkpointLog_.begin(), checkpointLog_.end(),
                [](const auto& a, const auto& b) {
                    return a.first < b.first;
                });

            if (maxIt != checkpointLog_.end())
                checkpointSeqNum = maxIt->first;
        }
        
        cout << "Last Checkpointed Sequence Number: " + to_string(checkpointSeqNum) << "\n";
        cout << "Low Water Mark: " + to_string(lowWaterMark_.load()) + " | High Water Mark: " + to_string(highWaterMark_.load()) << "\n";
        cout << "Current view : " + to_string(currentView_.load()) << "\n";
        {
            lock_guard<mutex> l(leaderMx_);
            if (isLeader_.load()) cout << "I am primary!\n";
            else cout << "Primary is "+leaderId_+"\n";
        }
    }

    void printStatus(int seqNum)
    {
        cout << "Status for sequence number " + to_string(seqNum) +" is : ";
        shared_ptr<MainNode::PBFTLogEntry> entry;
        {
            lock_guard<mutex> lk(sequenceLogMapMx_);
            auto it = sequenceLogMap_.find(seqNum);
            if (it != sequenceLogMap_.end()) {
                entry = it->second;
                logger_.log(logLevel::DEBUG, "[Func: requestClientMessage] STATE - Found PBFTLogEntry for seq=" + to_string(seqNum));
                if (entry->is_executed) {
                    string add_string;
                    if (entry->request_digest == "NULL") add_string = "It's NoOp.";
                    if (entry->is_fast_path) {
                        cout << "E: Executed with fast path. "+add_string+"\n";
                    }
                    else {
                        cout << "E: Executed with normal path. "+add_string+"\n";
                    }
                }
                else if (entry->is_committed) {
                    if (seqNum <= lastExecutedSeqNum_.load())
                    {
                        cout << "E: Executed due to state synchronization via Checkpointing.\n";
                        return;
                    }
                    
                    string add_string;
                    if (entry->request_digest == "NULL") add_string = "It's NoOp.";
                    cout << "C: Committed with normal path. "+add_string+"\n";
                }
                else if (entry->is_prepared) {
                    if (seqNum <= lastExecutedSeqNum_.load())
                    {
                        cout << "E: Executed due to state synchronization via Checkpointing.\n";
                        return;
                    }
                    string add_string;
                    if (entry->request_digest == "NULL") add_string = "It's NoOp.";
                    if (entry->is_fast_path) {
                        cout << "C: Committed with fast path. "+add_string+"\n";
                    }
                    else {
                        cout << "P: Prepared with normal path. "+add_string+"\n";
                    }
                }
                else {
                    if (seqNum <= lastExecutedSeqNum_.load())
                    {
                        cout << "E: Executed due to state synchronization via Checkpointing.\n";
                        return;
                    }
                    string add_string;
                    if (entry->request_digest == "NULL") add_string = "It's NoOp.";
                    cout << "PP: Pre-prepared. "+add_string+"\n";
                }
            }
            else {
                logger_.log(logLevel::WARN, "[Func: requestClientMessage] BRANCH: No PBFTLogEntry found for seq=" + to_string(seqNum));
                if (seqNum <= lastExecutedSeqNum_.load())
                {
                    cout << "E: Executed due to state synchronization via Checkpointing.\n";
                    return;
                }
                cout << "X: No Status.\n";
            }
        }
    }

    void printView()
    {
        lock_guard<mutex> l(newViewMessageListMx_);
        cout << "Total number of NEW VIEW messages generated: " + to_string(newViewMessageList_.size()) + "\n";
        for (const auto& nv: newViewMessageList_) {
            cout << nv <<"\n";
        }
    }

    pair<bool, json> requestDataStateAndValidate(const string& node, const string& digest, const int& seqNum) {
        logger_.log(logLevel::INFO, "[Func: requestDataStateAndValidate] ENTER - Requesting data state from node " + node + " for seqNum=" + to_string(seqNum));
        DataStateRequest request;
        DataStateResponse response;
        json data_state;
        request.set_node(id_);
        auto* request_content = request.mutable_data_state_request_content();
        request_content->set_sequence_number(seqNum);
        request.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(*request_content)));
        logger_.log(logLevel::DEBUG, "[Func: requestDataStateAndValidate] STATE - Signed DataStateRequest prepared.");

        if (byzantineStatus_.load() && darkAttackActivated_ && nodes_[node].byzantineVictim) {
            logger_.log(logLevel::INFO, "[Func: requestDataStateAndValidate] ACTION - Skipping sending DataStateRequest message to "+node);
        }
        else {
            {
                lock_guard<mutex> l(logMx_);
                log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: requestDataStateAndValidate] - "+request.DebugString()));
            }
            grpc::ClientContext context;
            grpc::Status status = nodes_[node].nodeStub->SendDataStateRequest(&context, request, &response);
            
            if (status.ok()) {
                logger_.log(logLevel::DEBUG, "[Func: requestDataStateAndValidate] INFO - Response received successfully from node " + node);
                if (!bls_utils::verify_signature(nodes_[node].publicKey, response.signature(), serializeDeterministic(response.data_state_response_content()))) {
                    logger_.log(logLevel::ERROR, "[Func: requestDataStateAndValidate] ERROR - Signature verification failed for response from node " + node);
                    logger_.log(logLevel::INFO, "[Func: requestDataStateAndValidate] EXIT - Returning (false, empty data_state).");
                    return {false, data_state};
                }
                logger_.log(logLevel::DEBUG, "[Func: requestDataStateAndValidate] INFO - Signature verified successfully for node " + node);
                json stateToCheck = json::parse(response.data_state_response_content().data_state());
                if (digestFromJson(stateToCheck) == digest) {
                    logger_.log(logLevel::INFO, "[Func: requestDataStateAndValidate] EXIT - Digest matched, returning true with state.");
                    return {true, stateToCheck};
                }
                logger_.log(logLevel::WARN, "[Func: requestDataStateAndValidate] BRANCH: Digest mismatch for seqNum=" + to_string(seqNum));
            } else {
                logger_.log(logLevel::ERROR, "[Func: requestDataStateAndValidate] ERROR - RPC call to SendDataStateRequest failed for node " + node);
            }
        }
        logger_.log(logLevel::INFO, "[Func: requestDataStateAndValidate] EXIT - Returning (false, empty data_state).");
        return {false, data_state};
    }

    void requestClientMessage(const int& seqNum) {
        logger_.log(logLevel::INFO, "[Func: requestClientMessage] ENTER - Requesting client message for seqNum=" + to_string(seqNum));
        ClientMessageRequest request;
        request.set_node(id_);
        auto* request_content = request.mutable_client_message_request_content();
        request_content->set_sequence_number(seqNum);
        request.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(*request_content)));
        logger_.log(logLevel::DEBUG, "[Func: requestClientMessage] STATE - Signed ClientMessageRequest prepared.");
        {
            lock_guard<mutex> l(logMx_);
            log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: requestClientMessage] - "+request.DebugString()));
        }
        for (const auto& [id, node]:nodes_) {
            if (byzantineStatus_.load() && darkAttackActivated_ && node.byzantineVictim) {
                logger_.log(logLevel::DEBUG, "[Func: requestClientMessage] ACTION - Skipping sending SendClientMessageRequest to " + id);
            }
            else {
                ClientMessageResponse response;
                grpc::ClientContext context;
                grpc::Status status = node.nodeStub->SendClientMessageRequest(&context, request, &response);
                
                if (status.ok()) {
                    logger_.log(logLevel::DEBUG, "[Func: requestClientMessage] INFO - Response received successfully from node " + id);
                    {
                        lock_guard<mutex> l(logMx_);
                        log_.push_back(logger_.log(logLevel::MSG_IN, "[RPC: requestClientMessage] - "+response.DebugString()));
                    }
                    if (!bls_utils::verify_signature(node.publicKey, response.signature(), serializeDeterministic(response.client_message_response_content()))) {
                        logger_.log(logLevel::ERROR, "[Func: requestClientMessage] ERROR - Signature verification failed for response from node " + id + ", will retry with some other node.");
                    }
                    shared_ptr<MainNode::PBFTLogEntry> entry;
                    {
                        lock_guard<mutex> lk(sequenceLogMapMx_);
                        auto it = sequenceLogMap_.find(seqNum);
                        if (it != sequenceLogMap_.end()) {
                            entry = it->second;
                            logger_.log(logLevel::DEBUG, "[Func: requestClientMessage] STATE - Found PBFTLogEntry for seq=" + to_string(seqNum));
                        }
                        else {
                            logger_.log(logLevel::WARN, "[Func: requestClientMessage] BRANCH: No PBFTLogEntry found for seq=" + to_string(seqNum));
                            continue;
                        }
                    }

                    {
                        lock_guard<mutex> lk(entry->mtx);
                        if (entry->request_digest == computeDigest(serializeDeterministic(response.client_message_response_content().client_request())))
                        entry->client_request = make_shared<Request>(response.client_message_response_content().client_request());
                        else continue;
                    }
                    logger_.log(logLevel::INFO, "[Func: requestClientMessage] EXIT - Client message retrieved successfully.");
                    return;
                }
            }
        }
        logger_.log(logLevel::ERROR, "[Func: requestClientMessage] ERROR - RPC call to SendClientMessageRequest failed.");
        logger_.log(logLevel::INFO, "[Func: requestClientMessage] EXIT - Request not found.");
        return;
    }

    Reply executeTransaction(shared_ptr<Request> clientRequest, int view_number) {
        logger_.log(logLevel::INFO, "[Func: executeTransaction] ENTER - Executing transaction for client " + clientRequest->client() + " in view " + to_string(view_number));
        Reply reply;
        reply.set_node(id_);
        ReplyContent* reply_content = reply.mutable_reply_content();
        reply_content->set_message_type(REPLY);
        reply_content->set_view_number(view_number);
        reply_content->set_client(clientRequest->client());
        reply_content->set_timestamp(clientRequest->request_content().timestamp());
        logger_.log(logLevel::DEBUG, "[Func: executeTransaction] STATE - Preparing result based on operation type.");
        if (clientRequest->request_content().operation().type()==READ_PROMOTED_TO_READ_WRITE) {
            logger_.log(logLevel::DEBUG, "[Func: executeTransaction] BRANCH: READ_PROMOTED_TO_READ_WRITE -> executing read-only operation.");
            reply_content->mutable_result()->CopyFrom(executeReadOnlyOperation(clientRequest->request_content().operation().transaction().sender()));
        }
        else {
            logger_.log(logLevel::DEBUG, "[Func: executeTransaction] BRANCH: READ_WRITE -> executing read-write operation.");
            reply_content->mutable_result()->CopyFrom(executeReadWriteOperation(clientRequest->request_content().operation().transaction()));
        }
        reply.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(*reply_content)));
        logger_.log(logLevel::INFO, "[Func: executeTransaction] EXIT - Transaction executed successfully, reply signed and returned.");
        return reply;
    }

    void executeSequence(const int& sequence_number) {
        logger_.log(logLevel::INFO, "[Func: executeSequence] ENTER - Starting execution for sequence_number=" + to_string(sequence_number));
        shared_ptr<MainNode::PBFTLogEntry> entry;
        {
            lock_guard<mutex> lk(sequenceLogMapMx_);
            logger_.log(logLevel::DEBUG, "[Func: executeSequence] ACTION - Searching sequenceLogMap_ for seq=" + to_string(sequence_number));
            auto it = sequenceLogMap_.find(sequence_number);
            if (it != sequenceLogMap_.end()) {
                entry = it->second;
                logger_.log(logLevel::DEBUG, "[Func: executeSequence] STATE - Found PBFTLogEntry for seq=" + to_string(sequence_number));
            }
            else {
                logger_.log(logLevel::WARN, "[Func: executeSequence] BRANCH: No PBFTLogEntry found for seq=" + to_string(sequence_number));
                logger_.log(logLevel::INFO, "[Func: executeSequence] EXIT - Returning early.");
                return;
            }
        }

        {
            lock_guard<mutex> lk(entry->mtx);
            if (sequence_number <= lastExecutedSeqNum_.load()) {
                logger_.log(logLevel::DEBUG, "[Func: executeSequence] BRANCH: Sequence already executed or stale (seq=" + to_string(sequence_number) + ")");
                if (entry->request_digest!="NULL") {
                    logger_.log(logLevel::DEBUG, "[Func: executeSequence] ACTION - Resending reply to client " + entry->reply.reply_content().client());
                    sendReplyToClient(entry->reply, entry->reply.reply_content().client());
                }
                logger_.log(logLevel::INFO, "[Func: executeSequence] EXIT - Returning as sequence already executed.");
                return;
            }
        }
        
        {
            lock_guard<mutex> lk(executionInProgressMx_);
            int seqNumToExecute = lastExecutedSeqNum_.load() + 1;
            logger_.log(logLevel::DEBUG, "[Func: executeSequence] STATE - Starting execution loop from seqNumToExecute=" + to_string(seqNumToExecute));
            shared_ptr<MainNode::PBFTLogEntry> currEntry;
            while (true) {
                {
                    lock_guard<mutex> lk(sequenceLogMapMx_);
                    auto it = sequenceLogMap_.find(seqNumToExecute);
                    if (it != sequenceLogMap_.end()) {
                        currEntry = it->second;
                    }
                    else {
                        logger_.log(logLevel::DEBUG, "[Func: executeSequence] BRANCH: No entry found for seqNumToExecute=" + to_string(seqNumToExecute));
                        if (seqNumToExecute > sequence_number && seqNumToExecute > sequenceNumber_.load() && timer_.is_running()) {
                            logger_.log(logLevel::DEBUG, "[Func: executeSequence] ACTION - Stopping timer (no future entries to execute).");
                            timer_.stop();
                        }
                        logger_.log(logLevel::INFO, "[Func: executeSequence] EXIT - No further entries to execute.");
                        return;
                    }
                }
                {
                    lock_guard<mutex> lk(currEntry->mtx);
                    if (currEntry->is_executed) {
                        logger_.log(logLevel::DEBUG, "[Func: executeSequence] BRANCH: Entry already executed for seq=" + to_string(seqNumToExecute));
                        if (currEntry->request_digest!="NULL") sendReplyToClient(currEntry->reply, currEntry->reply.reply_content().client());
                        seqNumToExecute++;
                        continue;
                    }

                    if (currEntry->is_ready_for_execution) {
                        logger_.log(logLevel::DEBUG, "[Func: executeSequence] BRANCH: Entry ready for execution (seq=" + to_string(seqNumToExecute) + ")");
                        if (timer_.is_running())   timer_.reset();
                        if (currEntry->request_digest=="NULL") {
                            logger_.log(logLevel::DEBUG, "[Func: executeSequence] BRANCH: NULL digest -> marking executed without operation.");
                            currEntry->is_executed = true;
                            lastExecutedSeqNum_.fetch_add(1);
                        }
                        else {
                            logger_.log(logLevel::INFO, "[Func: executeSequence] ACTION - Executing transaction for seq=" + to_string(seqNumToExecute));
                            currEntry->reply = executeTransaction(currEntry->client_request, currentView_.load());
                            currEntry->is_executed = true;
                            lastExecutedSeqNum_.fetch_add(1);
                            {
                                lock_guard<mutex> lk1(clientTransactionsExecutionLogMx_);
                                clientTransactionsExecutionLog_[{currEntry->client_request->client(), currEntry->client_request->request_content().timestamp()}] = make_shared<Reply>(currEntry->reply);
                                logger_.log(logLevel::DEBUG, "[Func: executeSequence] STATE - Transaction logged for client " + currEntry->client_request->client());
                            }
                            sendReplyToClient(currEntry->reply, currEntry->client_request->client());
                            logger_.log(logLevel::DEBUG, "[Func: executeSequence] ACTION - Reply sent to client " + currEntry->client_request->client());
                        }

                        if (lastExecutedSeqNum_.load()%checkpointInterval_.load()==0)
                        {
                            logger_.log(logLevel::INFO, "[Func: executeSequence] ACTION - Generating checkpoint at seq=" + to_string(lastExecutedSeqNum_.load()));
                            shared_ptr<CheckpointData> checkpointEntry;
                            {
                                lock_guard<mutex> lk(checkpointLogMx_);
                                auto it = checkpointLog_.find(lastExecutedSeqNum_.load());
                                if (it != checkpointLog_.end()) {
                                    checkpointEntry = it->second;
                                }
                                else {
                                    checkpointEntry = make_shared<CheckpointData>();
                                    checkpointLog_[lastExecutedSeqNum_.load()] = checkpointEntry;
                                }
                            }
                            {
                                lock_guard<mutex> l(checkpointEntry->checkpoint_mtx);
                                checkpointEntry->sequence_number = lastExecutedSeqNum_.load();
                                auto [ds, dd] = getDataStateAndDigest(); 
                                checkpointEntry->data_state=ds; checkpointEntry->data_digest=dd;
                                logger_.log(logLevel::DEBUG, "[Func: executeSequence] STATE - Checkpoint digest=" + dd);
                                Checkpoint checkpoint;
                                checkpoint.set_node(id_);
                                CheckpointContent* checkpoint_content = checkpoint.mutable_checkpoint_content();
                                checkpoint_content->set_sequence_number(lastExecutedSeqNum_.load());
                                checkpoint_content->set_message_type(CHECKPOINT);
                                checkpoint_content->set_data_digest(checkpointEntry->data_digest);
                                checkpoint.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(*checkpoint_content)));
                                checkpointEntry->local_checkpoint = make_shared<Checkpoint>(checkpoint);
                                checkpointEntry->valid_checkpoint_count++;
                                checkpointEntry->valid_checkpoints_received.push_back(checkpointEntry->local_checkpoint);
                                checkpointEntry->nodes_received.push_back(id_);
                                logger_.log(logLevel::INFO, "[Func: executeSequence] ACTION - Broadcasting checkpoint seq=" + to_string(lastExecutedSeqNum_.load()));
                                broadcastCheckpoint(checkpoint);
                                if (checkpointEntry->valid_checkpoint_count >= quorumSize_.load() && !checkpointEntry->is_stable && lastExecutedSeqNum_.load() > lastStableCheckpointSeqNum_.load()) {
                                    logger_.log(logLevel::DEBUG, "[Func: executeSequence] BRANCH: Quorum received for Checkpoint and seq > lastStableCheckpointSeqNum_ -> updating stable checkpoint details");
                                    checkpointEntry->quorum_checkpoints = checkpointEntry->valid_checkpoints_received;
                                    checkpointEntry->quorum_nodes = checkpointEntry->nodes_received;
                                    checkpointEntry->is_stable = true;
                                    lastStableCheckpointSeqNum_.store(lastExecutedSeqNum_.load());
                                    lowWaterMark_.store(lastExecutedSeqNum_.load());
                                    highWaterMark_.store(lastExecutedSeqNum_.load() + validSequenceRange_.load());
                                }
                            }
                        }
                    }
                    else {
                        logger_.log(logLevel::DEBUG, "[Func: executeSequence] BRANCH: Entry not ready for execution (seq=" + to_string(seqNumToExecute) + ")");
                        if (seqNumToExecute > sequence_number && seqNumToExecute <= sequenceNumber_.load() && timer_.is_running()) {
                            logger_.log(logLevel::DEBUG, "[Func: executeSequence] ACTION - Resetting timer (waiting for next sequence).");
                            timer_.reset();
                        }
                        else if (seqNumToExecute > sequence_number && seqNumToExecute > sequenceNumber_.load() && timer_.is_running()) {
                            logger_.log(logLevel::INFO, "[Func: executeSequence] EXIT - No pending sequence numbers, hence stopping the timer.");
                            timer_.stop();
                        }
                        return;
                    }
                }
                seqNumToExecute++;
            }
        }
    }

    string serializeMap(const map<string, int>& data) {
        logger_.log(logLevel::INFO, "[Func: serializeMap] ENTER - Serializing map of size " + to_string(data.size()));
        stringstream ss;
        for (const auto& [key, value] : data) {
            logger_.log(logLevel::DEBUG, "[Func: serializeMap] STATE - Adding key=" + key + ", value=" + to_string(value));
            ss << key << ":" << value << ";";
        }
        logger_.log(logLevel::INFO, "[Func: serializeMap] EXIT - Serialization complete.");
        return ss.str();
    }

    string digestFromJson(const json& j) {
        logger_.log(logLevel::INFO, "[Func: digestFromJson] ENTER - Computing digest from JSON data.");
        map<string, int> orderedData;
        for (auto it = j.begin(); it != j.end(); ++it) {
            orderedData[it.key()] = it.value().get<int>();
            logger_.log(logLevel::DEBUG, "[Func: digestFromJson] STATE - Key=" + it.key() + ", Value=" + to_string(it.value().get<int>()));
        }

        string serialized = serializeMap(orderedData);
        logger_.log(logLevel::DEBUG, "[Func: digestFromJson] STATE - Serialized map: " + serialized);
        string digest = computeDigest(serialized);
        logger_.log(logLevel::INFO, "[Func: digestFromJson] EXIT - Digest computed: " + digest);
        return digest;
    }

    pair<json, string> getDataStateAndDigest() {
        logger_.log(logLevel::INFO, "[Func: getDataStateAndDigest] ENTER - Reading data state and computing digest.");
        lock_guard<mutex> lg(dataStateFileMx_);
        json dataState = openJsonFile(dataStateFile_);
        logger_.log(logLevel::DEBUG, "[Func: getDataStateAndDigest] STATE - Data state loaded from file.");
        string digest = digestFromJson(dataState);
        logger_.log(logLevel::INFO, "[Func: getDataStateAndDigest] EXIT - Digest=" + digest);
        return {dataState, digest};
    }

    void handleCollectedCommitAsync(shared_ptr<PBFTLogEntry> entry) {
        logger_.log(logLevel::INFO, "[Func: handleCollectedCommitAsync] ENTER - Handling collected commit for seq=" + 
                    to_string(entry->commit_data.local_commit->commit_content().sequence_number()));
        CollectedCommit collectedCommit;
        {
            lock_guard<mutex> lock(entry->commit_data.commit_mtx);
            logger_.log(logLevel::DEBUG, "[Func: handleCollectedCommitAsync] ACTION - Acquired commit_mtx to build CollectedCommit.");
            collectedCommit.set_node(id_);
            collectedCommit.mutable_commit_content()->CopyFrom(entry->commit_data.local_commit->commit_content());
            vector<int> node_ids;
            for (const auto& signer : entry->commit_data.quorum_nodes) {
                node_ids.push_back(signer[1]-'0');
                logger_.log(logLevel::DEBUG, "[Func: handleCollectedCommitAsync] STATE - Added signer node=" + signer);
            }
            collectedCommit.set_signature(bls_utils::aggregate_signatures(entry->commit_data.quorum_signatures, node_ids));
            logger_.log(logLevel::DEBUG, "[Func: handleCollectedCommitAsync] STATE - Aggregated commit signature generated.");
            entry->commit_data.collected_commit = make_shared<CollectedCommit>(collectedCommit);
        }
        {
            lock_guard<mutex> lk(entry->mtx);
            logger_.log(logLevel::DEBUG, "[Func: handleCollectedCommitAsync] ACTION - Marking entry as committed and ready for execution.");
            entry->is_committed = true;
            entry->is_ready_for_execution = true;
        }
        logger_.log(logLevel::INFO, "[Func: handleCollectedCommitAsync] ACTION - Scheduling sequence execution for seq=" + 
                    to_string(collectedCommit.commit_content().sequence_number()));
        threadPool_.enqueue([this, sequence_number = collectedCommit.commit_content().sequence_number()]() {
            executeSequence(sequence_number);
        });
        logger_.log(logLevel::INFO, "[Func: handleCollectedCommitAsync] ACTION - Broadcasting CollectedCommit.");
        broadcastCollectedCommit(collectedCommit);
        logger_.log(logLevel::INFO, "[Func: handleCollectedCommitAsync] EXIT - Completed handling collected commit.");
    }

    void handleCollectedPrepareAsync(shared_ptr<PBFTLogEntry> entry) {
        logger_.log(logLevel::INFO, "[Func: handleCollectedPrepareAsync] ENTER - Handling collected prepare for seq=" + to_string(entry->sequence_number));
        sleep_for(milliseconds(fastPathWaitTimeMs_.load()));
        logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Sleep complete (" + to_string(fastPathWaitTimeMs_.load()) + "ms).");

        CollectedPrepare collected_prepare;
        bool is_fast_path = false;
        {
            lock_guard<mutex> lock(entry->prepare_data.prepare_mtx);
            logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] ACTION - Acquired prepare_mtx to build CollectedPrepare.");
            collected_prepare.set_node(id_);
            collected_prepare.mutable_prepare_content()->CopyFrom(entry->prepare_data.local_prepare->prepare_content());
            logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - CP content signed on "+entry->prepare_data.local_prepare->prepare_content().SerializeAsString());
            if (entry->prepare_data.has_total_consensus) {
                logger_.log(logLevel::INFO, "[Func: handleCollectedPrepareAsync] INFO - Total consensus achieved in Prepare phase.");
                vector<int> node_ids;
                for (const auto& signer : entry->prepare_data.nodes_received){
                    int s = signer[1]-'0';
                    logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Fast path signer: " + to_string(s));
                    node_ids.push_back(s);
                }
                for (const auto& sign : entry->prepare_data.fast_path_agg_signatures){
                    logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Aggregated Signatures: " + sign);
                }
                collected_prepare.set_is_fast_path(true);
                logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Fast Path Signatures Vector Size : " + to_string(entry->prepare_data.fast_path_agg_signatures.size()));
                collected_prepare.set_signature(bls_utils::aggregate_signatures(entry->prepare_data.fast_path_agg_signatures, node_ids));
                logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Final Aggregated Signature: " + collected_prepare.signature());
                is_fast_path = true;
                logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Fast path aggregated signature created.");
            }
            else {
                logger_.log(logLevel::INFO, "[Func: handleCollectedPrepareAsync] INFO - Quorum consensus achieved (non-fast path).");
                vector<int> node_ids;
                for (const auto& signer : entry->prepare_data.quorum_nodes) {
                    int s = signer[1]-'0';
                    logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Quorum signer: " + to_string(s));
                    node_ids.push_back(s);
                }
                collected_prepare.set_is_fast_path(false);
                logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Quorum Signatures Vector Size : " + to_string(entry->prepare_data.quorum_signatures.size()));
                collected_prepare.set_signature(bls_utils::aggregate_signatures(entry->prepare_data.quorum_signatures, node_ids));
                logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Quorum aggregated signature created.");
            }
            entry->prepare_data.collected_prepare = make_shared<CollectedPrepare>(collected_prepare);
            logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - CollectedPrepare object stored in entry.");
        }

        if (is_fast_path) {
            logger_.log(logLevel::INFO, "[Func: handleCollectedPrepareAsync] BRANCH: Using fast path execution.");
            {
                lock_guard<mutex> lk(entry->mtx);
                entry->is_fast_path = true;
                entry->is_ready_for_execution = true;
                entry->is_prepared = true;
                logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Entry marked prepared and ready for fast execution.");
            }
            threadPool_.enqueue([this, sequence_number = entry->sequence_number]() {
                executeSequence(sequence_number);
            });
            logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] ACTION - Scheduled executeSequence for seq=" + to_string(entry->sequence_number));
        }
        else {
            logger_.log(logLevel::INFO, "[Func: handleCollectedPrepareAsync] BRANCH: Proceeding with standard commit path.");
            {
                lock_guard<mutex> lk(entry->mtx);
                entry->is_prepared = true;
                logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Entry marked as prepared (non-fast path).");
            }
            Commit commit;
            commit.mutable_commit_content()->CopyFrom(entry->prepare_data.local_prepare->prepare_content());
            commit.mutable_commit_content()->set_message_type(COMMIT);
            commit.set_node(id_);
            commit.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(commit.commit_content())));        
            {
                lock_guard<mutex> lk(entry->commit_data.commit_mtx);
                logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] ACTION - Storing local commit and updating commit_data.");
                entry->commit_data.local_commit = make_shared<Commit>(commit);
                entry->commit_data.valid_commits_received.push_back(entry->commit_data.local_commit);
                entry->commit_data.valid_commit_count++;
                entry->commit_data.nodes_received.push_back(id_);
                entry->commit_data.agg_signatures.push_back(commit.signature());
                logger_.log(logLevel::DEBUG, "[Func: handleCollectedPrepareAsync] STATE - Commit data updated for seq=" + to_string(entry->sequence_number));
            }
        }
        logger_.log(logLevel::INFO, "[Func: handleCollectedPrepareAsync] ACTION - Broadcasting CollectedPrepare for seq=" + to_string(entry->sequence_number));
        broadcastCollectedPrepare(collected_prepare);
        logger_.log(logLevel::INFO, "[Func: handleCollectedPrepareAsync] EXIT - Completed handling collected prepare.");
    }

    void setLeader()
    {
        logger_.log(logLevel::INFO, "[Func: setLeader] ENTER - Determining leader based on currentView=" + to_string(currentView_.load()));
        int view_mod = currentView_.load()%totalNodes_.load();
        if ((view_mod == processId_) || (view_mod==0 && processId_==7))
        {
            isLeader_.store(true);
            logger_.log(logLevel::INFO, "[Func: setLeader] STATE - This node (" + to_string(processId_) + ") is the leader.");
        }
        else
        {
            isLeader_.store(false);
            lock_guard<mutex> lg(leaderMx_);
            leaderId_ = viewLeaderMap_[view_mod];
            logger_.log(logLevel::INFO, "[Func: setLeader] STATE - Leader set to node " + leaderId_);
        }
        logger_.log(logLevel::INFO, "[Func: setLeader] EXIT - Leader determination complete.");
    }

    void ProcessCompletions() {
        logger_.log(logLevel::INFO, "[Func: ProcessCompletions] ENTER - Starting to process gRPC completions.");
        void* tag;
        bool ok;
        while (cq_.Next(&tag, &ok)) {
            auto* call = static_cast<AsyncNodeCall*>(tag);
            delete call;
            logger_.log(logLevel::DEBUG, "[Func: ProcessCompletions] ACTION - Completed async call cleanup.");
        }
        logger_.log(logLevel::INFO, "[Func: ProcessCompletions] EXIT - All completions processed.");
    }

    void sendReplyToClient(const Reply &reply, const string &clientId)
    {
        logger_.log(logLevel::INFO, "[Func: sendReplyToClient] ENTER - Sending reply to client " + clientId);
        {
            lock_guard<mutex> l(logMx_);
            log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: sendReplyToClient] - "+reply.DebugString()));
        }
        auto* call = new AsyncNodeCall;
        call->response_reader = clients_[clientId].nodeStub->AsyncSendReply(&call->context, reply, &cq_);
        call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        logger_.log(logLevel::DEBUG, "[Func: sendReplyToClient] ACTION - Async reply send initiated for client " + clientId);
        logger_.log(logLevel::INFO, "[Func: sendReplyToClient] EXIT - Reply dispatched to client " + clientId);
    }

    tuple<bool, vector<shared_ptr<Checkpoint>>, string> validateCheckpointEvidence(const ViewChangeContent& vcc) {
        logger_.log(logLevel::INFO, "[Func: validateCheckpointEvidence] ENTER - Validating checkpoint evidence for ViewChangeContent.");
        vector<shared_ptr<Checkpoint>> checkpointQuorum;
        string data_digest;
        if (vcc.checkpoint_evidence_size() < quorumSize_.load()) {
            logger_.log(logLevel::WARN, "[Func: validateCheckpointEvidence] BRANCH: Checkpoint evidence size (" + 
                        to_string(vcc.checkpoint_evidence_size()) + ") less than quorum size (" + 
                        to_string(quorumSize_.load()) + ")");
            logger_.log(logLevel::INFO, "[Func: validateCheckpointEvidence] EXIT - Validation failed (insufficient evidence).");
            return {false, checkpointQuorum, data_digest};
        }
        for (const auto& chk : vcc.checkpoint_evidence()) {
            string publicKey;
            if (chk.node() == id_) publicKey = publicKey_; else publicKey = nodes_[chk.node()].publicKey;
            if (!bls_utils::verify_signature(publicKey, chk.signature(), serializeDeterministic(chk.checkpoint_content()))) {
                logger_.log(logLevel::ERROR, "[Func: validateCheckpointEvidence] ERROR - Signature verification failed for node " + chk.node());
                logger_.log(logLevel::INFO, "[Func: validateCheckpointEvidence] EXIT - Returning false due to invalid signature.");
                return {false, checkpointQuorum, data_digest};
            }
            if (data_digest.empty()) {
                data_digest = chk.checkpoint_content().data_digest();
                logger_.log(logLevel::DEBUG, "[Func: validateCheckpointEvidence] STATE - Initial digest set to " + data_digest);
            }
            else if (data_digest != chk.checkpoint_content().data_digest()) {
                logger_.log(logLevel::ERROR, "[Func: validateCheckpointEvidence] ERROR - Digest mismatch detected for node " + chk.node());
                logger_.log(logLevel::INFO, "[Func: validateCheckpointEvidence] EXIT - Returning false due to inconsistent digest.");
                return {false, checkpointQuorum, data_digest};
            }
            checkpointQuorum.push_back(make_shared<Checkpoint>(chk));
            logger_.log(logLevel::DEBUG, "[Func: validateCheckpointEvidence] STATE - Checkpoint from node " + chk.node() + " added to quorum.");
        }
        logger_.log(logLevel::INFO, "[Func: validateCheckpointEvidence] EXIT - Validation succeeded, quorum size=" + to_string(checkpointQuorum.size()));
        return {true, checkpointQuorum, data_digest};
    }

    bool validateCollectedPrepare(const CollectedPrepare& collectedPrepare) {
        logger_.log(logLevel::INFO, "[Func: validateCollectedPrepare] ENTER - Validating collected prepare from node " + collectedPrepare.node());

        if (collectedPrepare.is_fast_path() && !bls_utils::verify_signature(aggFastPublicKey_, collectedPrepare.signature(), serializeDeterministic(collectedPrepare.prepare_content()))) {
            logger_.log(logLevel::ERROR, "[Func: validateCollectedPrepare] ERROR - Verification failed for fast path aggregated signature (node " + collectedPrepare.node() + ")");
            logger_.log(logLevel::INFO, "[Func: validateCollectedPrepare] EXIT - Returning false for fast path verification failure.");
            return false;
        }
        else if (!bls_utils::verify_signature(aggPublicKey_, collectedPrepare.signature(), serializeDeterministic(collectedPrepare.prepare_content()))) {
            logger_.log(logLevel::ERROR, "[Func: validateCollectedPrepare] ERROR - Verification failed for aggregated signature (node " + collectedPrepare.node() + ")");
            logger_.log(logLevel::INFO, "[Func: validateCollectedPrepare] EXIT - Returning false for quorum verification failure.");
            return false;
        }

        logger_.log(logLevel::INFO, "[Func: validateCollectedPrepare] EXIT - Validation successful for node " + collectedPrepare.node());
        return true;
    }

    void computePrePrepareSet(NewViewContent& nvc, int viewNo) {
        logger_.log(logLevel::INFO, "[Func: computePrePrepareSet] ENTER - Building PrePrepare set for view " + to_string(viewNo));
        int maxStableSeqNum = 0;
        int maxPreparedSeqNum = 0;
        vector<shared_ptr<Checkpoint>> stableCheckpointEvidence;
        string stableDataDigest;
        string stableNode;
        for (const auto& vce : nvc.view_change_evidence()) {
            if (vce.view_change_content().last_stable_sequence_number()>maxStableSeqNum) {
                auto [result, evidence, data_digest] = validateCheckpointEvidence(vce.view_change_content());
                if (result) {
                    maxStableSeqNum = vce.view_change_content().last_stable_sequence_number();
                    stableCheckpointEvidence = evidence;
                    stableDataDigest = data_digest;
                    stableNode = vce.node();
                }
            }
            int max_seq_num_for_vce = 0;
            const auto& prep = vce.view_change_content().prepared_evidence();
            if (!prep.empty()) {
                for (const auto& kv : prep) max_seq_num_for_vce = max(max_seq_num_for_vce, kv.first);
            }
            maxPreparedSeqNum = max(maxPreparedSeqNum, max_seq_num_for_vce);
        }
        logger_.log(logLevel::DEBUG, "[Func: computePrePrepareSet] STATE - maxStableSeqNum=" + to_string(maxStableSeqNum) + ", maxPreparedSeqNum=" + to_string(maxPreparedSeqNum));

        if (lastStableCheckpointSeqNum_.load()<maxStableSeqNum) {
            logger_.log(logLevel::INFO, "[Func: computePrePrepareSet] ACTION - Advancing stable checkpoint from " + to_string(lastStableCheckpointSeqNum_.load()) + " to " + to_string(maxStableSeqNum));
            shared_ptr<MainNode::CheckpointData> checkpointEntry;
            bool new_entry = false;
            {
                lock_guard<mutex> lg(checkpointLogMx_);
                auto cit = checkpointLog_.find(maxStableSeqNum);
                if (cit != checkpointLog_.end()) {
                    checkpointEntry = cit->second;
                }
                else {
                    checkpointEntry = make_shared<MainNode::CheckpointData>();
                    checkpointLog_[maxStableSeqNum] = checkpointEntry;
                    new_entry = true;
                }
            }
            json stable_data_state;
            bool is_sync_required = false;
            {
                lock_guard<mutex> lk(checkpointEntry->checkpoint_mtx);
                checkpointEntry->sequence_number = maxStableSeqNum;
                if (checkpointEntry->data_digest!=stableDataDigest) {
                    checkpointEntry->data_digest = stableDataDigest;
                    checkpointEntry->data_state.clear();
                }
                checkpointEntry->quorum_checkpoints = stableCheckpointEvidence;
                checkpointEntry->valid_checkpoint_count = stableCheckpointEvidence.size();
                checkpointEntry->valid_checkpoints_received = stableCheckpointEvidence;
                for (const auto& chp : stableCheckpointEvidence) {
                    checkpointEntry->nodes_received.push_back(chp->node());
                    checkpointEntry->quorum_nodes.push_back(chp->node());
                }
                if (checkpointEntry->data_state.is_null() && !(checkpointEntry->is_stable && maxStableSeqNum <= lastStableCheckpointSeqNum_.load())) {
                    logger_.log(logLevel::INFO, "[Func: computePrePrepareSet] ACTION - Requesting data state sync from node " + stableNode + " for seq " + to_string(maxStableSeqNum));
                    auto [valid, data_state] = requestDataStateAndValidate(stableNode, stableDataDigest, maxStableSeqNum);
                    checkpointEntry->data_state = data_state;
                    stable_data_state = data_state;
                    checkpointEntry->is_stable = true;
                    is_sync_required = true;
                    logger_.log(logLevel::DEBUG, string("[Func: computePrePrepareSet] STATE - State sync ") + (valid ? "valid" : "invalid"));
                }
            }

            if (is_sync_required) {
                {
                    lock_guard<mutex> lk1(executionInProgressMx_);
                    lastExecutedSeqNum_.store(maxStableSeqNum);
                }
                {
                    lock_guard<mutex> lg(dataStateFileMx_);
                    writeJsonFile(dataStateFile_, stable_data_state);
                }
                lastStableCheckpointSeqNum_.store(maxStableSeqNum);
                lowWaterMark_.store(maxStableSeqNum);
                highWaterMark_.store(maxStableSeqNum + validSequenceRange_.load());
                logger_.log(logLevel::INFO, "[Func: computePrePrepareSet] STATE - Stable checkpoint advanced; watermarks updated [" + to_string(lowWaterMark_.load()) + ", " + to_string(highWaterMark_.load()) + ")");
            }
        }

        auto* prePrepareMap = nvc.mutable_pre_prepare_set();
        int seqNum = maxStableSeqNum + 1;
        while (seqNum <= maxPreparedSeqNum) {
            bool found = false;
            PrePrepareViewChangeSet pre_prepare_set;
            PreparedViewChangeEvidence valid_evidence;
            int lastViewNo = 0;
            string digest;
            string takenFromNode;
            for (const auto& vce : nvc.view_change_evidence()) {
                auto it = vce.view_change_content().prepared_evidence().find(seqNum);
                if (it != vce.view_change_content().prepared_evidence().end()) {
                    const auto& evidence = it->second;
                    if (digest != evidence.collected_prepare().prepare_content().message_digest() 
                        && evidence.collected_prepare().prepare_content().view_number()>lastViewNo) {
                        if (validateCollectedPrepare(evidence.collected_prepare())) {
                            found = true;
                            valid_evidence = evidence;
                            digest = evidence.collected_prepare().prepare_content().message_digest();
                            lastViewNo = evidence.collected_prepare().prepare_content().view_number();
                            takenFromNode = evidence.collected_prepare().node();
                        }
                    }
                    else if (digest == evidence.collected_prepare().prepare_content().message_digest() 
                        && evidence.collected_prepare().prepare_content().view_number()>lastViewNo) {
                        lastViewNo = evidence.collected_prepare().prepare_content().view_number();
                    }
                }
            }
            bool is_fast_path = false;
            if (found) {
                auto* prePrepare = pre_prepare_set.mutable_pre_prepare();
                prePrepare->set_node(id_);
                prePrepare->mutable_pre_prepare_content()->CopyFrom(valid_evidence.collected_prepare().prepare_content());
                prePrepare->mutable_pre_prepare_content()->set_view_number(viewNo);
                prePrepare->mutable_pre_prepare_content()->set_message_type(PRE_PREPARE);
                prePrepare->set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(prePrepare->pre_prepare_content())));
                pre_prepare_set.mutable_prepared_evidence()->CopyFrom(valid_evidence);
                is_fast_path = valid_evidence.is_fast_path();
                logger_.log(logLevel::DEBUG, "[Func: computePrePrepareSet] STATE - seq " + to_string(seqNum) + " uses prepared evidence (fast=" + string(is_fast_path ? "true" : "false") + ")");
            }
            else {
                auto* prePrepare = pre_prepare_set.mutable_pre_prepare();
                prePrepare->set_node(id_);
                prePrepare->mutable_pre_prepare_content()->set_view_number(viewNo);
                prePrepare->mutable_pre_prepare_content()->set_message_type(PRE_PREPARE);
                prePrepare->mutable_pre_prepare_content()->set_sequence_number(seqNum);
                prePrepare->mutable_pre_prepare_content()->set_message_digest("NULL");
                prePrepare->set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(prePrepare->pre_prepare_content())));
                pre_prepare_set.mutable_prepared_evidence()->set_is_fast_path(false);
                is_fast_path = false;
                logger_.log(logLevel::DEBUG, "[Func: computePrePrepareSet] STATE - seq " + to_string(seqNum) + " uses NO OP (fast=" + string(is_fast_path ? "true" : "false") + ")");
            }
            (*prePrepareMap)[seqNum] = pre_prepare_set;
            shared_ptr<PBFTLogEntry> logEntry;
            {
                lock_guard<mutex> lk(sequenceLogMapMx_);
                auto it = sequenceLogMap_.find(seqNum);
                if (it!=sequenceLogMap_.end()) {
                    logEntry = sequenceLogMap_[seqNum];
                }
                else {
                    logEntry = make_shared<PBFTLogEntry>();
                    logEntry->sequence_number = seqNum;
                    sequenceLogMap_[seqNum] = logEntry;
                }
            }
            bool updateCollectedPrepare = false;
            {
                lock_guard<mutex> lk(logEntry->mtx);
                logEntry->view_number = viewNo;
                if (!logEntry->is_prepared && logEntry->request_digest!=pre_prepare_set.pre_prepare().pre_prepare_content().message_digest()) {
                    logEntry->request_digest = pre_prepare_set.pre_prepare().pre_prepare_content().message_digest();
                    if (logEntry->request_digest!="NULL") {
                        threadPool_.enqueue([this, sequence_number = seqNum]() {
                            requestClientMessage(sequence_number);
                        });
                    }
                    else logEntry->client_request = nullptr;
                }
                if (is_fast_path && !logEntry->is_fast_path && !logEntry->is_executed) {
                    logEntry->is_prepared = true;
                    logEntry->is_ready_for_execution = true;
                    logEntry->is_fast_path = true;
                    updateCollectedPrepare = true;
                }
            }
            {
                lock_guard<mutex> lk(logEntry->pre_prepare_data.pre_prepare_mtx);
                logEntry->pre_prepare_data.pre_prepare = make_shared<PrePrepare>(pre_prepare_set.pre_prepare());
            }
            Prepare prepare;
            prepare.mutable_prepare_content()->CopyFrom(pre_prepare_set.pre_prepare().pre_prepare_content());
            prepare.mutable_prepare_content()->set_message_type(PREPARE);
            prepare.set_node(id_);
            prepare.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(prepare.prepare_content())));
            prepare.set_fast_path_signature(bls_utils::sign_data(fastPrivateKey_, serializeDeterministic(prepare.prepare_content())));
            {
                lock_guard<mutex> lg(logEntry->prepare_data.prepare_mtx);
                logEntry->prepare_data.local_prepare = make_shared<Prepare>(prepare);
                logEntry->prepare_data.valid_prepares_received.clear();
                logEntry->prepare_data.valid_prepares_received.push_back(logEntry->prepare_data.local_prepare);
                logEntry->prepare_data.valid_prepare_count = 0;
                logEntry->prepare_data.valid_prepare_count++;
                logEntry->prepare_data.nodes_received.clear();
                logEntry->prepare_data.nodes_received.push_back(id_);
                logEntry->prepare_data.agg_signatures.clear();
                logEntry->prepare_data.agg_signatures.push_back(prepare.signature());
                logEntry->prepare_data.fast_path_agg_signatures.clear();
                logEntry->prepare_data.fast_path_agg_signatures.push_back(prepare.fast_path_signature());
                logEntry->prepare_data.has_quorum = false;
                logEntry->prepare_data.has_total_consensus = false;
                if (updateCollectedPrepare) {
                    logEntry->prepare_data.collected_prepare = make_shared<CollectedPrepare>(pre_prepare_set.prepared_evidence().collected_prepare());
                }
            }

            {
                lock_guard<mutex> lg(logEntry->commit_data.commit_mtx);
                logEntry->commit_data.valid_commits_received.clear();
                logEntry->commit_data.valid_commit_count = 0;
                logEntry->commit_data.nodes_received.clear();
                logEntry->commit_data.agg_signatures.clear();
                logEntry->commit_data.has_quorum = false;
            }

            {
                bool proceed = false;
                string client;
                long long timestamp;
                {
                    lock_guard<mutex> lk(logEntry->mtx);
                    if (logEntry->request_digest!="NULL") {
                        proceed = true;
                        client = logEntry->client_request->client();
                        timestamp = logEntry->client_request->request_content().timestamp();
                    }
                }
                
                if (proceed) {
                    lock_guard<mutex> lk(clientsMx_);
                    auto it = clients_.find(client);
                    if (it != clients_.end()) {
                        auto* clientMetadata = &it->second;
                        logger_.log(logLevel::DEBUG, "[Func: computePrePrepareSet] ACTION - Updating client's lastConsideredTransactionTimestamp.");
                        {
                            if (timestamp > clientMetadata->lastConsideredTransactionTimestamp) {
                                clientMetadata->lastConsideredTransactionTimestamp = timestamp;
                                logger_.log(logLevel::DEBUG, "[Func: computePrePrepareSet] STATE - Updated timestamp for client " + client);
                            }
                        }
                    }
                }
            }
                


            seqNum++;
        }

        if (maxPreparedSeqNum > 0) sequenceNumber_.store(maxPreparedSeqNum);
        else sequenceNumber_.store(lastStableCheckpointSeqNum_.load());
        logger_.log(logLevel::INFO, "[Func: computePrePrepareSet] EXIT - PrePrepare set completed; sequenceNumber_ now " + to_string(sequenceNumber_.load()));
        return;
    }

    bool computeAndVerifyPrePrepareSet(const NewViewContent& nvc, int viewNo) {
        logger_.log(logLevel::INFO, "[Func: computeAndVerifyPrePrepareSet] ENTER - Verifying PrePrepare set for view " + to_string(viewNo));
        int minSeqReceived = 0, maxSeqReceived = 0;
        if (!nvc.pre_prepare_set().empty()) {
            auto [min_it, max_it] = minmax_element(
                nvc.pre_prepare_set().begin(), nvc.pre_prepare_set().end(),
                [](const auto& a, const auto& b){ return a.first < b.first; });
            minSeqReceived = min_it->first;
            maxSeqReceived = max_it->first;
        }
        logger_.log(logLevel::DEBUG, "[Func: computeAndVerifyPrePrepareSet] STATE - Provided range: [" + to_string(minSeqReceived) + ", " + to_string(maxSeqReceived) + "]");

        int maxStableSeqNum = 0;
        int maxPreparedSeqNum = 0;
        vector<shared_ptr<Checkpoint>> stableCheckpointEvidence;
        string stableDataDigest;
        string stableNode;
        for (const auto& vce : nvc.view_change_evidence()) {
            if (vce.view_change_content().last_stable_sequence_number()>maxStableSeqNum) {
                auto [result, evidence, data_digest] = validateCheckpointEvidence(vce.view_change_content());
                if (result) {
                    maxStableSeqNum = vce.view_change_content().last_stable_sequence_number();
                    stableCheckpointEvidence = evidence;
                    stableDataDigest = data_digest;
                    stableNode = vce.node();
                }
                else {
                    logger_.log(logLevel::ERROR, "[Func: computeAndVerifyPrePrepareSet] ERROR - Checkpoint evidence invalid for node " + vce.node());
                    return false;
                }
            }
            int max_seq_num_for_vce = 0;
            const auto& prep = vce.view_change_content().prepared_evidence();
            if (!prep.empty()) {
                for (const auto& kv : prep) max_seq_num_for_vce = max(max_seq_num_for_vce, kv.first);
            }
            maxPreparedSeqNum = max(maxPreparedSeqNum, max_seq_num_for_vce);
        }
        logger_.log(logLevel::DEBUG, "[Func: computeAndVerifyPrePrepareSet] STATE - maxStableSeqNum=" + to_string(maxStableSeqNum) + ", maxPreparedSeqNum=" + to_string(maxPreparedSeqNum));

        if (minSeqReceived==0 && maxSeqReceived==0) {
            if (maxStableSeqNum<maxPreparedSeqNum) {
                logger_.log(logLevel::ERROR, "[Func: computeAndVerifyPrePrepareSet] ERROR - Provided pre-prepare set empty but prepared extends past stable.");
                return false;
            }
        }
        else if (minSeqReceived!=maxStableSeqNum+1 || maxSeqReceived!=maxPreparedSeqNum) {
            logger_.log(logLevel::ERROR, "[Func: computeAndVerifyPrePrepareSet] ERROR - Provided range does not match expected [" + to_string(maxStableSeqNum+1) + ", " + to_string(maxPreparedSeqNum) + "]");
            return false;
        }

        if (minSeqReceived!=0) {
            int seqNum = minSeqReceived;
            while (seqNum <= maxSeqReceived) {
                bool found = false;
                PrePrepareViewChangeSet pre_prepare_set;
                PreparedViewChangeEvidence valid_evidence;
                int lastViewNo = 0;
                string digest;
                string takenFromNode;
                for (const auto& vce : nvc.view_change_evidence()) {
                    auto it = vce.view_change_content().prepared_evidence().find(seqNum);
                    if (it != vce.view_change_content().prepared_evidence().end()) {
                        const auto& evidence = it->second;
                        if (digest != evidence.collected_prepare().prepare_content().message_digest() 
                            && evidence.collected_prepare().prepare_content().view_number()>lastViewNo) {
                            if (validateCollectedPrepare(evidence.collected_prepare())) {
                                found = true;
                                valid_evidence = evidence;
                                digest = evidence.collected_prepare().prepare_content().message_digest();
                                lastViewNo = evidence.collected_prepare().prepare_content().view_number();
                                takenFromNode = evidence.collected_prepare().node();
                            } else {
                                logger_.log(logLevel::WARN, "[Func: computeAndVerifyPrePrepareSet] BRANCH - Invalid collected prepare for seq " + to_string(seqNum));
                            }
                        }
                        else if (digest == evidence.collected_prepare().prepare_content().message_digest() 
                            && evidence.collected_prepare().prepare_content().view_number()>lastViewNo) {
                            lastViewNo = evidence.collected_prepare().prepare_content().view_number();
                        }
                    }
                }
                bool is_fast_path = false;
                if (found) {
                    auto* prePrepare = pre_prepare_set.mutable_pre_prepare();
                    prePrepare->set_node(id_);
                    prePrepare->mutable_pre_prepare_content()->CopyFrom(valid_evidence.collected_prepare().prepare_content());
                    prePrepare->mutable_pre_prepare_content()->set_view_number(viewNo);
                    prePrepare->mutable_pre_prepare_content()->set_message_type(PRE_PREPARE);
                    prePrepare->set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(prePrepare->pre_prepare_content())));
                    pre_prepare_set.mutable_prepared_evidence()->CopyFrom(valid_evidence);
                    is_fast_path = valid_evidence.is_fast_path();
                }
                else {
                    auto* prePrepare = pre_prepare_set.mutable_pre_prepare();
                    prePrepare->set_node(id_);
                    prePrepare->mutable_pre_prepare_content()->set_view_number(viewNo);
                    prePrepare->mutable_pre_prepare_content()->set_message_type(PRE_PREPARE);
                    prePrepare->mutable_pre_prepare_content()->set_sequence_number(seqNum);
                    prePrepare->mutable_pre_prepare_content()->set_message_digest("NULL");
                    prePrepare->set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(prePrepare->pre_prepare_content())));
                    pre_prepare_set.mutable_prepared_evidence()->set_is_fast_path(false);
                    is_fast_path = false;
                }

                auto it = nvc.pre_prepare_set().find(seqNum);
                if (it!=nvc.pre_prepare_set().end()) {
                    const auto& provided = it->second;
                    if (provided.pre_prepare().pre_prepare_content().DebugString()!=pre_prepare_set.pre_prepare().pre_prepare_content().DebugString()) {
                        logger_.log(logLevel::ERROR, "[Func: computeAndVerifyPrePrepareSet] ERROR - Mismatch in supplied PrePrepare content for seq " + to_string(seqNum));
                        return false;
                    }
                }
                else {
                    logger_.log(logLevel::ERROR, "[Func: computeAndVerifyPrePrepareSet] ERROR - Missing PrePrepare for expected seq " + to_string(seqNum));
                    return false;
                }

                shared_ptr<PBFTLogEntry> logEntry = nullptr;
                {
                    lock_guard<mutex> lk(sequenceLogMapMx_);
                    auto it2 = sequenceLogMap_.find(seqNum);
                    if (it2!=sequenceLogMap_.end()) {
                        logEntry = it2->second;
                    }
                }
                if (logEntry!=nullptr && logEntry->is_prepared && logEntry->request_digest!=pre_prepare_set.pre_prepare().pre_prepare_content().message_digest()) {
                    logger_.log(logLevel::ERROR, "[Func: computeAndVerifyPrePrepareSet] ERROR - Local prepared digest conflicts with provided for seq " + to_string(seqNum));
                    return false;
                }
                seqNum++;
            }
        }
        
        if (lastStableCheckpointSeqNum_.load()<maxStableSeqNum) {
            logger_.log(logLevel::INFO, "[Func: computeAndVerifyPrePrepareSet] ACTION - Reconciling stable checkpoint to seq " + to_string(maxStableSeqNum));
            shared_ptr<MainNode::CheckpointData> checkpointEntry;
            bool new_entry = false;
            {
                lock_guard<mutex> lg(checkpointLogMx_);
                auto cit = checkpointLog_.find(maxStableSeqNum);
                if (cit != checkpointLog_.end()) {
                    checkpointEntry = cit->second;
                }
                else {
                    checkpointEntry = make_shared<MainNode::CheckpointData>();
                    checkpointLog_[maxStableSeqNum] = checkpointEntry;
                    new_entry = true;
                }
            }
            json stable_data_state;
            bool is_sync_required = false;
            {
                lock_guard<mutex> lk(checkpointEntry->checkpoint_mtx);
                checkpointEntry->sequence_number = maxStableSeqNum;
                if (checkpointEntry->data_digest!=stableDataDigest) {
                    logger_.log(logLevel::ERROR, "[Func: computeAndVerifyPrePrepareSet] ERROR - Stable digest mismatch during reconciliation.");
                    return false;
                }
                checkpointEntry->quorum_checkpoints = stableCheckpointEvidence;
                checkpointEntry->valid_checkpoint_count = stableCheckpointEvidence.size();
                checkpointEntry->valid_checkpoints_received = stableCheckpointEvidence;
                for (const auto& chp : stableCheckpointEvidence) {
                    checkpointEntry->nodes_received.push_back(chp->node());
                    checkpointEntry->quorum_nodes.push_back(chp->node());
                }
                if (checkpointEntry->data_state.is_null() && !(checkpointEntry->is_stable && maxStableSeqNum <= lastStableCheckpointSeqNum_.load())) {
                    is_sync_required = true;
                }
            }

            if (is_sync_required) {
                logger_.log(logLevel::INFO, "[Func: computeAndVerifyPrePrepareSet] ACTION - Fetching data state from node " + stableNode + " for seq " + to_string(maxStableSeqNum));
                auto [valid, data_state] = requestDataStateAndValidate(stableNode, stableDataDigest, maxStableSeqNum);
                {
                    lock_guard<mutex> lk(checkpointEntry->checkpoint_mtx);
                    if (checkpointEntry->data_state.is_null() && !(checkpointEntry->is_stable && maxStableSeqNum <= lastStableCheckpointSeqNum_.load())) {
                        checkpointEntry->data_state = data_state;
                        stable_data_state = data_state;
                        checkpointEntry->is_stable = true;
                    }
                }
                bool valid_to_write=false;
                {
                    lock_guard<mutex> lk1(executionInProgressMx_);
                    if (lastExecutedSeqNum_.load()<maxStableSeqNum) {
                        valid_to_write = true;
                        lastExecutedSeqNum_.store(maxStableSeqNum);
                    }
                }
                if (valid_to_write)
                {
                    {
                        lock_guard<mutex> lg(dataStateFileMx_);
                        writeJsonFile(dataStateFile_, stable_data_state);
                    }
                    lastStableCheckpointSeqNum_.store(maxStableSeqNum);
                    lowWaterMark_.store(maxStableSeqNum);
                    highWaterMark_.store(maxStableSeqNum + validSequenceRange_.load());
                    logger_.log(logLevel::INFO, "[Func: computeAndVerifyPrePrepareSet] STATE - Advanced stable checkpoint and updated watermarks.");
                }
            }
        }
        if (maxPreparedSeqNum > 0) sequenceNumber_.store(maxPreparedSeqNum);
        else sequenceNumber_.store(lastStableCheckpointSeqNum_.load());
        logger_.log(logLevel::INFO, "[Func: computeAndVerifyPrePrepareSet] EXIT - Verification succeeded; sequenceNumber_ now " + to_string(sequenceNumber_.load()));
        return true;
    }

    void createNewView(const int& viewNo) {
        logger_.log(logLevel::INFO, "[Func: createNewView] ENTER - Creating NEW_VIEW for view " + to_string(viewNo));
        NewView newView;
        newView.set_node(id_);
        auto* nvc = newView.mutable_new_view_content();
        nvc->set_message_type(NEW_VIEW);
        nvc->set_view_number(viewNo);
        shared_ptr<ViewChangeData> viewChangeEntry;
        {
            lock_guard<mutex> lk(viewChangeLogMx_);
            logger_.log(logLevel::DEBUG, "[Func: createNewView] ACTION - Searching viewChangeLog_ for view " + to_string(viewNo));
            auto it = viewChangeLog_.find(viewNo);
            if (it != viewChangeLog_.end()) {
                viewChangeEntry = it->second;
            }
            else {
                logger_.log(logLevel::ERROR, "[Func: createNewView] ERROR - ViewChange entry missing for view " + to_string(viewNo));
                cerr << "Checkpoint entry missing!\n";
                return;
            }
        }
        {
            lock_guard<mutex> lk(viewChangeEntry->view_change_mtx);
            logger_.log(logLevel::DEBUG, "[Func: createNewView] ACTION - Copying quorum view-change evidence.");
            for (const auto& viewChangeMessage : viewChangeEntry->quorum_view_changes) {
                auto* v = nvc->add_view_change_evidence();
                v->CopyFrom(*viewChangeMessage);
            }
        }
        logger_.log(logLevel::INFO, "[Func: createNewView] ACTION - Computing PrePrepare set.");
        computePrePrepareSet(*nvc, viewNo);
        newView.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(*nvc)));
        logger_.log(logLevel::DEBUG, "[Func: createNewView] STATE - NEW_VIEW signed.");
        {
            lock_guard<mutex> l(newViewMessageListMx_);
            newViewMessageList_.push_back(newView.DebugString());
        }
        broadcastNewView(newView);
        logger_.log(logLevel::INFO, "[Func: createNewView] ACTION - Broadcasted NEW_VIEW.");
        isViewStable_.store(true);
        latestViewChangeCount_.store(0);
        leastViewNumViewChangeReceived_.store(0);
        consecutiveViewChangeCount_.store(0);
        int gracePeriod = timeoutMs_.load()*0.2;
        {
            lock_guard<mutex> lg(pendingRequestMx_);
            if (pendingRequest_.size()>0) {
                sleep_for(milliseconds(gracePeriod));
                while (!pendingRequest_.empty()) {
                    processPendingRequest(pendingRequest_.front());
                    pendingRequest_.pop_front();
                }
            }
        }
        if (sequenceNumber_.load()>lastStableCheckpointSeqNum_.load()) {
            if (!timer_.is_running()) {
                logger_.log(logLevel::DEBUG, "[Func: createNewView] BRANCH: Timer not running -> starting timeout timer.");
                timer_.start(milliseconds(timeoutMs_.load()), [this]() {
                    isViewStable_.store(false);
                    initiateViewChange();
                });
            }
        }
        logger_.log(logLevel::INFO, "[Func: createNewView] EXIT - View stabilized for view " + to_string(viewNo));
    }

    void processPendingRequest(shared_ptr<Request> request) {
        if (request->request_content().operation().type() == READ_ONLY) {
            logger_.log(logLevel::DEBUG, "[Func: processPendingRequest] BRANCH: Request is READ_ONLY");
            {
                lock_guard<mutex> lk(clientsMx_);
                auto it = clients_.find(request->client());
                if (it != clients_.end()) {
                    auto* clientMetadata = &it->second;
                    logger_.log(logLevel::DEBUG, "[Func: processPendingRequest] STATE - Updating lastConsideredTransactionTimestamp for READ_ONLY request.");
                    {
                        if (request->request_content().timestamp() > clientMetadata->lastConsideredTransactionTimestamp) {
                            clientMetadata->lastConsideredTransactionTimestamp = request->request_content().timestamp();
                        }
                        else return;
                    }
                }
            }
            logger_.log(logLevel::INFO, "[Func: processPendingRequest] ACTION - Enqueuing processReadOnlyRequest to threadPool_");
            threadPool_.enqueue([this, requestCopy = *request]() {
                processReadOnlyRequest(requestCopy);
            });
        }
        else {
            logger_.log(logLevel::DEBUG, "[Func: processPendingRequest] BRANCH: Request is READ_WRITE (or promoted).");
            {
                lock_guard<mutex> lk(clientsMx_);
                auto it = clients_.find(request->client());
                if (it != clients_.end()) {
                    auto* clientMetadata = &it->second;
                    logger_.log(logLevel::DEBUG, "[Func: processPendingRequest] STATE - Updating lastConsideredTransactionTimestamp for READ_WRITE request.");
                    {
                        if (request->request_content().timestamp() > clientMetadata->lastConsideredTransactionTimestamp) {
                            clientMetadata->lastConsideredTransactionTimestamp = request->request_content().timestamp();
                        }
                        else return;
                    }
                }
            }   
            logger_.log(logLevel::INFO, "[Func: processPendingRequest] ACTION - Enqueuing handleRequest() in threadPool_ for client " + request->client());
            threadPool_.enqueue([this, requestCopy = *request]() {
                handleRequest(requestCopy);
            });
        }
    }

    ViewChange createViewChange() {
        logger_.log(logLevel::INFO, "[Func: createViewChange] ENTER - Creating VIEW_CHANGE for currentView=" + to_string(currentView_.load()));
        ViewChange vc;
        vc.set_node(id_);
        auto* vc_content = vc.mutable_view_change_content();
        vc_content->set_message_type(VIEW_CHANGE);
        vc_content->set_view_number(currentView_.load());
        if (lastStableCheckpointSeqNum_.load()>0) {
            vc_content->set_last_stable_sequence_number(lastStableCheckpointSeqNum_.load());
            shared_ptr<CheckpointData> checkpointEntry;
            {
                lock_guard<mutex> lk(checkpointLogMx_);
                logger_.log(logLevel::DEBUG, "[Func: createViewChange] ACTION - Looking up checkpoint at seq " + to_string(lastStableCheckpointSeqNum_.load()));
                auto it = checkpointLog_.find(lastStableCheckpointSeqNum_.load());
                if (it != checkpointLog_.end()) {
                    checkpointEntry = it->second;
                }
                else logger_.log(logLevel::WARN, "[Func: createViewChange] BRANCH - Checkpoint entry missing for seq " + to_string(lastStableCheckpointSeqNum_.load()));
            }
            if (checkpointEntry)
            {
                lock_guard<mutex> lk(checkpointEntry->checkpoint_mtx);
                vc_content->set_data_digest(checkpointEntry->data_digest);
                logger_.log(logLevel::DEBUG, "[Func: createViewChange] STATE - Added data_digest=" + checkpointEntry->data_digest);
                for (const auto& chkPtr : checkpointEntry->quorum_checkpoints) {
                    if (chkPtr) {
                        *vc_content->add_checkpoint_evidence() = *chkPtr;
                    }
                }
                logger_.log(logLevel::DEBUG, "[Func: createViewChange] STATE - Added " + to_string(vc_content->checkpoint_evidence_size()) + " checkpoint evidence messages.");
            }
        }
        if (sequenceNumber_.load()>0) {
            int seqNum = lastStableCheckpointSeqNum_.load() + 1;
            int maxSeqNum = sequenceNumber_.load();
            logger_.log(logLevel::DEBUG, "[Func: createViewChange] STATE - Scanning prepared range [" + to_string(seqNum) + ", " + to_string(maxSeqNum) + "]");
            auto* preparedMap = vc_content->mutable_prepared_evidence();
            while (seqNum <= maxSeqNum) {
                shared_ptr<PBFTLogEntry> logEntry;
                {
                    lock_guard<mutex> lk(sequenceLogMapMx_);
                    auto it = sequenceLogMap_.find(seqNum);
                    if (it != sequenceLogMap_.end()) {
                        logEntry = it->second;
                    }
                }
                if (!logEntry) { ++seqNum; continue; }
                bool prepared = false;
                bool is_fast_path;
                {
                    lock_guard<mutex> lk(logEntry->mtx);
                    prepared = logEntry->is_prepared;
                    is_fast_path = logEntry->is_fast_path;
                }

                if (prepared) {
                    PreparedViewChangeEvidence ev;
                    ev.set_is_fast_path(is_fast_path);
                    {
                        lock_guard<mutex> lk(logEntry->prepare_data.prepare_mtx);
                        ev.mutable_collected_prepare()->CopyFrom(*logEntry->prepare_data.collected_prepare);
                    }
                    (*preparedMap)[seqNum] = ev;
                    logger_.log(logLevel::DEBUG, "[Func: createViewChange] STATE - Added prepared evidence for seq " + to_string(seqNum) + " (fast=" + string(is_fast_path ? "true" : "false") + ")");
                }
                seqNum++;
            }
        }
        vc.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(*vc_content)));
        logger_.log(logLevel::INFO, "[Func: createViewChange] Private Key used : "+privateKey_+" || Signature: "+vc.signature()+" || Content Signed On: "+vc_content->SerializeAsString());
        logger_.log(logLevel::INFO, "[Func: createViewChange] EXIT - VIEW_CHANGE constructed and signed.");
        return vc;
    }

    void initiateViewChange(int viewNo = 0)
    {
        logger_.log(logLevel::INFO, "[Func: initiateViewChange] ENTER - Initiating view change. Input viewNo=" + to_string(viewNo));
        if (timer_.is_running()) timer_.stop();
        bool expected = true;
        isViewStable_.compare_exchange_strong(expected, false);
        latestViewChangeCount_.store(0);
        leastViewNumViewChangeReceived_.store(0);
        if (viewNo==0) viewNo = currentView_.fetch_add(1) + 1;
        else currentView_.store(viewNo);
        logger_.log(logLevel::DEBUG, "[Func: initiateViewChange] STATE - New currentView_=" + to_string(currentView_.load()));
        setLeader();
        ViewChange viewChange = createViewChange();
        shared_ptr<ViewChangeData> viewChangeEntry;
        bool newEntry = false;
        {
            lock_guard<mutex> lk(viewChangeLogMx_);
            auto it = viewChangeLog_.find(viewNo);
            if (it != viewChangeLog_.end()) {
                viewChangeEntry = it->second;
            }
            else {
                viewChangeEntry = make_shared<ViewChangeData>();
                viewChangeLog_[viewNo] = viewChangeEntry;
                newEntry = true;
            }
        }
        {
            lock_guard<mutex> lk(viewChangeEntry->view_change_mtx);
            if (newEntry) viewChangeEntry->view_number = viewNo;
            viewChangeEntry->local_view_change = make_shared<ViewChange>(viewChange);
            viewChangeEntry->valid_view_changes_received.push_back(viewChangeEntry->local_view_change);
            viewChangeEntry->nodes_received.push_back(id_);
            viewChangeEntry->valid_view_change_count++;
            logger_.log(logLevel::DEBUG, "[Func: initiateViewChange] STATE - Stored local VIEW_CHANGE for view " + to_string(viewNo));
        }
        broadcastViewChange(viewChange);
        logger_.log(logLevel::INFO, "[Func: initiateViewChange] EXIT - VIEW_CHANGE broadcast for view " + to_string(viewNo));
    }

    void prePrepareSequence(const Request &request, const int& seqNum, const unordered_set<string>& nodeSet) {
        logger_.log(logLevel::INFO, "[Func: prePrepareSequence] ENTER - Handling client request from " + request.client());
        auto logEntry = make_shared<PBFTLogEntry>();
        logEntry->sequence_number = seqNum;
        logEntry->view_number = currentView_.load();
        logEntry->client_request = make_shared<Request>(request);
        logEntry->request_digest = computeDigest(serializeDeterministic(request));
        logger_.log(logLevel::DEBUG, "[Func: prePrepareSequence] STATE - Allocated seqNum=" + to_string(seqNum) + ", digest=" + logEntry->request_digest);
        PrePrepare prePrepare;
        prePrepare.set_node(id_);
        *prePrepare.mutable_message() = *logEntry->client_request;
        auto* prePrepareContent = prePrepare.mutable_pre_prepare_content();
        prePrepareContent->set_view_number(logEntry->view_number);
        prePrepareContent->set_sequence_number(logEntry->sequence_number);
        prePrepareContent->set_message_digest(logEntry->request_digest);
        prePrepareContent->set_message_type(PRE_PREPARE);
        prePrepare.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(*prePrepareContent)));
        {
            lock_guard<mutex> lg(logEntry->pre_prepare_data.pre_prepare_mtx);
            logEntry->pre_prepare_data.pre_prepare = make_shared<PrePrepare>(prePrepare);
        }
        {
            lock_guard<mutex> lg(sequenceLogMapMx_);
            sequenceLogMap_[seqNum] = logEntry;
        }
        Prepare prepare;
        prepare.mutable_prepare_content()->CopyFrom(prePrepare.pre_prepare_content());
        prepare.mutable_prepare_content()->set_message_type(PREPARE);
        prepare.set_node(id_);
        prepare.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(prepare.prepare_content())));
        prepare.set_fast_path_signature(bls_utils::sign_data(fastPrivateKey_, serializeDeterministic(prepare.prepare_content())));
        {
            lock_guard<mutex> lg(logEntry->prepare_data.prepare_mtx);
            logEntry->prepare_data.local_prepare = make_shared<Prepare>(prepare);
            logEntry->prepare_data.valid_prepares_received.push_back(logEntry->prepare_data.local_prepare);
            logEntry->prepare_data.valid_prepare_count++;
            logEntry->prepare_data.nodes_received.push_back(id_);
            logEntry->prepare_data.agg_signatures.push_back(prepare.signature());
            logEntry->prepare_data.fast_path_agg_signatures.push_back(prepare.fast_path_signature());
        }
        logger_.log(logLevel::INFO, "[Func: prePrepareSequence] STATE - Signature used in prepare message " + prepare.signature());
        logger_.log(logLevel::INFO, "[Func: prePrepareSequence] STATE - Content signed in prepare message " + prepare.prepare_content().SerializeAsString());
        logger_.log(logLevel::INFO, "[Func: prePrepareSequence] STATE - Keys used to sign in prepare message " + privateKey_ + " | " + fastPrivateKey_);
        
        logger_.log(logLevel::INFO, "[Func: prePrepareSequence] ACTION - Broadcasting PRE_PREPARE for seq " + to_string(seqNum));
        broadcastPrePrepare(prePrepare, nodeSet);
        logger_.log(logLevel::INFO, "[Func: prePrepareSequence] EXIT - PRE_PREPARE sent for seq " + to_string(seqNum));
    }

    void handleRequest(const Request &request)
    {
        logger_.log(logLevel::INFO, "[Func: handleRequest] ENTER - Handling client request from " + request.client());
        int seqNum;
        if (isLeader_.load() && byzantineStatus_.load() && equivocationAttackActivated_) {
            logger_.log(logLevel::INFO, "[Func: handleRequest] BRANCH - Handling client request with Equivocation attack.");
            {
                lock_guard<mutex> lk(sequenceMx_);
                seqNum = sequenceNumber_.fetch_add(1) + 1;
                threadPool_.enqueue([this, request_ = request, seqNum_ = seqNum, nodeSet_ = firstSet_]() {
                    prePrepareSequence(request_, seqNum_, nodeSet_);
                });
                logger_.log(logLevel::INFO, "[Func: handleRequest] STATE - Sent sequence number "+to_string(seqNum)+" to first set.");
                seqNum = sequenceNumber_.fetch_add(1) + 1;
                threadPool_.enqueue([this, request_ = request, seqNum_ = seqNum, nodeSet_ = secondSet_]() {
                    prePrepareSequence(request_, seqNum_, nodeSet_);
                });
                logger_.log(logLevel::INFO, "[Func: handleRequest] STATE - Sent sequence number "+to_string(seqNum)+" to second set.");
            }
        }
        else {
            seqNum = sequenceNumber_.fetch_add(1) + 1;
            threadPool_.enqueue([this, request_ = request, seqNum_ = seqNum, nodeSet_ = completeSet_]() {
                prePrepareSequence(request_, seqNum_, nodeSet_);
            });
            logger_.log(logLevel::INFO, "[Func: handleRequest] STATE - Sent sequence number "+to_string(seqNum)+" to complete set.");
        }
        logger_.log(logLevel::INFO, "[Func: handleRequest] EXIT - PRE_PREPARE sent for seq " + to_string(seqNum));
    }

    void broadcastPrePrepare(const PrePrepare &prePrepare, const unordered_set<string>& nodeSet)
    {
        logger_.log(logLevel::INFO, "[Func: broadcastPrePrepare] ENTER - Broadcasting PRE_PREPARE seq=" + to_string(prePrepare.pre_prepare_content().sequence_number()));
        {
            lock_guard<mutex> l(logMx_);
            log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: broadcastPrePrepare] - "+prePrepare.DebugString()));
        }
        if (isLeader_.load() && byzantineStatus_.load() && timeAttackActivated_) {
            int delay = timeoutMs_.load()/4;
            logger_.log(logLevel::INFO, "[Func: broadcastPrePrepare] ENTER - Delaying broadcasting PRE_PREPARE for " + to_string(delay));
            sleep_for(milliseconds(delay));
        }
        for (auto &node : nodeSet)
        {
            if (node == id_ || (byzantineStatus_.load() && darkAttackActivated_ && nodes_[node].byzantineVictim)) {
                logger_.log(logLevel::INFO, "[Func: broadcastPrePrepare] Skipping sending PrePrepare to node "+node);
                continue;
            }
            Empty reply;
            auto* call = new AsyncNodeCall;
            call->response_reader = nodes_[node].nodeStub->AsyncSendPrePrepare(&call->context, prePrepare, &cq_);
            call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        }
        logger_.log(logLevel::INFO, "[Func: broadcastPrePrepare] EXIT - PRE_PREPARE dispatched to all peers.");
    }

    void broadcastCollectedPrepare(const CollectedPrepare &collected_prepare)
    {
        logger_.log(logLevel::INFO, "[Func: broadcastCollectedPrepare] ENTER - Broadcasting CollectedPrepare seq=" + to_string(collected_prepare.prepare_content().sequence_number()));
        {
            lock_guard<mutex> l(logMx_);
            log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: broadcastCollectedPrepare] - "+collected_prepare.DebugString()));
        }
        if (isLeader_.load() && byzantineStatus_.load() && timeAttackActivated_) {
            int delay = timeoutMs_.load()/4;
            logger_.log(logLevel::INFO, "[Func: broadcastCollectedPrepare] ENTER - Delaying broadcasting CollectedPrepare for " + to_string(delay));
            sleep_for(milliseconds(delay));
        }
        for (auto &nodePair : nodes_)
        {
            if (nodePair.first == id_ || (byzantineStatus_.load() && darkAttackActivated_ && nodes_[nodePair.first].byzantineVictim)) {
                logger_.log(logLevel::INFO, "[Func: broadcastCollectedPrepare] Skipping sending CollectedPrepare to node "+nodePair.first);
                continue;
            }
            Empty reply;
            auto* call = new AsyncNodeCall;
            call->response_reader = nodePair.second.nodeStub->AsyncSendCollectedPrepare(&call->context, collected_prepare, &cq_);
            call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        }
        logger_.log(logLevel::INFO, "[Func: broadcastCollectedPrepare] EXIT - CollectedPrepare dispatched to all peers.");
    }

    void broadcastCollectedCommit(const CollectedCommit &collectedCommit)
    {
        logger_.log(logLevel::INFO, "[Func: broadcastCollectedCommit] ENTER - Broadcasting CollectedCommit seq=" + to_string(collectedCommit.commit_content().sequence_number()));
        {
            lock_guard<mutex> l(logMx_);
            log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: broadcastCollectedCommit] - "+collectedCommit.DebugString()));
        }
        if (isLeader_.load() && byzantineStatus_.load() && timeAttackActivated_) {
            int delay = timeoutMs_.load()/4;
            logger_.log(logLevel::INFO, "[Func: broadcastCollectedCommit] ENTER - Delaying broadcasting CollectedCommit for " + to_string(delay));
            sleep_for(milliseconds(delay));
        }
        for (auto &nodePair : nodes_)
        {
            if (nodePair.first == id_ || (byzantineStatus_.load() && darkAttackActivated_ && nodes_[nodePair.first].byzantineVictim)) {
                logger_.log(logLevel::INFO, "[Func: broadcastCollectedCommit] Skipping sending SendCollectedCommit to node "+nodePair.first);
                continue;
            }
            logger_.log(logLevel::DEBUG, "[Func: broadcastCollectedCommit] ACTION - Send to node " + nodePair.first);
            Empty reply;
            auto* call = new AsyncNodeCall;
            call->response_reader = nodePair.second.nodeStub->AsyncSendCollectedCommit(&call->context, collectedCommit, &cq_);
            call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        }
        logger_.log(logLevel::INFO, "[Func: broadcastCollectedCommit] EXIT - Dispatch complete.");
    }

    void broadcastCheckpoint(const Checkpoint &checkpoint)
    {
        logger_.log(logLevel::INFO, "[Func: broadcastCheckpoint] ENTER - Broadcasting CHECKPOINT seq=" + to_string(checkpoint.checkpoint_content().sequence_number()));
        {
            lock_guard<mutex> l(logMx_);
            log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: broadcastCheckpoint] - "+checkpoint.DebugString()));
        }
        if (isLeader_.load() && byzantineStatus_.load() && timeAttackActivated_) {
            int delay = timeoutMs_.load()/4;
            logger_.log(logLevel::INFO, "[Func: broadcastCheckpoint] ENTER - Delaying broadcasting CHECKPOINT for " + to_string(delay));
            sleep_for(milliseconds(delay));
        }
        for (auto &nodePair : nodes_)
        {
            if (nodePair.first == id_ || (byzantineStatus_.load() && darkAttackActivated_ && nodes_[nodePair.first].byzantineVictim)) {
                logger_.log(logLevel::INFO, "[Func: broadcastCheckpoint] Skipping sending SendCheckpoint to node "+nodePair.first);
                continue;
            }
            logger_.log(logLevel::DEBUG, "[Func: broadcastCheckpoint] ACTION - Send to node " + nodePair.first);
            Empty reply;
            auto* call = new AsyncNodeCall;
            call->response_reader = nodePair.second.nodeStub->AsyncSendCheckpoint(&call->context, checkpoint, &cq_);
            call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        }
        logger_.log(logLevel::INFO, "[Func: broadcastCheckpoint] EXIT - Dispatch complete.");
    }

    void broadcastViewChange(const ViewChange &viewChange)
    {
        logger_.log(logLevel::INFO, "[Func: broadcastViewChange] ENTER - Broadcasting VIEW_CHANGE view=" + to_string(viewChange.view_change_content().view_number()));
        {
            lock_guard<mutex> l(logMx_);
            log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: broadcastViewChange] - "+viewChange.DebugString()));
        }
        if (isLeader_.load() && byzantineStatus_.load() && timeAttackActivated_) {
            int delay = timeoutMs_.load()/4;
            logger_.log(logLevel::INFO, "[Func: broadcastViewChange] ENTER - Delaying broadcasting VIEW_CHANGE for " + to_string(delay));
            sleep_for(milliseconds(delay));
        }
        for (auto &nodePair : nodes_)
        {
            if (nodePair.first == id_ || (byzantineStatus_.load() && darkAttackActivated_ && nodes_[nodePair.first].byzantineVictim)) {
                logger_.log(logLevel::INFO, "[Func: broadcastViewChange] Skipping sending SendViewChange to node "+nodePair.first);
                continue;
            }
            logger_.log(logLevel::DEBUG, "[Func: broadcastViewChange] ACTION - Send to node " + nodePair.first);
            Empty reply;
            auto* call = new AsyncNodeCall;
            call->response_reader = nodePair.second.nodeStub->AsyncSendViewChange(&call->context, viewChange, &cq_);
            call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        }
        logger_.log(logLevel::INFO, "[Func: broadcastViewChange] EXIT - Dispatch complete.");
    }

    void broadcastNewView(const NewView& newView)
    {
        logger_.log(logLevel::INFO, "[Func: broadcastNewView] ENTER - Broadcasting NEW_VIEW view=" + to_string(newView.new_view_content().view_number()));
        {
            lock_guard<mutex> l(logMx_);
            log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: broadcastNewView] - "+newView.DebugString()));
        }
        if (isLeader_.load() && byzantineStatus_.load() && timeAttackActivated_) {
            int delay = timeoutMs_.load()/4;
            logger_.log(logLevel::INFO, "[Func: broadcastNewView] ENTER - Delaying broadcasting NEW_VIEW for " + to_string(delay));
            sleep_for(milliseconds(delay));
        }
        for (auto &nodePair : nodes_)
        {
            if (nodePair.first == id_ || (byzantineStatus_.load() && darkAttackActivated_ && nodes_[nodePair.first].byzantineVictim)) {
                logger_.log(logLevel::INFO, "[Func: broadcastNewView] Skipping sending NewView to node "+nodePair.first);
                continue;
            }
            logger_.log(logLevel::DEBUG, "[Func: broadcastNewView] ACTION - Send to node " + nodePair.first);
            Empty reply;
            auto* call = new AsyncNodeCall;
            call->response_reader = nodePair.second.nodeStub->AsyncSendNewView(&call->context, newView, &cq_);
            call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        }
        logger_.log(logLevel::INFO, "[Func: broadcastNewView] EXIT - Dispatch complete.");
    }

    string computeDigest(const string& input) {
        logger_.log(logLevel::DEBUG, "[Func: computeDigest] ENTER - Computing SHA-256 over input size=" + to_string(input.size()));
        unsigned char hash[SHA256_DIGEST_LENGTH];
        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, input.c_str(), input.size());
        SHA256_Final(hash, &ctx);

        stringstream ss;
        for (unsigned char c : hash)
            ss << hex << setw(2) << setfill('0') << (int)c;
        string out = ss.str();
        logger_.log(logLevel::DEBUG, "[Func: computeDigest] EXIT - Digest=" + out);
        return out;
    }

    Result executeReadOnlyOperation(const string &client)
    {
        logger_.log(logLevel::INFO, "[Func: executeReadOnlyOperation] ENTER - client=" + client);
        Result result;
        lock_guard<mutex> lg(dataStateFileMx_);
        json dataState = openJsonFile(dataStateFile_);
        if (dataState.contains(client))
        {
            result.set_successfully_processed(true);
            result.set_balance_value(dataState[client].get<int>());
            logger_.log(logLevel::DEBUG, "[Func: executeReadOnlyOperation] STATE - Hit. balance=" + to_string(result.balance_value()));
        }
        else
        {
            result.set_successfully_processed(false);
            result.set_balance_value(0);
            logger_.log(logLevel::DEBUG, "[Func: executeReadOnlyOperation] STATE - Miss. setting success=false");
        }
        logger_.log(logLevel::INFO, "[Func: executeReadOnlyOperation] EXIT");
        return result;
    }

    Result executeReadWriteOperation(const TransactionEntry &transaction)
    {
        logger_.log(logLevel::INFO, "[Func: executeReadWriteOperation] ENTER - tx sender=" + transaction.sender() + " -> receiver=" + transaction.receiver() + " units=" + to_string(transaction.unit()));
        Result result;
        lock_guard<mutex> lg(dataStateFileMx_);
        json dataState = openJsonFile(dataStateFile_);
        string sender = transaction.sender();
        string receiver = transaction.receiver();
        int unit = transaction.unit();
        if (dataState.contains(sender) && dataState.contains(receiver))
        {
            int sender_balance = dataState[sender].get<int>();
            int receiver_balance = dataState[receiver].get<int>();
            logger_.log(logLevel::DEBUG, "[Func: executeReadWriteOperation] STATE - Balances: sender=" + to_string(sender_balance) + ", receiver=" + to_string(receiver_balance));
            if (unit<=sender_balance) {
                sender_balance = sender_balance - unit;
                receiver_balance = receiver_balance + unit;
                dataState[sender] = sender_balance;
                dataState[receiver] = receiver_balance;
                if (writeJsonFile(dataStateFile_, dataState)) {
                    result.set_successfully_processed(true);
                    result.set_balance_value(sender_balance);
                    logger_.log(logLevel::INFO, "[Func: executeReadWriteOperation] ACTION - Transfer committed. New sender balance=" + to_string(sender_balance));
                }
                else {
                    result.set_successfully_processed(false);
                    result.set_balance_value(sender_balance + unit);
                    logger_.log(logLevel::ERROR, "[Func: executeReadWriteOperation] ERROR - Persist failed. Reverting visible balance in result to pre-transfer.");
                }
            }
            else {
                result.set_successfully_processed(false);
                result.set_balance_value(sender_balance);
                logger_.log(logLevel::WARN, "[Func: executeReadWriteOperation] WARN - Insufficient funds. Needed=" + to_string(unit) + ", available=" + to_string(sender_balance));
            }
        }
        else
        {
            result.set_successfully_processed(false);
            result.set_balance_value(0);
            logger_.log(logLevel::WARN, "[Func: executeReadWriteOperation] WARN - Account missing (sender or receiver).");
        }
        logger_.log(logLevel::INFO, "[Func: executeReadWriteOperation] EXIT - success=" + string(result.successfully_processed() ? "true" : "false") + ", balance=" + to_string(result.balance_value()));
        return result;
    }

    void processReadOnlyRequest(const Request &request)
    {
        logger_.log(logLevel::INFO, "[Func: processReadOnlyRequest] ENTER - client=" + request.client() + ", ts=" + to_string(request.request_content().timestamp()));
        Reply reply;
        reply.set_node(id_);
        ReplyContent* reply_content =  reply.mutable_reply_content();
        reply_content->set_timestamp(request.request_content().timestamp());
        reply_content->set_message_type(REPLY);
        reply_content->set_view_number(currentView_.load());
        reply_content->set_client(request.client());
        *reply_content->mutable_result() = executeReadOnlyOperation(request.request_content().operation().transaction().sender());
        reply.set_signature(bls_utils::sign_data(privateKey_, serializeDeterministic(*reply_content)));
        if (request.request_content().operation().type()!= READ_ONLY) {
            lock_guard<mutex> lg(clientTransactionsExecutionLogMx_);
            clientTransactionsExecutionLog_[{request.client(), request.request_content().timestamp()}] = make_shared<Reply>(reply);
        }
        if (byzantineStatus_.load() && crashAttackActivated_) {
            logger_.log(logLevel::INFO, "[Func: processReadOnlyRequest] ACTION - Avoiding sending Reply sent client " + request.client() + ", since it is read only.");
        }
        else {
            sendReplyToClient(reply, request.client());
        }
        logger_.log(logLevel::INFO, "[Func: processReadOnlyRequest] EXIT - Reply sent to client " + request.client());
    }

    void forwardRequestToLeader(const Request &request)
    {
        logger_.log(logLevel::INFO, "[Func: forwardRequestToLeader] ENTER - Forwarding request from client=" + request.client());
        {
            lock_guard<mutex> l(logMx_);
            log_.push_back(logger_.log(logLevel::MSG_OUT, "[RPC: forwardRequestToLeader] - "+request.DebugString()));
        }
        Empty reply;
        auto* call = new AsyncNodeCall;
        {
            lock_guard<mutex> lg(leaderMx_);
            auto it = nodes_.find(leaderId_);
            if (it != nodes_.end()) {
                if ((byzantineStatus_.load() && darkAttackActivated_ && nodes_[it->first].byzantineVictim)) {
                    logger_.log(logLevel::DEBUG, "[Func: forwardRequestToLeader] ACTION - Skipping forwardRequestToLeader " + leaderId_);
                }
                else {
                    logger_.log(logLevel::DEBUG, "[Func: forwardRequestToLeader] ACTION - Leader=" + leaderId_);
                    call->response_reader = it->second.nodeStub->AsyncSendRequest(&call->context, request, &cq_);
                    call->response_reader->Finish(&call->reply, &call->status, (void*)call);
                }
            }
            else {
                logger_.log(logLevel::ERROR, "[Func: forwardRequestToLeader] ERROR - Leader not found in nodes_ map. leaderId_=" + leaderId_);
            }
        }
        logger_.log(logLevel::INFO, "[Func: forwardRequestToLeader] EXIT");
    }

    inline ByzantineAttack FromProto(const ByzantineAtt& p) {
        logger_.log(logLevel::DEBUG, "[Func: FromProto] ENTER - type=" + p.type() + ", targets=" + to_string(p.target_node_ids_size()));
        return {p.type(), {p.target_node_ids().begin(), p.target_node_ids().end()}};
    }

    inline vector<ByzantineAttack> RepeatedToVector(const google::protobuf::RepeatedPtrField<ByzantineAtt>& rpt) {
        logger_.log(logLevel::DEBUG, "[Func: RepeatedToVector] ENTER - count=" + to_string(rpt.size()));
        vector<ByzantineAttack> v;
        v.reserve(rpt.size());
        for (const auto& p : rpt) v.push_back(FromProto(p));
        logger_.log(logLevel::DEBUG, "[Func: RepeatedToVector] EXIT - produced=" + to_string(v.size()));
        return v;
    }

    void runServer()
    {
        logger_.log(logLevel::INFO, "[Func: runServer] ENTER - Starting gRPC server on port " + to_string(port_));
        string server_address("0.0.0.0:" + to_string(port_));
        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&orchestratorService_);
        builder.RegisterService(&nodeService_);
        server_ = builder.BuildAndStart();
        cout << "Server listening on " << server_address << endl;
        nodeThread_ = thread([this]()
                             { server_->Wait(); });
        sleep_for(seconds(5));
        logger_.log(logLevel::INFO, "[Func: runServer] EXIT - Server started at " + server_address);
    }

    void stopServer() {
        logger_.log(logLevel::INFO, "[Func: ~MainNode] ENTER - Shutting down node and cleaning up resources.");
        cq_.Shutdown();
        logger_.log(logLevel::DEBUG, "[Func: ~MainNode] ACTION - Completion queue shutdown initiated.");
        if (cqThread_.joinable()) cqThread_.join();
        if (server_) 
        {
            server_->Shutdown();
            logger_.log(logLevel::DEBUG, "[Func: ~MainNode] ACTION - Server shutdown completed.");
        }
        if (nodeThread_.joinable()) nodeThread_.join();
        logger_.log(logLevel::INFO, "[Func: ~MainNode] EXIT - Cleanup completed successfully.");
    }

    json openJsonFile(const string &file_path)
    {
        logger_.log(logLevel::DEBUG, "[Func: openJsonFile] ENTER - path=" + file_path);
        ifstream f(file_path);
        json j;
        if (!f.is_open())
        {
            logger_.log(logLevel::WARN, "[Func: openJsonFile] WARN - Could not open file: " + file_path);
            return j;
        }
        f >> j;
        logger_.log(logLevel::DEBUG, "[Func: openJsonFile] EXIT - loaded keys=" + to_string(j.size()));
        return j;
    }

    bool writeJsonFile(const string& fileName, json& dataJson)
    {
        logger_.log(logLevel::DEBUG, "[Func: writeJsonFile] ENTER - path=" + fileName + ", bytes=" + to_string(dataJson.dump().size()));
        ofstream outFile(fileName);
        if (!outFile.is_open())
        {
            logger_.log(logLevel::ERROR, "[Func: writeJsonFile] ERROR - could not open file for writing: " + fileName);
            return false;
        }

        outFile << dataJson.dump();
        outFile.close();
        logger_.log(logLevel::DEBUG, "[Func: writeJsonFile] EXIT - write success");
        return true;
    }
    
    void revertDataState() {
        json data;
        for (char c = 'A'; c <= 'J'; ++c) {
            std::string key(1, c);
            data[key] = 10;
        }
        lock_guard<mutex> lk(dataStateFileMx_);
        writeJsonFile(dataStateFile_, data);
    }

    string serializeDeterministic(const google::protobuf::Message& msg) {
        string out;
        google::protobuf::io::StringOutputStream stringStream(&out);
        google::protobuf::io::CodedOutputStream codedStream(&stringStream);
        codedStream.SetSerializationDeterministic(true);
        msg.SerializeToCodedStream(&codedStream);
        return out;
    }

    void initResources(const json &resourcesJson)
    {
        logger_.log(logLevel::INFO, "[Func: initResources] ENTER - items=" + to_string(resourcesJson.size()));
        for (auto &r : resourcesJson)
        {
            if (r["id"] == id_)
            {
                privateKey_ = r["private_key"].get<string>();
                publicKey_ = r["public_key"].get<string>();
                fastPrivateKey_ = r["fast_private_key"].get<string>();
                invalidKey_ = r["invalid_key"].get<string>();
                fastPublicKey_ = r["fast_public_key"].get<string>();
                aggPublicKey_ = r["agg_public_key"].get<string>();
                aggFastPublicKey_ = r["fast_agg_pub_key"].get<string>();
                logger_.log(logLevel::DEBUG, "[Func: initResources] STATE - Local keys loaded for id=" + id_);
                continue;
            }

            if (r["type"] == "node")
            {
                auto channel = grpc::CreateChannel(r["host"].get<string>() + ":" + to_string(r["port"]), grpc::InsecureChannelCredentials());
                unique_ptr<NodeService::Stub> stub(NodeService::NewStub(channel));
                nodes_[r["id"]] = {r["id"], r["host"], r["port"], move(stub), r["public_key"], r["fast_public_key"]};
                completeSet_.insert(r["id"].get<string>());
                logger_.log(logLevel::DEBUG, "[Func: initResources] ACTION - Added NODE " + string(r["id"]) + " @ " + string(r["host"]) + ":" + to_string(r["port"]));
            }
            else if (r["type"] == "client")
            {
                auto channel = grpc::CreateChannel(r["host"].get<string>() + ":" + to_string(r["port"]), grpc::InsecureChannelCredentials());
                lock_guard<mutex> l1(clientsMx_);
                unique_ptr<NodeService::Stub> stub(NodeService::NewStub(channel));
                clients_[r["id"]] = {r["id"], r["host"], r["port"], move(stub), r["public_key"]};
                logger_.log(logLevel::DEBUG, "[Func: initResources] ACTION - Added CLIENT " + string(r["id"]) + " @ " + string(r["host"]) + ":" + to_string(r["port"]));
            }
        }
        logger_.log(logLevel::INFO, "[Func: initResources] EXIT - nodes=" + to_string(nodes_.size()) + ", clients=" + to_string(clients_.size()));
    }

};

int main(int argc, char *argv[])
{
    bls_utils::initBLS();
    struct sigaction sa{};
    sa.sa_handler = crash_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESETHAND;
    sigaction(SIGSEGV, &sa, nullptr);
    sigaction(SIGABRT, &sa, nullptr);
    string id, stateFile;
    int port = -1;

    for (int i = 1; i < argc; i++)
    {
        string arg = argv[i];
        if (arg == "--id" && i + 1 < argc)
        {
            id = argv[++i];
        }
        else if (arg == "--stateFile" && i + 1 < argc)
        {
            stateFile = argv[++i];
        }
        else if (arg == "--port" && i + 1 < argc)
        {
            port = stoi(argv[++i]);
        }
    }

    if (id.empty() || stateFile.empty())
    {
        cerr << "Usage: ./orchestrator --id <id> --port <port> --stateFile <file>\n";
        return 1;
    }

    MainNode node(id, port, stateFile, "node_"+id+".txt");
    node.run();
    return 0;
}
