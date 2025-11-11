# PBFT Implementation (CSE535 Project 2)

This is an implementation of the Practical Byzantine Fault Tolerance (PBFT) consensus protocol for CSE535.

## Implementation Details

### Core Components
- Linear PBFT consensus protocol implementation using collector same as primary
- Pre-prepare, Prepare, and Commit phases
- View change mechanism
- Basic client-replica communication

### Bonus Features Implemented

1. **Threshold Signatures**
   - Used for signature aggregation in consensus phases like sending collected prepare and collected commit
   - Implemented using BLS signature scheme via Herumi BLS library

2. **Checkpointing**
   - Periodic state synchronization mechanism
   - State recovery functionality

3. **Optimistic Phase Reduction**
   - Reduced commit consensus phases in fault-free scenarios i.e. full quorum (3f+1)
   - Fallback to standard PBFT when needed

## Project Structure

- `client.cpp`: Client implementation
- `node.cpp`: Replica node implementation
- `orchestrator.cpp`: System orchestrator
- `messages.proto`: Protocol buffer message definitions
- `bls_helper.h`: BLS signature utilities
- `logger.h`: Logging functionality
- `threadpool.h`: Thread pool implementation
- `resettable_timer.h`: Timer implementation

## Build Instructions

```bash
mkdir build
cd build
cmake ..
```

## Running Instructions

```bash
cmake --build . --target run_orchestrator # inside build directory
```

## Dependencies
- CMake (>= 3.10)
- gRPC/Protocol Buffers
- BLS Signature Library
- C++17 compiler

## References
- ChatGPT : Used chatGPT to discuss design scenarios, optimise data structure usage and to debug code errors.
- https://github.com/herumi/bls : For sample CPP bls usage and structure identification
- Co-pilot : For formatting the readme file.