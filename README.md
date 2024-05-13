# CS350-Distributed-System
BU CS350
# HW1: Weather Stations
# Introduction

There are `k` weather stations around the world. Your task is to compute the current average temperature across these stations every `averagePeriod` seconds. We will provide a function that queries the temperature data at a given weather station and returns the current temperature. The function makes RPC calls across the network to the weather stations.

However, the weather station may take some time to respond or may not respond at all. Compute the average temperature for the weather stations that do respond, ensuring the entire calculation is completed within `averagePeriod` seconds. Your implementation should also gracefully handle shutdown requests from the `quit` channel.

<u>**Late responses should be ignored instead of included in the next batch.**</u>

If a batch contains zero observations, return NAN (this is the default behavior of floating point division by zero in Go).

You will be implementing **two** distinct solutions to this problem.

1. **Channel-based solution:** Your first implementation should exclusively utilize channels. In this approach, the use of mutexes or locks is not permitted.
2. **Mutex-based solution:** For your second implementation, you should modify your approach to instead rely on mutexes for managing concurrency.

## API

Your code will make calls to the getWeatherData function provided as an argument to your aggregator, which takes a weather station ID and a batch ID and returns a WeatherReport struct. Here are the signatures:

```go
type WeatherReport struct {
   Value float64
   Id    int
   Batch int
}

func getWeatherData(id int, batch int) WeatherReport {}
```

Your code should call GetWeatherData once for each weather station 0-k. For each batch of `averagePeriod` seconds of observations, increment the `batch` parameter. For example, if `averagePeriod` is 5 seconds, then the first batch will be 0, and then 5 seconds later, you will request a new batch with `batch` set to 1 and use those observations to compute the average temperature for batch 1.

The `value` parameter is the temperature reading from the weather station.

Every `averagePeriod` seconds, your code should send a `WeatherReport` to the `out` channel. The `WeatherReport` should have the average temperature and the batch number. (The `id` field can be set to -1 or left blank.)

Your code must terminate immediately upon receiving a signal on the `quit` channel.

# Instructions

1. Clone the repository and navigate to the `ws` directory.
2. Put your code in the appropritate methods/files.
   1. Solution with channels goes in `channelAggregator` in `channel_aggregator.go`.
   2. Solution with mutexes goes in `mutexAggregator` in `mutex_aggregator.go`.
3. Run the tests in `ws_test.go`:
   1. `go test -v -race`
   2. Your IDE should also be able to run the tests and show you the results in a GUI. (Note that if you're using VSCode, you will likely need to edit the default go test timeout to be more than 30s.)
   3. For Windows users, you will need WSL set up to run the tests.
4. Upload the `channel_aggregator.go` and `mutex_aggregator.go` files to Gradescope. Do not change the file names, or the autograder may not recognize them.

#HW2: Bank Transactions
## **Introduction**

Correctness of data is very important in real world applications such as banking. An ideal bank should be able to handle millions of requests for various accounts simultaneously, whilst ensuring that all user data is correct.

Some common tasks that all banking applications have to handle are:

- Create account
- Deposit funds
- Withdraw funds
- Transfer funds

In this assignment, you are given a codebase with some parts of a banking application implemented. Some parts of the code are not implemented and some might be buggy. Your task is to implement the missing code, and fix any possible bugs in the code.

## **Code Overview and Objectives**

All code for this assignment resides in bank.go

You are provided with the following structs for defining a bank and accounts within it:

```go
type Bank struct {
	bankLock *sync.RWMutex
	accounts map[int]*Account
}

type Account struct {
	balance int
	lock    *sync.Mutex
}
```

You need to ensure that the following methods are implemented correctly:

- CreateAccount()
- Deposit()
- Withdraw()
- Transfer()
- **\[BONUS!\]** DepositAndCompare()

Your code’s execution should be **deterministic.** The tester will make a huge number of concurrent calls to your code, and running the exact same input multiple times should not affect the final state of the Bank or any of the Accounts within it.

Your code should also be able to handle errors such as handling account ids that dont exist or are being duplicated by throwing an error. In such cases, your code needs to log appropriate error messages.

### **DPrintf**

We also provide you with a function called DPrintf(). It is similar to Printf, but you can set

```go
const Debug = false
```

to disable the DPrintf() statements, which may help you debug your code.

It is set to true by default. Using DPrintf is optional, but recommended for ease of coding.

## **Instructions**

1. Clone the repository and navigate to the bank directory.
2. Put your code in the appropriate methods/files.
3. Run the tests
   1. go test -v -race
   2. You should ensure that you the entire test suite works multiple times, ie. try running the tests at least 5 times successfully.
4. Upload the bank.go file to Gradescope. Do not change the file name, or the autograder may not recognize it.

Your output should be something like this

```
go test -v -race

=== RUN   TestCreateAccountBasic
--- PASS: TestCreateAccountBasic (0.00s)
=== RUN   TestCreateAccountMany
--- PASS: TestCreateAccountMany (0.00s)
=== RUN   TestManyDepositsAndWithdraws
--- PASS: TestManyDepositsAndWithdraws (3.57s)
=== RUN   TestFewTransfers
--- PASS: TestFewTransfers (0.00s)
=== RUN   TestManyTransfers
--- PASS: TestManyTransfers (0.00s)
=== RUN   TestDepositAndCompare
--- PASS: TestDepositAndCompare (0.01s)
PASS
ok  	cs350/bank-transactions	4.771s
```

#HW3: MapReduce
## Introduction

In this lab you'll build a MapReduce system. You'll implement a worker process that calls application Map and Reduce functions and handles reading and writing files, and a coordinator process that hands out tasks to workers and copes with failed workers. You'll be building something similar to the [MapReduce paper](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

## Getting started

Update your existing fork of the `starter-code` repo

```
git fetch upstream
git pull
```

We supply you with a simple sequential mapreduce implementation in `mr-main/mrsequential.go`. It runs the maps and reduces one at a time, in a single process. We also provide you with a couple of MapReduce applications: word-count in `mr-main/mrapps/wc.go`, and a text indexer in `mr-main/mrapps/indexer.go`. You can run word count sequentially as follows:

```
$ cd mr-main
$ go build -buildmode=plugin ./mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so ../data/pg*.txt
$ more mr-out-0
A 509
ABOUT 2
ACT 8
...
```

`mrsequential.go` leaves its output in the file `mr-out-0`. The input is from the text files named `pg-xxx.txt` in the `data` folder.

Feel free to borrow code from `mrsequential.go`. You should also have a look at `mr-main/mrapps/wc.go` to see what MapReduce application code looks like.

## Your Job

1. Your first task is to implement a distributed MapReduce, consisting of two programs, the coordinator and the worker. There will be just one coordinator process, and one or more worker processes executing in parallel. In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine. The workers will talk to the coordinator via RPC. Each worker process will ask the coordinator for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files. The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

2. Once your MapReduce implementation is working as expected, please proceed to [Task 2](#task-2).

## Task 1

We have given you a little code to start you off. The "main" routines for the coordinator and worker are in `mr-main/mrcoordinator.go` and `mr-main/mrworker.go`; **do NOT change these files**. You should put your implementation in `mr/coordinator.go`, `mr/worker.go`, and `mr/rpc.go`.

### Manully build & run your code

In order to run your code on a MapReduce application (e.g. word-count), please navigate to the `mr-main` directory. First, make sure the word-count plugin is freshly built:

```
$ go build -buildmode=plugin ../mr-main/mrapps/wc.go
```

Run the coordinator.

```
$ rm mr-out*
$ go run mrcoordinator.go ../data/pg-*.txt
```

The `../data/pg-*.txt` arguments to `mrcoordinator.go` are the input files; each file corresponds to one "split", and is the input to one Map task.

In one or more **other** windows/terminals, run some workers:

```
$ go run mrworker.go wc.so
```

When the workers and coordinator have finished, look at the output in `mr-out-*`. When you've completed the lab, the sorted union of the output files should match the sequential output, like this:

```
$ cat mr-out-* | sort | more
A 509
ABOUT 2
ACT 8
...
```

### Test your code

We supply you with a test script in `mr-main/test-mr.sh`. The tests check that the `wc` and `indexer` MapReduce applications produce the correct output when given the `pg-xxx.txt` files as input. The tests also check that your implementation runs the Map and Reduce tasks in parallel, and that your implementation recovers from workers that crash while running tasks.

If you run the test script now, it will hang because the coordinator never finishes:

```
$ cd mr-main
$ bash test-mr.sh
*** Starting wc test.
```

You can change `ret := false` to true in the `Done` function in `mr/coordinator.go` so that the coordinator exits immediately. Then:

```
$ bash ./test-mr.sh
*** Starting wc test.
sort: No such file or directory
cmp: EOF on mr-wc-all
--- wc output is not the same as mr-correct-wc.txt
--- wc test: FAIL
```

The test script expects to see output in files named `mr-out-X`, one for each reduce task. The empty implementations of `mr/coordinator.go` and `mr/worker.go` don't produce those files (or do much of anything else), so the test fails.

When you've finished, the test script output should look like this:

```
$ bash ./test-mr.sh
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

You'll also see some errors from the Go RPC package that look like

```
2019/12/16 13:27:09 rpc.Register: method "Done" has 1 input parameters; needs exactly three
```

Ignore these messages.

### A few rules:

- The map phase should divide the intermediate keys into buckets for `nReduce` reduce tasks, where `nReduce` is the argument that `mr-main/mrcoordinator.go` passes to `MakeCoordinator()`.
- The worker implementation should put the output of the X'th reduce task in the file `mr-out-X`.
- A `mr-out-X` file should contain one line per Reduce function output. The line should be generated with the Go `"%v %v"` format, called with the key and value. Have a look in `mr-main/mrsequential.go` for the line commented "this is the correct format". The test script will fail if your implementation deviates too much from this format.
- You can modify `mr/worker.go`, `mr/coordinator.go`, and `mr/rpc.go`. You can temporarily modify other files for testing, but make sure your code works with the original versions; we'll test with the original versions.
- The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.
- `mr-main/mrcoordinator.go` expects `mr/coordinator.go` to implement a `Done()` method that returns true when the MapReduce job is completely finished; at that point, `mrcoordinator.go` will exit.
- When the job is completely finished, the worker processes should exit. A simple way to implement this is to use the return value from `call()`: if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, and so the worker can terminate too. Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers.

## Task 2

You are required to implement a MapReduce application [credit.go](../mr-main/mrapps/credit.go), just like the others seen in the `mrapps` folder. For this application, you are given the input dataset of the form:

| User ID | Agency | Year | Credit Score |
| --- | --- | --- | --- |
| 64 | Equifax | 2023 | 660
| 128 | TransUnion | 2021 | 380

Your goal is to compute the total number of people per agency whose credit score in 2023 was larger than 400. 

This data is present as CSV files in `data/credit-score/`. The expected output for the given data is shown below, but your code may be tested against multiple **different data sets** upon submission.

```
Equifax: 648
Experian: 671
TransUnion: 659
Yellow Banana: 677
```

You can use a script in `mr-main/test-mr-app.sh` to test your MR application. Please refer to the [test script](../mr-main/test-mr-app.sh) for more info.

## Hints

- One way to get started is to modify `mr/worker.go`'s `Worker()` to send an RPC to the coordinator asking for a task. Then modify the coordinator to respond with the file name of an as-yet-unstarted map task. Then modify the worker to read that file and call the application Map function, as in `mrsequential.go`.
- The application Map and Reduce functions are loaded at run-time using the Go plugin package, from files whose names end in `.so`.
- If you change anything in the `mr/` directory, you will probably have to re-build any MapReduce plugins you use, with something like `go build -buildmode=plugin ../mrapps/wc.go`.
- This lab relies on the workers sharing a file system. That's straightforward when all workers run on the same machine, but would require a global filesystem like GFS if the workers ran on different machines.
- A reasonable naming convention for intermediate files is `mr-X-Y`, where `X` is the Map task number, and `Y` is the reduce task number.
- The worker's map task code will need a way to store intermediate key/value pairs in files in a way that can be correctly read back during reduce tasks. One possibility is to use Go's `encoding/json` package. To write key/value pairs to a JSON file:

```
  enc := json.NewEncoder(file)
  for _, kv := ... {
    err := enc.Encode(&kv)
```

and to read such a file back:

```
  dec := json.NewDecoder(file)
  for {
    var kv KeyValue
    if err := dec.Decode(&kv); err != nil {
      break
    }
    kva = append(kva, kv)
  }
```

- The map part of your worker can use the `ihash(key)` function (in `worker.go`) to pick the reduce task for a given key.
- You can steal some code from `mrsequential.go` for reading Map input files, for sorting intermedate key/value pairs between the Map and Reduce, and for storing Reduce output in files.
- The coordinator, as an RPC server, will be concurrent; don't forget to lock shared data.
- Use Go's race detector, with `go build -race` and `go run -race`. `test-mr.sh` has a comment that shows you how to enable the race detector for the tests.
- Workers will sometimes need to wait, e.g. reduces can't start until the last map has finished. One possibility is for workers to periodically ask the coordinator for work, sleeping with `time.Sleep()` between each request. Another possibility is for the relevant RPC handler in the coordinator to have a loop that waits, either with `time.Sleep()` or `sync.Cond`. Go runs the handler for each RPC in its own thread, so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.
- The coordinator can't reliably distinguish between crashed workers, workers that are alive but have stalled for some reason, and workers that are executing but too slowly to be useful. The best you can do is have the coordinator wait for some amount of time, and then give up and re-issue the task to a different worker. For this lab, have the coordinator wait for ten seconds; after that the coordinator should assume the worker has died (of course, it might not have).
- To test crash recovery, you can use the `mrapps/crash.go` application plugin. It randomly exits in the Map and Reduce functions.
- To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once it is completely written. You can use `ioutil.TempFile` to create a temporary file and `os.Rename` to atomically rename it.
- `test-mr.sh` runs all the processes in the sub-directory `mr-tmp`, so if something goes wrong and you want to look at intermediate or output files, look there.
- If you are a Windows user and use WSL, you might have to do `dos2unix test-mr.sh` before running the test script (do this in case you get weird errors when run `bash test-mr.sh`).

#HW4: Raft
## Introduction

In this series of assignments you'll implement Raft, a replicated state machine protocol. A replicated service achieves fault tolerance by storing complete copies of its state (i.e., data) on multiple replica servers. Replication allows the service to continue operating even if some of its servers experience failures (crashes or a broken or flaky network). The challenge is that failures may cause the replicas to hold differing copies of the data.

Raft organizes client requests into a sequence, called the log, and ensures that all the replica servers see the same log. Each replica executes client requests in log order, applying them to its local copy of the service's state. Since all the live replicas see the same log contents, they all execute the same requests in the same order, and thus continue to have identical service state. If a server fails but later recovers, Raft takes care of bringing its log up to date. Raft will continue to operate as long as at least a majority of the servers are alive and can talk to each other. If there is no such majority, Raft will make no progress, but will pick up where it left off as soon as a majority can communicate again.

In these assignments you'll implement Raft as a Go object type with associated methods, meant to be used as a module in a larger service. A set of Raft instances talk to each other with RPC to maintain replicated logs. Your Raft interface will support an indefinite sequence of numbered commands, also called log entries. The entries are numbered with index numbers. The log entry with a given index will eventually be committed. At that point, your Raft should send the log entry to the larger service for it to execute.

You should follow the design in the [extended Raft paper](https://cs-people.bu.edu/liagos/651-2022/papers/raft-extended.pdf), with particular attention to Figure 2. You'll implement most of what's in the paper, including saving persistent state and reading it after a node fails and then restarts. You will not implement cluster membership changes (Section 6) or log snapshotting.

You may find this [guide](https://thesquareplanet.com/blog/students-guide-to-raft/) useful, as well as this advice about [locking](https://cs-people.bu.edu/liagos/651-2022/labs/raft-locking.txt) and [structure](https://cs-people.bu.edu/liagos/651-2022/labs/raft-structure.txt) for concurrency. For a wider perspective, have a look at Paxos, Chubby, Paxos Made Live, Spanner, Zookeeper, Harp, Viewstamped Replication, and [Bolosky et al.](http://static.usenix.org/event/nsdi11/tech/full_papers/Bolosky.pdf) (Note: the student's guide was written several years ago, and part 2D in particular has since changed. Make sure you understand why a particular implementation strategy makes sense before blindly following it!)

This assignment is due in three parts. You must submit each part on the corresponding due date.

## Getting Started

We supply you with skeleton code `src/raft/raft.go`. We also supply a set of tests, which you should use to drive your implementation efforts, and which we'll use to grade your submitted assignment. The tests are in `src/raft/test_test.go`.

To get up and running, execute the following commands. Don't forget the `git pull` to get the latest software.

```
$ cd raft
$ go test -race
Test (4A): initial election ...
--- FAIL: TestInitialElection4A (5.04s)
        config.go:326: expected one leader, got none
Test (4A): election after network failure ...
--- FAIL: TestReElection4A (5.03s)
        config.go:326: expected one leader, got none
...
```

To run a specific set of tests, use `go test -race -run 4A` or `go test -race -run TestInitialElection4A`.

## The Code

Implement Raft by adding code to `raft/raft.go`. In that file you'll find skeleton code, plus examples of how to send and receive RPCs.

Your implementation must support the following interface, which the Tester and (eventually) your key/value server will use. You'll find more details in comments in `raft.go`.

```
// create a new Raft server instance:
rf := Make(peers, me, persister, applyCh)

// start agreement on a new log entry:
rf.Start(command interface{}) (index, term, isleader)

// ask a Raft for its current term, and whether it thinks it is leader
rf.GetState() (term, isLeader)

// each time a new entry is committed to the log, each Raft peer
// should send an ApplyMsg to the service (or Tester).
type ApplyMsg
```

A service calls `Make(peers,me,…)` to create a Raft peer. The peers argument is an array of network identifiers of the Raft peers (including this one), for use with RPC. The `me` argument is the index of this peer in the peers array. `Start(command)` asks Raft to start the processing to append the command to the replicated log. `Start()` should return immediately, without waiting for the log appends to complete. The service expects your implementation to send an `ApplyMsg` for each newly committed log entry to the `applyCh` channel argument to `Make()`.

`raft.go` contains example code that sends an RPC (`sendRequestVote()`) and that handles an incoming RPC (`RequestVote()`). Your Raft peers should exchange RPCs using the labrpc Go package (source in `src/labrpc`). The Tester can tell `labrpc` to delay RPCs, re-order them, and discard them to simulate various network failures. While you can temporarily modify `labrpc`, make sure your Raft works with the original `labrpc`, since that's what we'll use to test and grade your assignment. Your Raft instances must interact only with RPC; for example, they are not allowed to communicate using shared Go variables or files.

## Part 4A: Leader Election

### Task

Implement Raft leader election and heartbeats (`AppendEntries` RPCs with no log entries). The goal for Part 4A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost. Run `go test -run 4A -race` to test your 4A code.

### Hints

- You can't easily run your Raft implementation directly; instead you should run it by way of the Tester, i.e. `go test -run 4A -race`.
- Follow the paper's Figure 2. At this point you care about sending and receiving `RequestVote` RPCs, the Rules for Servers that relate to elections, and the State related to leader election,
- Add the Figure 2 state for leader election to the `Raft` struct in `raft.go`. You'll also need to define a struct to hold information about each log entry.
- Fill in the `RequestVoteArgs` and `RequestVoteReply` structs. Modify `Make()` to create a background goroutine that will kick off leader election periodically by sending out `RequestVote` RPCs when it hasn't heard from another peer for a while. This way a peer will learn who is the leader, if there is already a leader, or become the leader itself. Implement the `RequestVote()` RPC handler so that servers will vote for one another.
- To implement heartbeats, define an `AppendEntries` RPC struct (though you may not need all the arguments yet), and have the leader send them out periodically. Write an `AppendEntries` RPC handler method that resets the election timeout so that other servers don't step forward as leaders when one has already been elected.
- Make sure the election timeouts in different peers don't always fire at the same time, or else all peers will vote only for themselves and no one will become the leader.
- The Tester requires that the leader send heartbeat RPCs no more than ten times per second.
- The Tester requires your Raft to elect a new leader within five seconds of the failure of the old leader (if a majority of peers can still communicate). Remember, however, that leader election may require multiple rounds in case of a split vote (which can happen if packets are lost or if candidates unluckily choose the same random backoff times). You must pick election timeouts (and thus heartbeat intervals) that are short enough that it's very likely that an election will complete in less than five seconds even if it requires multiple rounds.
- The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds. Such a range only makes sense if the leader sends heartbeats considerably more often than once per 150 milliseconds. Because the Tester limits you to 10 heartbeats per second, you will have to use an election timeout larger than the paper's 150 to 300 milliseconds, but not too large, because then you may fail to elect a leader within five seconds.
- You may find Go's [rand](https://golang.org/pkg/math/rand/) useful.
- You'll need to write code that takes actions periodically or after delays in time. The easiest way to do this is to create a goroutine with a loop that calls [time.Sleep()](https://golang.org/pkg/time/#Sleep); (see the `ticker()` goroutine that `Make()` creates for this purpose). Don't use Go's `time.Timer` or `time.Ticker`, which are difficult to use correctly.
- The [Guidance page](https://cs-people.bu.edu/liagos/651-2022/labs/guidance.html) has some tips on how to develop and debug your code.
- If your code has trouble passing the tests, read the paper's Figure 2 again; the full logic for leader election is spread over multiple parts of the figure.
- Don't forget to implement `GetState()`.
- The Tester calls your Raft's `rf.Kill()` when it is permanently shutting down an instance. You can check whether `Kill()` has been called using `rf.killed()`. You may want to do this in all loops, to avoid having dead Raft instances print confusing messages.
- Go RPC sends only struct fields whose names start with capital letters. Sub-structures must also have capitalized field names (e.g. fields of log records in an array). The `labgob` package will warn you about this; don't ignore the warnings.

Be sure you pass the 4A tests before submitting Part 4A, so that you see something like this:

```
$ go test -run 4A -race
Test (4A): initial election ...
  ... Passed --   4.0  3   32    9170    0
Test (4A): election after network failure ...
  ... Passed --   6.1  3   70   13895    0
PASS
ok      raft    10.187s
```

Each "Passed" line contains five numbers; these are the time that the test took in seconds, the number of Raft peers (usually 3 or 5), the number of RPCs sent during the test, the total number of bytes in the RPC messages, and the number of log entries that Raft reports were committed. Your numbers will differ from those shown here. You can ignore the numbers if you like, but they may help you sanity-check the number of RPCs that your implementation sends. For all of the parts the grading script will fail your solution if it takes more than 600 seconds for all of the tests (`go test`), or if any individual test takes more than 120 seconds.

## Part 4B: Log Replication and Persistence

If a Raft-based server reboots it should resume service where it left off. This requires that Raft keep persistent state that survives a reboot. The paper's Figure 2 mentions which state should be persistent. A real implementation would write Raft's persistent state to disk each time it changed, and would read the state from disk when restarting after a reboot. Your implementation won't use the disk; instead, it will save and restore persistent state from a `Persister` object (see `persister.go`).

### Task

Implement the leader and follower code to append new log entries and persist state, so that the `go test -run 4B -race` tests pass.

Whoever calls `Raft.Make()` supplies a `Persister` that initially holds Raft's most recently persisted state (if any). Raft should initialize its state from that `Persister`, and should use it to save its persistent state each time the state changes. Use the `Persister`'s `ReadRaftState()` and `SaveRaftState()` methods.

Complete the functions `persist()` and `readPersist()` in `raft.go` by adding code to save and restore persistent state. You will need to encode (or "serialize") the state as an array of bytes in order to pass it to the `Persister`. Use the `labgob` encoder; see the comments in `persist()` and `readPersist()`. `labgob` is like Go's `gob` encoder but prints error messages if you try to encode structures with lower-case field names.

Insert calls to `persist()` at the points where your implementation changes persistent state. Once you've done this, you should pass the remaining tests.

**Note:** In order to avoid running out of memory, Raft must periodically discard old log entries, but you **do not have** to worry about this.

### Hints

- Your first goal should be to pass `TestBasicAgree4B()`. Start by implementing `Start()`, then write the code to send and receive new log entries via `AppendEntries` RPCs, following Figure 2.
- You will need to implement the election restriction (section 5.4.1 in the paper).
- One way to fail to reach agreement in the early Part 4B tests is to hold repeated elections even though the leader is alive. Look for bugs in election timer management, or not sending out heartbeats immediately after winning an election.
- Your code may have loops that repeatedly check for certain events. Don't have these loops execute continuously without pausing, since that will slow your implementation enough that it fails tests. Use Go's [condition variables](https://golang.org/pkg/sync/#Cond), or insert a `time.Sleep(10 * time.Millisecond)` in each loop iteration.
- Do yourself a favor and write (or re-write) code that's clean and clear. For ideas, re-visit the [Guidance page](https://cs-people.bu.edu/liagos/651-2022/labs/guidance.html) with tips on how to develop and debug your code.
- If you fail a test, look over the code for the test in `config.go` and `test_test.go` to get a better understanding what the test is testing. `config.go` also illustrates how the Tester uses the Raft API.
- Many of the persistence tests involve servers failing and the network losing RPC requests or replies. These events are non-deterministic, and you may get lucky and pass the tests, even though your code has bugs. Typically running the test several times will expose those bugs.
- You will probably need the optimization that backs up nextIndex by more than one entry at a time. Look at the [extended Raft](https://cs-people.bu.edu/liagos/651-2022/papers/raft-extended.pdf) paper starting at the bottom of page 7 and top of page 8 (marked by a gray line). The paper is vague about the details; you will need to fill in the gaps, perhaps with the help of the Raft lectures.

The tests may fail your code if it runs too slowly. You can check how much real time and CPU time your solution uses with the `time` command. Here's typical output:

```
$ time go test -run 4B
Test (4B): basic agreement ...
  ... Passed --   1.6  3   18    5158    3
Test (4B): RPC byte count ...
  ... Passed --   3.3  3   50  115122   11
Test (4B): agreement despite follower disconnection ...
  ... Passed --   6.3  3   64   17489    7
Test (4B): no agreement if too many followers disconnect ...
  ... Passed --   4.9  5  116   27838    3
Test (4B): concurrent Start()s ...
  ... Passed --   2.1  3   16    4648    6
Test (4B): rejoin of partitioned leader ...
  ... Passed --   8.1  3  111   26996    4
Test (4B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  28.6  5 1342  953354  102
Test (4B): RPC counts aren't too high ...
... Passed --   3.4  3   30    9050   12
Test (4B): basic persistence ...
... Passed --   7.2  3  206   42208    6
Test (4B): more persistence ...
  ... Passed --  23.2  5 1194  198270   16
Test (4B): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   3.2  3   46   10638    4
Test (4B): Figure 8 ...
  ... Passed --  35.1  5 9395 1939183   25
Test (4B): unreliable agreement ...
  ... Passed --   4.2  5  244   85259  246
Test (4B): Figure 8 (unreliable) ...
  ... Passed --  36.3  5 1948 4175577  216
Test (4B): churn ...
  ... Passed --  16.6  5 4402 2220926 1766
Test (4B): unreliable churn ...
  ... Passed --  16.5  5  781  539084  221
PASS
ok      cs350/raft      189.840s
go test -run 4B  10.32s user 5.81s system 8% cpu 3:11.40 total
```

The "ok raft 189.840s" means that Go measured the time taken for the 4B tests to be 189.840 seconds of real (wall-clock) time. The "10.32s user" means that the code consumed 10.32s seconds of CPU time, or time spent actually executing instructions (rather than waiting or sleeping). If your solution uses an unreasonable amount of time, look for time spent sleeping or waiting for RPC timeouts, loops that run without sleeping or waiting for conditions or channel messages, or large numbers of RPCs sent.

#### A few other hints:

- Run git pull to get the latest lab software.
- Failures may be caused by problems in your code for 4A or log replication. Your code should pass all the 4A and 4B tests.

It is a good idea to run the tests multiple times before submitting and check that each run prints `PASS`.

```
$ for i in {0..10}; do go test; done
```
