




In each module, we dive deeply into a subject, going from the abstract to the concrete:
 * Abstract models
 * Fundamental challenges
 * Protocols and mechanisms


## Module 1: Time and Asynchrony

* The Synchronous and asynchronous network models. Partial orders.
* Causality.  Causality violations.  Clock skew.
* FIFO, Causal and totally-ordered delivery and broadcast protocols. Lamport clocks.  Vector clocks. Consistent snapshots.

## Module 2: Fault Tolerance

* Fault models.  Reliability. Availability. Principles of redundancy.
* Partial failure.  Non-independence of faults. Replication anomalies (see next section).
* Reliable delivery and broadcast.  Failure detection.  Primary/Backup, Chain and Quorum replication.  Recovery. Write-ahead logging.  Checkpointing.

## Module 3: Consistency

* Consistency models (single-copy consistency, sequential consistency, linearizability, serializability, causal consistency and other weak models)
* Message reordering in the presence of redundancy.  Race conditions. 
* Consistency in replication systems.   Two-phase locking.  Two-phase commit.  Consensus.

## Module 4: Parallelism and Scaleout

* Scaleability.  Data parallellism.  Embarrassing parallelism.
* Amdahl's law.  CALM Theorem.
* Distributed data processing systems and programming models.
