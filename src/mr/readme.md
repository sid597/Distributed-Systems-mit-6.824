This is a lab project for the MIT 6.824 graduate course which I implemented.

Prerequisites for taking this course: Computation Structures (6.004) and one of Computer System Engineering (6.033) or Operating System Engineering (6.828), or equivalent. Substantial programming experience will be helpful for the lab assignments.

What I had done : Among courses only Computation Structures, in case of programming experience : I wrote a compiler and operating system( very very basic) based on a course Nand2Tetris, I had built a website in flask (~3k lines of code) and a few small projects.

How did I overcome the knowledge gap due to other requirements I did not have ? 

I extensively read about concurrency, threads and RPC's from various resourses until I was able to understand how they are implemented in operating systems and follow the discussion in class. I think it is possible to implement labs by being ignorant to operating systems but after reading operating systems I found that one gains knowledge of what things can cause fault or delays in a distributed systems due to operating systems and how virtual machines can be implemented.

I think the role of this lab is to make the students familiar with GO, threads, RPC, implementing research papers. These reasons also make this lab difficult to implement for someone like me with not very much programming experience, I overcame this lack of experience by gaining experience on this lab although the code quality is not good in this lab but it improved in Lab 2 and 3. 

### Differences of this implementation from a production implementation :

 We don't have GFS and not enough money to buy real machines so the difference due to these designs are :
 -  All the data is read from and written to local disk.

 -  Master and Workers are just processes, I think we can also use containers to get the same result.

 - Not using heartbeat mechanism, will use 10 seconds timeout (for this lab) and if no response is received from a worker in progress we reassign.

    <i>It is easy to implement heartbeat from our side, we will do it in raft  implementation.</i>
- No concurrent job scheduling i.e cannot submit multiple mapReduce jobs and then the MapReduce library schedules these jobs to a set of available machines within the cluster.

- Master selection process and how master picks idle workers is not detailed in paper. So in labs who master is, is decided by middleware and workers call master for a job and based on if all map tasks are finished or not master assigns map task or reduce task to the asking worker. 

- The naming convention for machines might be different i.e how master knows which machines to communicate and how worker knows who the master is. In lab Master and worker implementation is in same package so workers call master object for job and then master assigns some id to these workers which is remembered by both the master and the worker.

- Using a garbage collected language GO insted of of systems language like rust, C or C++ for performance.

