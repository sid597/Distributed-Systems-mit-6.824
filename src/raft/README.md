This is a lab project to implement Raft Consensus Algorithm for the MIT 6.824 graduate course.

Implemented leader election, log replication, accelerated log backtracking and state persistence using GO 
language.

**My experience :**

It is easy to read the paper watch some lectures and think I understand what this is about, but it is far 
from truth.
First time I read the paper I thought I am following what the author is saying, things are making sense 
but at that time I was not thinking about server failures, delayed messages and network partitions. And when 
these are taken into consideration then, each and every if, but, and and, in Figure 2 become crucial. There 
are some implementation details that are not covered in paper but occur while implementing like receiving 
RPC's from leader of some previous term. Some things are not intutive while reading the paper and causes doubt
if it will do the correct thing

**Debugging** raft is also sometimes difficult, situations like
Every node is doing something but collectively no progress
is made for example no leader is getting selected or once a leader is elected, some other node starts election.
These are difficult to debug I had to look very long logs, spend hours to find where and why the bugs were. The 
more I read my buggy code the more I thought it was correct. My first iteration of raft with a few hundereds of
lines of code had to be thrown because I could not figure out what was wrong.

Another type of bug I faced was for most of the time I got correct result but sometimes wrong result was passed
All the failures I faced are also mentioned in the failures file which contains part wise failures

**How to make the implementation easier ?**

On hindsight I think the best way to go about implementing would be first writing down all the type of 
corner cases one can think of and then write down what you think the library should do and arrive at the 
correct outcome.

Despite all the hurdles it was so much fun implementing this lab, I would say this is the best programming task
I have done so far.
