# Assignment 1

## w257wang

### Question 1

Pairs implementation. I used 2 jobs, one to count the total words and the second one use the total words find from first job to get the PMI. Input to the job one is the files, output from job, is the paired words and word line counts and total line counts. I saved them to HDFS, at the beginning of second job, read in, parse it, calculate it. the final output is a pair of string and the corresponding PMI. Job 1 intermediate key-value pairs are total line number processed from each mapper and total line occur of each word processed from each mapper and word co-occurance pairs and numbers. From Job 2 Intermediate key-value pairs only is word co-occurance pairs and numbers (pairs of String, number).

Stripes implementation. I used 2 jobs, same idea, one to count the total words and the second one use the total words find from first job to get the PMI. Input to the job one is the files, output from job, is the  words followed by word line counts, total line counts and the pair counts groups in the Stripes. I saved them to HDFS, at the beginning of second job, read in, parse it, calculate it. the final output is a Text word and the corresponding PMIs to all its pairs. Intermediate key-value pairs is kind of like the Pairs implementation, but data format as a key and stripe pair (key, HashMap).

### Question 2
	
At linux.student.cs.uwaterloo.ca:
+ Pairs : 32.997 seconds
+ Stripes : 23.962 seconds

### Question 3

At linux.student.cs.uwaterloo.ca:
+ Pairs : 33.783 seconds
+ Stripes : 24.862 seconds

### Question 4

- line: 38505  
- word: 115515
- byte: 1262808

### Question 5

+ (maine, anjou)	3.633142306524297

Thought: this two words are mostly occur with each other, and this two words are not common words.

### Question 6

+ (tears, eyes)	1.1651669513642202
+ (tears, heart)	0.650591773147556
+ (tears, her)	0.4773128031518101
+ (death, father's)	1.1202520038357642
+ (death, life)	0.7381345555274458
+ (death, after)	0.5617616404704495

### Question 7

+ (waterloo, napoleon)	1.9084397333304322
+ (waterloo, wellington)	1.528625787950042
+ (waterloo, ontario)	1.4269869581928345
+ (toronto, marlboros)	2.3539964727131575
+ (toronto, spadina)	2.312603813951679
+ (toronto, leafs)	2.3070976265059926
